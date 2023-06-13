/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.reflection;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.Pair;

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.catalog.CatalogEntityKey;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.sql.DremioSqlToRelConverter;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

public class DatasetHashUtils {

  /**
   * @return true if the dataset type is PHYSICAL_*
   */
  public static boolean isPhysicalDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  /**
   * Computes a hash for the input dataset by recursively looking through parent views and tables.
   *
   * @param dataset Dataset which we are computing a hash for
   * @param catalogService
   * @param ignorePds Exclude parent PDS from hash so that an additive change such as adding a column doesn't cause
   *                  a reflection anchored on a child view to require a full refresh (when previously incremental)
   * @return
   * @throws NamespaceException
   */
  public static Integer computeDatasetHash(DatasetConfig dataset, CatalogService catalogService, boolean ignorePds) throws NamespaceException {
    Queue<DatasetConfig> q = new LinkedList<>();
    q.add(dataset);
    int hash = 1;
    boolean isFirst = true;
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    while (!q.isEmpty()) {
      dataset = q.poll();
      if (isPhysicalDataset(dataset.getType())) {
        if (!ignorePds || isFirst) {
          hash = 31 * hash + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
        }
      } else {
        int schemaHash = 0;
        if (isFirst) {
          final List<ViewFieldType> types = new ArrayList<>();
          dataset.getVirtualDataset().getSqlFieldsList().forEach(type -> {
            if (type.getSerializedField() != null) {
              ViewFieldType newType = new ViewFieldType();
              ProtostuffIOUtil.mergeFrom(ProtostuffIOUtil.toByteArray(type, ViewFieldType.getSchema(), LinkedBuffer.allocate()), newType, ViewFieldType.getSchema());
              types.add(newType.setSerializedField(null));
            } else {
              types.add(type);
            }
          });
          schemaHash = types.hashCode();
        }
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode() + schemaHash;
        if (dataset.getVirtualDataset().getParentsList() != null) { // select 1 has null parents list
          for (ParentDataset parent : dataset.getVirtualDataset().getParentsList()) {
            int size = parent.getDatasetPathList().size();
            if (!(size > 1 && parent.getDatasetPathList().get(size - 1).equalsIgnoreCase("external_query"))) {
              DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, new NamespaceKey(parent.getDatasetPathList()));
              if (datasetConfig == null) {
                throw new NamespaceNotFoundException(new NamespaceKey(parent.getDatasetPathList()), "Not found");
              }
              q.add(datasetConfig);
            }
          }
        }
      }
      isFirst = false;
    }
    return hash;
  }

  /**
   * check with ignorePds true and then also false, for backward compatibility
   */
  static boolean hashEquals(int hash, DatasetConfig dataset, CatalogService cs) throws NamespaceException {
    return
      hash == computeDatasetHash(dataset, cs, true)
        ||
        hash == computeDatasetHash(dataset, cs, false);
  }

  /**
   * Similar to above {@link #computeDatasetHash(DatasetConfig, CatalogService, boolean)} except can handle
   * versioned parents such as views/tables from versioned sources. Prior to versioned sources, a view's parents were cached in the
   * KV store on save.  However, for versioned views or Sonar Views that reference versioned tables, the KV store
   * no longer stores the parents.  So instead, we need to get the parents for a view from the RelNode tree
   * which is capable of resolving version context based on AT syntax, refs and/or default source version.
   *
   * @param dataset Dataset which we are computing a hash for
   * @param catalogService
   * @param relNode RelNode tree for dataset
   * @param ignorePds Same as ignorePds in {@link #computeDatasetHash(DatasetConfig, CatalogService, boolean)}
   * @return
   */
  public static Integer computeDatasetHash(DatasetConfig dataset, CatalogService catalogService,
                                           RelNode relNode, boolean ignorePds) {
    ParentDatasetBuilder builder = new ParentDatasetBuilder(dataset, relNode, catalogService);
    Queue<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> q = new LinkedList<>();
    Pair<SubstitutionUtils.VersionedPath, DatasetConfig> current = Pair.of(SubstitutionUtils.VersionedPath.of(dataset.getFullPathList(), null), dataset);
    q.add(current);
    int hash = 1;
    boolean isFirst = true;
    while (!q.isEmpty()) {
      current = q.poll();
      dataset = current.right;
      if (isPhysicalDataset(dataset.getType())) {
        if (!ignorePds || isFirst) {
          hash = 31 * hash + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
        }
      } else {
        int schemaHash = 0;
        if (isFirst) {
          final List<ViewFieldType> types = new ArrayList<>();
          dataset.getVirtualDataset().getSqlFieldsList().forEach(type -> {
            if (type.getSerializedField() != null) {
              ViewFieldType newType = new ViewFieldType();
              ProtostuffIOUtil.mergeFrom(ProtostuffIOUtil.toByteArray(type, ViewFieldType.getSchema(), LinkedBuffer.allocate()), newType, ViewFieldType.getSchema());
              types.add(newType.setSerializedField(null));
            } else {
              types.add(type);
            }
          });
          schemaHash = types.hashCode();
        }
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode() + schemaHash;
        // A versioned view doesn't (actually can't) store their parent datasets as part of the view metadata
        for (Pair<SubstitutionUtils.VersionedPath, DatasetConfig> parent : builder.getParents(current.left)) {
          int size = parent.left.left.size();
          if (!(size > 1 && parent.left.left.get(size - 1).equalsIgnoreCase("external_query"))) {
            q.add(parent);
          }
        }
      }
      isFirst = false;
    }
    return hash;
  }

  /**
   * ParentDatasetBuilder builds a mapping of views to parent datasets.  Since both the view and the parent dataset
   * can be versioned tables/views, we track both the path components and table version context for each table/view.
   */
  @VisibleForTesting
  static class ParentDatasetBuilder extends RoutingShuttle {
    private Deque<SubstitutionUtils.VersionedPath> expansions = new LinkedList<>();
    private ListMultimap<SubstitutionUtils.VersionedPath, Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> parents = ArrayListMultimap.create();
    private EntityExplorer catalog;

    public ParentDatasetBuilder(final DatasetConfig config, final RelNode relNode, final CatalogService catalogService) {
      catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
      // Initialize with query root
      expansions.add(SubstitutionUtils.VersionedPath.of(config.getFullPathList(), null));
      relNode.accept(this);
    }

    public List<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> getParents(SubstitutionUtils.VersionedPath child) {
      return parents.get(child);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof ExpansionNode) {
        ExpansionNode expansionNode = (ExpansionNode) other;
        SubstitutionUtils.VersionedPath scan = SubstitutionUtils.VersionedPath.of(expansionNode.getPath().getPathComponents(), expansionNode.getVersionContext());
        DremioTable view = CatalogUtil.getTable(CatalogEntityKey.newBuilder().
          keyComponents(scan.left).
          tableVersionContext(scan.right).build(), catalog);
        parents.put(expansions.peekLast(), Pair.of(scan, view.getDatasetConfig()));
        expansions.addLast(scan);
        RelNode child = visitChild(other, 0, other.getInput(0));
        expansions.removeLast();
        return child;
      }
      return this.visitChildren(other);
    }

    @Override
    public RelNode visit(TableScan tableScan) {
      DremioTable table = tableScan.getTable().unwrap(DremioTable.class);
      parents.put(expansions.peekLast(), Pair.of(SubstitutionUtils.VersionedPath.of(table.getPath().getPathComponents(),
        table.getDataset().getVersionContext()), table.getDatasetConfig()));
      return tableScan;
    }
  }

  /**
   * TODO: REMOVE ME
   * Sample code to show how to construct a QueryContext and SqlConverter so that one can turn a DatasetConfig
   * into a RelNode query tree.
   *
   * @param sabotContext
   * @param dataset
   * @return
   */
  public static RelNode expandView(SabotContext sabotContext, DatasetConfig dataset)
  {
    NamespaceKey anchorView = new NamespaceKey(dataset.getFullPathList());
    Map<String, VersionContext> versionContextMap = ReflectionUtils.buildVersionContext(dataset.getId().getId());
    final UserSession session = ReflectionServiceImpl.systemSession(sabotContext.getOptionManager());
    for (Map.Entry<String, VersionContext> versionContext : versionContextMap.entrySet()) {
      session.setSessionVersionForSource(versionContext.getKey(), versionContext.getValue());
    }
    RelRoot root = null;
    try (QueryContext context = new QueryContext(session, sabotContext, new AttemptId().toQueryId(),
      java.util.Optional.of(false), java.util.Optional.of(false))) {
      SqlConverter converter = new SqlConverter(
        context.getPlannerSettings(),
        context.getOperatorTable(),
        context,
        MaterializationDescriptorProvider.EMPTY,
        context.getFunctionRegistry(),
        context.getSession(),
        AbstractAttemptObserver.NOOP,
        context.getCatalog(),
        context.getSubstitutionProviderFactory(),
        context.getConfig(),
        context.getScanResult(),
        context.getRelMetadataQuerySupplier());

      root = DremioSqlToRelConverter.expandView(null, new CatalogUser(SystemUser.SYSTEM_USERNAME),
        "select * from " + anchorView.getSchemaPath(), null, converter,
        null, null);

      System.out.println(RelOptUtil.toString(root.rel));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return root.rel;
  }
}
