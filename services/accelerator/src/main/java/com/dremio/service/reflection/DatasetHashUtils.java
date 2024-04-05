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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.Pair;

public class DatasetHashUtils {

  /**
   * @return true if the dataset type is PHYSICAL_*
   */
  public static boolean isPhysicalDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET
        || t == DatasetType.PHYSICAL_DATASET_SOURCE_FILE
        || t == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER
        || t == DatasetType.PHYSICAL_DATASET_HOME_FILE
        || t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  /**
   * Computes a hash for the input dataset by recursively looking through parent views and tables.
   *
   * @param dataset Dataset which we are computing a hash for
   * @param catalog
   * @param ignorePds Exclude parent PDS from hash so that an additive change such as adding a
   *     column doesn't cause a reflection anchored on a child view to require a full refresh (when
   *     previously incremental)
   * @return
   * @throws NamespaceException
   */
  public static Integer computeDatasetHash(
      DatasetConfig dataset, EntityExplorer catalog, boolean ignorePds) throws NamespaceException {
    Queue<DatasetConfig> q = new LinkedList<>();
    q.add(dataset);
    int hash = 1;
    boolean isFirst = true;
    while (!q.isEmpty()) {
      dataset = q.poll();
      if (isPhysicalDataset(dataset.getType())) {
        if (!ignorePds || isFirst) {
          hash =
              31 * hash
                  + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
        }
      } else {
        int schemaHash = 0;
        if (isFirst) {
          final List<ViewFieldType> types = new ArrayList<>();
          dataset
              .getVirtualDataset()
              .getSqlFieldsList()
              .forEach(
                  type -> {
                    if (type.getSerializedField() != null) {
                      ViewFieldType newType = new ViewFieldType();
                      ProtostuffIOUtil.mergeFrom(
                          ProtostuffIOUtil.toByteArray(
                              type, ViewFieldType.getSchema(), LinkedBuffer.allocate()),
                          newType,
                          ViewFieldType.getSchema());
                      types.add(newType.setSerializedField(null));
                    } else {
                      types.add(type);
                    }
                  });
          schemaHash = types.hashCode();
        }
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode() + schemaHash;
        if (dataset.getVirtualDataset().getParentsList()
            != null) { // select 1 has null parents list
          for (ParentDataset parent : dataset.getVirtualDataset().getParentsList()) {
            int size = parent.getDatasetPathList().size();
            if (!(size > 1
                && parent.getDatasetPathList().get(size - 1).equalsIgnoreCase("external_query"))) {
              DatasetConfig datasetConfig =
                  CatalogUtil.getDatasetConfig(
                      catalog, new NamespaceKey(parent.getDatasetPathList()));
              if (datasetConfig == null) {
                throw new NamespaceNotFoundException(
                    new NamespaceKey(parent.getDatasetPathList()), "Not found");
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

  /** check with ignorePds true and then also false, for backward compatibility */
  static boolean hashEquals(int hash, DatasetConfig dataset, EntityExplorer catalog)
      throws NamespaceException {
    return hash == computeDatasetHash(dataset, catalog, true)
        || hash == computeDatasetHash(dataset, catalog, false);
  }

  /**
   * Similar to above {@link #computeDatasetHash(DatasetConfig, EntityExplorer, boolean)} except can
   * handle versioned parents such as views/tables from versioned sources. Prior to versioned
   * sources, a view's parents were cached in the KV store on save. However, for versioned views or
   * Sonar Views that reference versioned tables, the KV store no longer stores the parents. So
   * instead, we need to get the parents for a view from the RelNode tree which is capable of
   * resolving version context based on AT syntax, refs and/or default source version.
   *
   * @param catalog
   * @param relNode RelNode tree for dataset
   * @param ignorePds Same as ignorePds in {@link #computeDatasetHash(DatasetConfig, EntityExplorer,
   *     boolean)}
   * @return
   */
  public static Integer computeDatasetHash(
      EntityExplorer catalog, RelNode relNode, boolean ignorePds) {
    ParentDatasetBuilder builder = new ParentDatasetBuilder(relNode, catalog);
    Queue<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> q = new LinkedList<>();
    q.addAll(builder.getRootParents());
    int hash = 1;
    boolean isFirst = true;
    Pair<SubstitutionUtils.VersionedPath, DatasetConfig> current;
    while (!q.isEmpty()) {
      current = q.poll();
      final DatasetConfig dataset = current.right;
      if (isPhysicalDataset(dataset.getType())) {
        if (!ignorePds || isFirst) {
          hash =
              31 * hash
                  + (dataset.getRecordSchema() == null ? 1 : dataset.getRecordSchema().hashCode());
        }
      } else {
        int schemaHash = 0;
        if (isFirst) {
          final List<ViewFieldType> types = new ArrayList<>();
          dataset
              .getVirtualDataset()
              .getSqlFieldsList()
              .forEach(
                  type -> {
                    if (type.getSerializedField() != null) {
                      ViewFieldType newType = new ViewFieldType();
                      ProtostuffIOUtil.mergeFrom(
                          ProtostuffIOUtil.toByteArray(
                              type, ViewFieldType.getSchema(), LinkedBuffer.allocate()),
                          newType,
                          ViewFieldType.getSchema());
                      types.add(newType.setSerializedField(null));
                    } else {
                      types.add(type);
                    }
                  });
          schemaHash = types.hashCode();
        }
        hash = 31 * hash + dataset.getVirtualDataset().getSql().hashCode() + schemaHash;
        // A versioned view doesn't (actually can't) store their parent datasets as part of the view
        // metadata
        for (Pair<SubstitutionUtils.VersionedPath, DatasetConfig> parent :
            builder.getParents(current.left)) {
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
   * ParentDatasetBuilder builds a mapping of views to parent datasets. Since both the view and the
   * parent dataset can be versioned tables/views, we track both the path components and table
   * version context for each table/view.
   */
  @VisibleForTesting
  static class ParentDatasetBuilder extends RoutingShuttle {
    private Deque<SubstitutionUtils.VersionedPath> expansions = new LinkedList<>();
    private ListMultimap<
            SubstitutionUtils.VersionedPath, Pair<SubstitutionUtils.VersionedPath, DatasetConfig>>
        parents = ArrayListMultimap.create();
    private EntityExplorer catalog;

    public ParentDatasetBuilder(final RelNode relNode, final EntityExplorer catalog) {
      this.catalog = catalog;
      // Initialize with query root
      expansions.add(SubstitutionUtils.VersionedPath.of(null, null));
      relNode.accept(this);
      Preconditions.checkState(
          expansions.size() == 1, "Error tracking views in ParentDatasetBuilder");
    }

    public List<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> getRootParents() {
      return parents.get(SubstitutionUtils.VersionedPath.of(null, null));
    }

    public List<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> getParents(
        SubstitutionUtils.VersionedPath child) {
      return parents.get(child);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof ExpansionNode) {
        ExpansionNode expansionNode = (ExpansionNode) other;
        SubstitutionUtils.VersionedPath scan =
            SubstitutionUtils.VersionedPath.of(
                expansionNode.getPath().getPathComponents(), expansionNode.getVersionContext());
        if (parents.containsKey(scan)) {
          return other;
        }
        DremioTable view =
            catalog.getTableSnapshot(
                CatalogEntityKey.newBuilder()
                    .keyComponents(scan.left)
                    .tableVersionContext(scan.right)
                    .build());
        parents.put(expansions.peekLast(), Pair.of(scan, view.getDatasetConfig()));
        expansions.addLast(scan);
        visitChild(other, 0, other.getInput(0));
        expansions.removeLast();
        return other;
      }
      return this.visitChildren(other);
    }

    @Override
    public RelNode visit(TableScan tableScan) {
      DremioTable table = tableScan.getTable().unwrap(DremioTable.class);
      parents.put(
          expansions.peekLast(),
          Pair.of(
              SubstitutionUtils.VersionedPath.of(
                  table.getPath().getPathComponents(), table.getDataset().getVersionContext()),
              table.getDatasetConfig()));
      return tableScan;
    }
  }
}
