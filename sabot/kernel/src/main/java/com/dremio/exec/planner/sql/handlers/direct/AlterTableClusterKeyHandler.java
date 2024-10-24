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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.store.SystemSchemas.CLUSTERING_TABLE_PROPERTY;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableClusterKey;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;

public class AlterTableClusterKeyHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public AlterTableClusterKeyHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableClusterKey sqlAlterTableClusterKey =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableClusterKey.class);
    QueryContext context = Preconditions.checkNotNull(config.getContext());
    OptionManager optionManager = Preconditions.checkNotNull(context.getOptions());
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlNode, optionManager);
    NamespaceKey path =
        CatalogUtil.getResolvePathForTableManagement(catalog, sqlAlterTableClusterKey.getTable());

    IcebergUtils.validateIcebergAutoClusteringIfDeclared(sql, context.getOptions());
    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    DremioTable table = catalog.getTableNoResolve(path);
    SimpleCommandResult result =
        SqlHandlerUtil.validateSupportForDDLOperations(catalog, config, path, table);
    if (!result.ok) {
      return Collections.singletonList(result);
    }

    // validate partition, auto clustering only works on nonpartitioned table
    PartitionSpec partitionSpec =
        IcebergUtils.getCurrentPartitionSpec(
            table.getDataset().getDatasetConfig().getPhysicalDataset());
    if (partitionSpec != null && partitionSpec.isPartitioned()) {
      throw UserException.unsupportedError()
          .message("Dremio supports altering clustering key only on nonpartitioned table")
          .buildSilently();
    }

    // if table has sort order already set, we cannot add clustering keys
    PhysicalDataset physicalDataset = table.getDataset().getDatasetConfig().getPhysicalDataset();
    String sortOrderString =
        IcebergUtils.getCurrentSortOrder(physicalDataset, context.getOptions());
    SortOrder sortOrder =
        IcebergSerDe.deserializeSortOrderFromJson(
            SchemaConverter.getBuilder().build().toIcebergSchema(table.getSchema()),
            sortOrderString);

    // When there are sort order AND Clustering Columns Table Property not set
    if (!IcebergUtils.hasClusteringColumnsProperty(physicalDataset.getIcebergMetadata())
        && (sortOrder != null && !sortOrder.isUnsorted())) {
      throw UserException.unsupportedError()
          .message(
              "Table: [%s] has sort order already defined, please unset sort order before adding clustering keys",
              path.getName())
          .buildSilently();
    }

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    TableMutationOptions tableMutationOptions =
        TableMutationOptions.newBuilder().setResolvedVersionContext(resolvedVersionContext).build();

    List<String> clusterKeyList = sqlAlterTableClusterKey.getClusterKeyList();
    String message = "";
    if (clusterKeyList.isEmpty()) {
      // make sure table has clustering key first, to avoid unset sort order
      if (!IcebergUtils.hasClusteringColumnsProperty(physicalDataset.getIcebergMetadata())) {
        throw UserException.unsupportedError()
            .message("Table: [%s] does not have clustering key defined", path.getName())
            .buildSilently();
      }
      // Unset SortOrder
      catalog.alterSortOrder(
          path,
          table.getDatasetConfig(),
          table.getSchema(),
          Collections.emptyList(),
          tableMutationOptions);
      // Unset clustering columns table property
      setClusteringTableProperty(path, table, tableMutationOptions, false);
      message = String.format("Cluster Key has been removed from Table: [%S]", path.getRoot());
    } else {
      Set<String> fieldSet =
          table.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toSet());

      for (String col : clusterKeyList) {
        if (!fieldSet.contains(col)) {
          throw UserRemoteException.validationError()
              .message(String.format("Column '%s' does not exist in the table.", col))
              .buildSilently();
        }
      }
      // Set SortOrder
      catalog.alterSortOrder(
          path, table.getDatasetConfig(), table.getSchema(), clusterKeyList, tableMutationOptions);
      // Set clustering table property if not already set
      if (!IcebergUtils.hasClusteringColumnsProperty(physicalDataset.getIcebergMetadata())) {
        setClusteringTableProperty(path, table, tableMutationOptions, true);
      }
      String clusterKeys = clusterKeyList.stream().collect(Collectors.joining(","));
      message = String.format("Cluster Key: [%s] set for Table: [%S]", clusterKeys, path.getRoot());
    }
    DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    return Collections.singletonList(SimpleCommandResult.successful(message));
  }

  private void setClusteringTableProperty(
      NamespaceKey path,
      DremioTable table,
      TableMutationOptions tableMutationOptions,
      boolean isSet) {
    Map<String, String> tableProperties;
    if (isSet) {
      tableProperties =
          IcebergUtils.convertTableProperties(
              List.of(CLUSTERING_TABLE_PROPERTY), List.of("true"), false);
    } else {
      tableProperties =
          IcebergUtils.convertTableProperties(List.of(CLUSTERING_TABLE_PROPERTY), null, true);
    }

    catalog.updateTableProperties(
        path,
        table.getDatasetConfig(),
        table.getSchema(),
        tableProperties,
        tableMutationOptions,
        !isSet);
  }
}
