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

import static com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult.successful;
import static java.util.Collections.singletonList;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.users.SystemUser;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;

/** Handler for {@link SqlRefreshTable} command. */
public class RefreshTableHandler extends SimpleDirectHandler {

  private final Catalog catalog;
  private final boolean errorOnConcurrentRefresh;
  private final NamespaceService namespaceService;

  public RefreshTableHandler(
      Catalog catalog, NamespaceService namespaceService, boolean errorOnConcurrentRefresh) {
    this.catalog = catalog;
    this.namespaceService = namespaceService;
    this.errorOnConcurrentRefresh = errorOnConcurrentRefresh;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlRefreshTable sqlRefreshTable = SqlNodeUtil.unwrap(sqlNode, SqlRefreshTable.class);

    if (isOptionEnabled(sqlRefreshTable.getFileRefresh())) {
      throw UserException.validationError()
          .message("Refresh metadata for files are not supported")
          .buildSilently();
    }

    NamespaceKey tableNSKey =
        catalog.resolveSingle(new NamespaceKey(sqlRefreshTable.getTable().names));
    DatasetConfig datasetConfig = getConfigFromNamespace(tableNSKey);
    catalog.validatePrivilege(tableNSKey, SqlGrant.Privilege.ALTER);
    if (datasetConfig != null) {
      tableNSKey = new NamespaceKey(datasetConfig.getFullPathList());
    }

    DatasetRetrievalOptions.Builder builder = DatasetRetrievalOptions.newBuilder();
    if (sqlRefreshTable.getDeleteUnavail().getValue() != null) {
      builder.setDeleteUnavailableDatasets(sqlRefreshTable.getDeleteUnavail().booleanValue());
    }
    if (sqlRefreshTable.getForceUpdate().getValue() != null) {
      builder.setForceUpdate(sqlRefreshTable.getForceUpdate().booleanValue());
    }
    if (sqlRefreshTable.getPromotion().getValue() != null) {
      builder.setAutoPromote(sqlRefreshTable.getPromotion().booleanValue());
    }

    if (isOptionEnabled(sqlRefreshTable.getFileRefresh())) {
      builder.setFilesList(createFilesList(sqlRefreshTable));
    } else if (isOptionEnabled(sqlRefreshTable.getPartitionRefresh())) {
      builder.setPartition(createPartitionMap(sqlRefreshTable));
    }

    builder.setRefreshQuery(
        new MetadataRefreshQuery(
            sqlRefreshTable.toRefreshDatasetQuery(
                tableNSKey.getPathComponents(),
                Optional.ofNullable(datasetConfig)
                    .map(DatasetConfig::getPhysicalDataset)
                    .map(PhysicalDataset::getFormatSettings)
                    .map(FileConfig::getFileNameRegex),
                errorOnConcurrentRefresh),
            SystemUser.SYSTEM_USERNAME));

    UpdateStatus status = catalog.refreshDataset(tableNSKey, builder.build(), false);

    final String message;
    switch (status) {
      case CHANGED:
        message = "Metadata for table '%s' refreshed.";
        break;
      case DELETED:
        message = "Table '%s' no longer exists, metadata removed.";
        break;
      case UNCHANGED:
        message =
            "Table '%s' read signature reviewed but source stated metadata is unchanged, no refresh occurred.";
        break;
      default:
        throw new IllegalStateException();
    }

    return singletonList(successful(String.format(message, sqlRefreshTable.getTable().toString())));
  }

  private boolean isOptionEnabled(SqlLiteral option) {
    return option != null && option.getValue() != null && option.booleanValue();
  }

  private List<String> createFilesList(SqlRefreshTable sqlRefreshTable) {
    return sqlRefreshTable.getFilesList().getList().stream()
        .map(v -> ((SqlLiteral) v).getValueAs(String.class))
        .collect(Collectors.toList());
  }

  private Map<String, String> createPartitionMap(SqlRefreshTable sqlRefreshTable) {
    final Map<String, String> partition = new LinkedHashMap<>();
    sqlRefreshTable
        .getPartitionList()
        .forEach(
            node -> {
              final SqlNodeList pair = (SqlNodeList) node;
              final SqlIdentifier name = (SqlIdentifier) pair.get(0);
              final SqlLiteral value = (SqlLiteral) pair.get(1);

              if (value.getTypeName().equals(SqlTypeName.NULL)) {
                partition.put(name.getSimple(), null);
              } else {
                partition.put(name.getSimple(), value.getValueAs(String.class));
              }
            });

    return partition;
  }

  private DatasetConfig getConfigFromNamespace(NamespaceKey key) {
    try {
      return namespaceService.getDataset(key);
    } catch (NamespaceNotFoundException ex) {
      return null;
    } catch (NamespaceException ex) {
      throw new RuntimeException(ex);
    }
  }
}
