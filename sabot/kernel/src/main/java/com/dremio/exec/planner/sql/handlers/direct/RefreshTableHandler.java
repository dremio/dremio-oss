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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Handler for <code>REFRESH TABLE tblname</code> command.
 */
public class RefreshTableHandler extends SimpleDirectHandler {

  private final Catalog catalog;
  private final boolean allowPartialRefresh;

  public RefreshTableHandler(Catalog catalog, boolean allowPartialRefresh) {
    this.catalog = catalog;
    this.allowPartialRefresh = allowPartialRefresh;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlRefreshTable sqlRefreshTable = SqlNodeUtil.unwrap(sqlNode, SqlRefreshTable.class);

    if (!allowPartialRefresh) {
      if (isOptionEnabled(sqlRefreshTable.getAllFilesRefresh()) || isOptionEnabled(sqlRefreshTable.getFileRefresh()) ||
          isOptionEnabled(sqlRefreshTable.getAllPartitionsRefresh()) || isOptionEnabled(sqlRefreshTable.getPartitionRefresh())) {
        throw new UnsupportedOperationException("Refresh Metadata feature not yet implemented.");
      }
    }

    final NamespaceKey tableNSKey = catalog.resolveSingle(new NamespaceKey(sqlRefreshTable.getTable().names));

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

    UpdateStatus status = catalog.refreshDataset(tableNSKey, builder.build());

    final String message;
    switch(status){
    case CHANGED:
      message = "Metadata for table '%s' refreshed.";
      break;
    case DELETED:
      message = "Table '%s' no longer exists, metadata removed.";
      break;
    case UNCHANGED:
      message = "Table '%s' read signature reviewed but source stated metadata is unchanged, no refresh occurred.";
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
    return sqlRefreshTable.getFilesList().getList().stream().map(v -> ((SqlLiteral) v).getValueAs(String.class)).collect(Collectors.toList());
  }

  private Map<String, String> createPartitionMap(SqlRefreshTable sqlRefreshTable) {
    final Map<String, String> partition = new LinkedHashMap<>();
    sqlRefreshTable.getPartitionList().forEach(node -> {
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
}
