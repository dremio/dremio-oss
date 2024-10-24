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
package com.dremio.exec.planner.sql.handlers.query;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.iceberg.RowLevelOperationMode;

/** Handles the UPDATE DML. */
public class UpdateHandler extends DmlHandler {

  @Override
  public NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlUpdateTable.class).getPath();
  }

  @Override
  protected SqlOperator getSqlOperator() {
    return SqlUpdateTable.OPERATOR;
  }

  @Override
  protected void validatePrivileges(
      Catalog catalog, CatalogEntityKey key, SqlNode sqlNode, VersionContext versionContext) {
    NamespaceKey namespaceKey = key.toNamespaceKey();
    validate(namespaceKey, versionContext);
  }

  @Override
  protected RowLevelOperationMode getRowLevelOperationMode(DremioTable table) {
    TableProperties updateDmlWriteMode =
        DmlUtils.getDmlWriteProp(table, org.apache.iceberg.TableProperties.UPDATE_MODE);
    return DmlUtils.getDmlWriteMode(updateDmlWriteMode);
  }

  /**
   * Check if Merge-On-Read update DML plan will require the row-splitter.
   *
   * <p>see {@link com.dremio.exec.store.iceberg.IcebergMergeOnReadRowSplitterTableFunction} for
   * more details on row splitter and its qualifications
   *
   * @param options Writer options
   * @param updateCall Update call
   * @return True if row splitter is needed, false otherwise
   */
  @Override
  protected boolean checkIfRowSplitterNeeded(
      RowLevelOperationMode dmlWriteMode, WriterOptions options, SqlNode updateCall) {
    boolean isMergeOnRead =
        ((SqlDmlOperator) updateCall).getDmlWriteMode() == RowLevelOperationMode.MERGE_ON_READ;
    boolean isPartitionedTable =
        options.getPartitionColumns() != null && !options.getPartitionColumns().isEmpty();

    if (!isMergeOnRead || !isPartitionedTable) {
      return false;
    }

    List<SqlNode> updateColumns = ((SqlUpdate) updateCall).getTargetColumnList().getList();

    return updateColumns.stream()
        .anyMatch(column -> options.getPartitionColumns().contains(column.toString()));
  }
}
