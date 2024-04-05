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
package com.dremio.exec.planner.sql.parser;

import static com.dremio.exec.catalog.CatalogUtil.resolveVersionContext;
import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.exec.util.ColumnUtils.COPY_HISTORY_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.ROW_INDEX_COLUMN_NAME;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.RowLevelOperationMode;
import org.projectnessie.model.ContentKey;

public final class DmlUtils {

  private DmlUtils() {}

  @VisibleForTesting
  public static final Map<TableModify.Operation, String> DML_OUTPUT_COLUMN_NAMES =
      new HashMap<TableModify.Operation, String>() {
        {
          put(TableModify.Operation.DELETE, "Rows Deleted");
          put(TableModify.Operation.INSERT, "Rows Inserted");
          put(TableModify.Operation.MERGE, "Rows Merged");
          put(TableModify.Operation.UPDATE, "Rows Updated");
        }
      };

  /** iceberg DML Operations use file path and Row Index System Columns regularly */
  public static final int SYSTEM_COLUMN_COUNT = 2;

  public static final String ROWS_REJECTED_COL_NAME = "Rows Rejected";

  public static SqlNode extendTableWithDataFileSystemColumns(SqlNode table) {
    SqlParserPos pos = table.getParserPosition();
    SqlNodeList nodes = new SqlNodeList(pos);

    addColumn(nodes, pos, FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR);
    addColumn(nodes, pos, ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT);

    return SqlStdOperatorTable.EXTEND.createCall(pos, table, nodes);
  }

  /**
   * Add an extra error column to the table
   *
   * @param table original table
   * @return decorated table
   */
  public static SqlNode extendTableWithCopyHistoryColumn(SqlNode table) {
    SqlParserPos pos = table.getParserPosition();
    SqlNodeList nodes = new SqlNodeList(pos);

    addColumn(nodes, pos, COPY_HISTORY_COLUMN_NAME, SqlTypeName.VARCHAR);

    return SqlStdOperatorTable.EXTEND.createCall(pos, table, nodes);
  }

  public static NamespaceKey getPath(SqlNode table) {
    if (table.getKind() == SqlKind.COLLECTION_TABLE) {
      String path =
          ((SqlCall) ((SqlVersionedTableCollectionCall) table).getOperandList().get(0))
              .getOperandList()
              .get(0)
              .toString()
              .replace("'", "")
              .replace("\"", "");
      ContentKey contentKey = ContentKey.fromPathString(path);
      return new NamespaceKey(contentKey.getElements());
    } else if (table.getKind() == SqlKind.EXTEND) {
      table = ((SqlCall) table).getOperandList().get(0);
    }
    SqlIdentifier tableIdentifier = (SqlIdentifier) table;
    return new NamespaceKey(tableIdentifier.names);
  }

  private static void addColumn(
      SqlNodeList nodes, SqlParserPos pos, String name, SqlTypeName type) {
    nodes.add(new SqlIdentifier(name, pos));
    nodes.add(new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type, -1, null, pos), pos));
  }

  public static boolean isInsertOperation(final CreateTableEntry createTableEntry) {
    return !ACCELERATOR_STORAGEPLUGIN_NAME.equals(createTableEntry.getPlugin().getId().getName())
        && createTableEntry instanceof CreateParquetTableEntry
        && (createTableEntry.getIcebergTableProps().getIcebergOpType()
            == IcebergCommandType.INSERT); // TODO: Add CREATE for CTAS with DX-48616
  }

  public static boolean isInsertOperation(final WriterOptions writerOptions) {
    return writerOptions
            .getTableFormatOptions()
            .getIcebergSpecificOptions()
            .getIcebergTableProps()
            .getIcebergOpType()
        == IcebergCommandType.INSERT; // TODO: Add CREATE for CTAS with DX-48616
  }

  public static RelDataType evaluateOutputRowType(
      final RelNode input, final RelOptCluster cluster, final TableModify.Operation operation) {
    RelDataTypeFactory.FieldInfoBuilder builder =
        cluster
            .getTypeFactory()
            .builder()
            .add(
                DML_OUTPUT_COLUMN_NAMES.get(operation),
                SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
                    SqlTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.BIGINT), true));
    if (input.getRowType().getField(COPY_HISTORY_COLUMN_NAME, false, false) != null) {
      builder.add(
          ROWS_REJECTED_COL_NAME,
          SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
              SqlTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.BIGINT), true));
    }

    return builder.build();
  }

  public static TableVersionContext getVersionContext(SqlDmlOperator sqlDmlOperator) {
    TableVersionSpec tableVersionSpec = sqlDmlOperator.getTableVersionSpec();
    if (tableVersionSpec != null) {
      return tableVersionSpec.getTableVersionContext();
    }
    return TableVersionContext.NOT_SPECIFIED;
  }

  public static TableVersionContext getVersionContext(SqlNode sqlNode) {
    if (sqlNode instanceof SqlDmlOperator) {
      SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) sqlNode;
      return getVersionContext(sqlDmlOperator);
    }
    return TableVersionContext.NOT_SPECIFIED;
  }

  public static RowLevelOperationMode getDmlWriteMode(TableProperties property) {
    if (property != null
        && property
            .getTablePropertyValue()
            .equalsIgnoreCase(RowLevelOperationMode.MERGE_ON_READ.modeName())) {
      return RowLevelOperationMode.MERGE_ON_READ;
    } else {
      return RowLevelOperationMode.COPY_ON_WRITE;
    }
  }

  /**
   * searches the iceberg metadata's property list for the desired TableProperty. If property not
   * found, return null. null = presumed default.
   */
  public static TableProperties getDmlWriteProp(DremioTable table, String propertyName) {

    TableProperties writeProp = null;

    List<TableProperties> icebergTableProperties =
        Optional.ofNullable(table)
            .map(DremioTable::getDatasetConfig)
            .map(DatasetConfig::getPhysicalDataset)
            .map(PhysicalDataset::getIcebergMetadata)
            .map(IcebergMetadata::getTablePropertiesList)
            .orElse(Collections.emptyList());

    for (TableProperties prop : icebergTableProperties) {
      if (prop.getTablePropertyName().equalsIgnoreCase(propertyName)) {
        writeProp = prop;
      }
    }
    return writeProp;
  }

  /** Tries to use version specification from the SQL. Otherwise use session's version spec. */
  public static ResolvedVersionContext resolveVersionContextForDml(
      SqlHandlerConfig config, SqlDmlOperator sqlDmlOperator, String sourceName) {
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(sourceName);
    final VersionContext statementVersion =
        DmlUtils.getVersionContext(sqlDmlOperator).asVersionContext();
    final VersionContext context =
        statementVersion != VersionContext.NOT_SPECIFIED ? statementVersion : sessionVersion;
    return resolveVersionContext(config.getContext().getCatalog(), sourceName, context);
  }
}
