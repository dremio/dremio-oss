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
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
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

  public static final String WRITE_DELETE_PROPERTY = "write.delete.mode";

  public static final String WRITE_UPDATE_PROPERTY = "write.update.mode";

  public static final String WRITE_MERGE_PROPERTY = "write.merge.mode";

  public static final String MERGE_ON_READ_WRITE_MODE = "merge-on-read";

  public static SqlNode extendTableWithDataFileSystemColumns(SqlNode table) {
    return extendTableWithColumns(
        table,
        ImmutableMap.of(
            FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR, ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT));
  }

  /**
   * Extends a given SQL table with additional columns. This method creates a new SqlNode
   * representing the extended table. The new table will include all the columns from the original
   * table {@param table} plus the columns specified by the {@param colNames} map. The {@param
   * colNames} map provides a mapping between column names and their data types.
   *
   * @param table the table to be extended
   * @param colNames a map containing the names and data types of the additional columns
   * @return a new SqlNode representing the extended table
   */
  public static SqlNode extendTableWithColumns(SqlNode table, Map<String, SqlTypeName> colNames) {
    if (colNames.isEmpty()) {
      return table;
    }
    SqlParserPos pos = table.getParserPosition();
    SqlNodeList nodes = new SqlNodeList(pos);
    colNames.forEach((name, type) -> addColumn(nodes, pos, name, type));
    return SqlStdOperatorTable.EXTEND.createCall(pos, table, nodes);
  }

  public static NamespaceKey getPath(SqlNode table) {
    if (table.getKind() == SqlKind.EXTEND) {
      table = ((SqlCall) table).getOperandList().get(0);
    }
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

  /** Check if Delete operation is set to Merge-On-Read based on the iceberg table's table-props */
  public static boolean isMergeOnReadDelete(IcebergTableProps icebergTableProps) {
    return Optional.of(icebergTableProps)
        .filter(props -> props.getIcebergOpType() == IcebergCommandType.DELETE)
        .map(props -> props.getTableProperties().get(WRITE_DELETE_PROPERTY))
        .map(value -> value.equals(MERGE_ON_READ_WRITE_MODE))
        .orElse(false);
  }

  /** Check if Update operation is set to Merge-On-Read based on the iceberg table's table-props */
  public static boolean isMergeOnReadUpdate(IcebergTableProps icebergTableProps) {
    return Optional.of(icebergTableProps)
        .filter(props -> props.getIcebergOpType() == IcebergCommandType.UPDATE)
        .map(props -> props.getTableProperties().get(WRITE_UPDATE_PROPERTY))
        .map(value -> value.equals(MERGE_ON_READ_WRITE_MODE))
        .orElse(false);
  }

  /** Check if Merge operation is set to Merge-On-Read based on the iceberg table's table-props */
  public static boolean isMergeOnReadMerge(IcebergTableProps icebergTableProps) {
    return Optional.of(icebergTableProps)
        .filter(props -> props.getIcebergOpType() == IcebergCommandType.MERGE)
        .map(props -> props.getTableProperties().get(WRITE_MERGE_PROPERTY))
        .map(value -> value.equals(MERGE_ON_READ_WRITE_MODE))
        .orElse(false);
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

  /** Get the DML RowLevelOperationMode for the given property */
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
   * Get the TableProperties for the given propertyName
   *
   * @param table the table to get the property from
   * @param propertyName the property name to get
   * @return the TableProperties for the given propertyName
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

  /**
   * Build the set of outdated target column names. If the update call exists, use updateColumns.
   * Otherwise, we are in an insert_only case. In the insert_only case, all target columns are
   * outdated and replaced by source.
   *
   * @param updateColumns list of column names references in the update call
   * @param table used to acquire names of all original target table column names.
   * @return a set of column names which represent the target columns to be excluded.
   */
  public static Set<String> getOutdatedTargetColumns(
      Set<String> updateColumns, RelOptTable table, List<String> partitionColumns) {

    // Get set of target column names (does not include target columns)
    if (updateColumns.isEmpty()) {
      return table.getRowType().getFieldNames().stream()
          .filter(f -> !ColumnUtils.isSystemColumn(f))
          .collect(Collectors.toSet());
    }

    Set<String> safePartitionColumns =
        (partitionColumns != null) ? new HashSet<>(partitionColumns) : Collections.emptySet();

    return updateColumns.stream()
        .filter(f -> !safePartitionColumns.contains(f))
        .collect(Collectors.toSet());
  }

  /**
   * Find the number of source (insert) columns
   *
   * <p>insertColCount should be equal to either: <br>
   *
   * <ul>
   *   <li><b>Possibility 1</b>: Target Column Count (excluding system columns)
   *   <li><b>Possibility 2</b>: Zero. This occurs when there is no insert call to begin with
   * </ul>
   *
   * The best way to understand the calculation, is to first acknowledge all the possible components
   * that make up the input RelNode:
   *
   * <ul>
   *   <li><b>Target cols</b>: The Table's orignal data Columns (Appears in Merge-On-Read Only).
   *   <li><b>System cols</b>: 'file_path' and 'pos'.
   *   <li><b>Source cols</b>: The insert columns, if exist.
   *   <li><b>Update cols</b>: The update columns, if exist.
   * </ul>
   *
   * If total input columns compute to these 4 components, then by mathematics associative property,
   * we can calculate the source columns: <b> source = total - target - system - update </b>
   *
   * <p>(Merge On Read Only for Now. Zero Otherwise)... The final piece we must consider is the
   * outdated target columns that may have been removed from the input during the logical plan.
   * Target col count may be incomplete, so, to guarantee we account for all the target columns, we
   * add the outdated column count to compensate for the removed target cols.
   *
   * <p><b> Result: Source Cols = (Total Cols found in Input) - (Dated-Target Cols) + (Outdated
   * Target Cols) - (System Cols) - (Update Cols) </b>
   *
   * @param totalInputColumns the total columns found in the input
   * @param outdatedTargetColumns outdated target cols (<b>Merge-On-Read Only</b>)
   * @param outputTableColumnCount Count of output table's expected columns (if exist). This
   *     consists of target and system columns. Note: Copy-On-Write input notes don't contain target
   *     columns.
   * @param updateColumnCount update columns
   * @return Merge On Read Insert (source) Column Count found in 'input' RelNode.
   */
  public static int calculateInsertColumnCount(
      int totalInputColumns,
      int outdatedTargetColumns,
      int outputTableColumnCount,
      int updateColumnCount) {

    return (totalInputColumns + (outdatedTargetColumns))
        - outputTableColumnCount
        - updateColumnCount;
  }

  /** Check the table's write configuration of the dml command. default is 'Copy-on-Write'. */
  public static RowLevelOperationMode getDmlWriteMode(DremioTable dremioTable, SqlKind dmlKind) {
    List<TableProperties> tableProperties =
        Optional.ofNullable(dremioTable)
            .map(DremioTable::getDatasetConfig)
            .map(DatasetConfig::getPhysicalDataset)
            .map(PhysicalDataset::getIcebergMetadata)
            .map(IcebergMetadata::getTablePropertiesList)
            .orElse(Collections.emptyList());

    final String dmlWriteModePropertyKey = "write." + dmlKind.toString().toLowerCase() + ".mode";
    for (TableProperties prop : tableProperties) {
      if (prop.getTablePropertyName().equalsIgnoreCase(dmlWriteModePropertyKey)) {
        if (prop.getTablePropertyValue()
            .equalsIgnoreCase(RowLevelOperationMode.MERGE_ON_READ.modeName())) {
          return RowLevelOperationMode.MERGE_ON_READ;
        } else {
          return RowLevelOperationMode.COPY_ON_WRITE;
        }
      }
    }
    return RowLevelOperationMode.COPY_ON_WRITE;
  }

  /** Check if the operation is a Merge-On-Read operation. */
  public static boolean isMergeOnReadDmlOperation(final WriterOptions writerOptions) {
    return Optional.ofNullable(writerOptions)
        .map(WriterOptions::getTableFormatOptions)
        .map(DmlUtils::isMergeOnReadDmlOperation)
        .orElse(false);
  }

  /** Check if the operation is a Merge-On-Read 'Delete' Operation */
  public static boolean isMergeOnReadDeleteOperation(final WriterOptions writerOptions) {
    return Optional.ofNullable(writerOptions)
        .map(WriterOptions::getTableFormatOptions)
        .map(DmlUtils::isMergeOnReadDeleteOperation)
        .orElse(false);
  }

  /** Check if the operation is a Merge-On-Read operation. */
  public static boolean isMergeOnReadDmlOperation(
      final TableFormatWriterOptions tableFormatWriterOptions) {
    return Optional.ofNullable(tableFormatWriterOptions)
        .map(TableFormatWriterOptions::getIcebergSpecificOptions)
        .map(IcebergWriterOptions::getIcebergTableProps)
        .map(DmlUtils::isMergeOnReadDmlOperation)
        .orElse(false);
  }

  /** Check if the operation is a Merge-On-Read 'Delete' Operation */
  public static boolean isMergeOnReadDeleteOperation(
      final TableFormatWriterOptions tableFormatWriterOptions) {
    return Optional.ofNullable(tableFormatWriterOptions)
        .map(TableFormatWriterOptions::getIcebergSpecificOptions)
        .map(IcebergWriterOptions::getIcebergTableProps)
        .map(DmlUtils::isMergeOnReadDelete)
        .orElse(false);
  }

  /** Check if the operation is a Merge-On-Read operation. */
  public static boolean isMergeOnReadDmlOperation(IcebergTableProps icebergTableProps) {
    return (isMergeOnReadDelete(icebergTableProps)
        || isMergeOnReadUpdate(icebergTableProps)
        || isMergeOnReadMerge(icebergTableProps));
  }
}
