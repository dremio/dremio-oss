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

import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.ACCELERATOR_STORAGEPLUGIN_NAME;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.ROW_INDEX_COLUMN_NAME;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public class DmlUtils {

  @VisibleForTesting
  public static final Map<TableModify.Operation, String> DML_OUTPUT_COLUMN_NAMES =
    new HashMap<TableModify.Operation, String>(){{
      put(TableModify.Operation.DELETE, "Rows Deleted");
      put(TableModify.Operation.INSERT, "Rows Inserted");
      put(TableModify.Operation.MERGE, "Rows Merged");
      put(TableModify.Operation.UPDATE, "Rows Updated");
    }};

  public static SqlNode extendTableWithDataFileSystemColumns(SqlNode table) {
    SqlParserPos pos = table.getParserPosition();
    SqlNodeList nodes = new SqlNodeList(pos);

    addColumn(nodes, pos, FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR);
    addColumn(nodes, pos, ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT);

    return SqlStdOperatorTable.EXTEND.createCall(pos, table, nodes);
  }

  public static NamespaceKey getPath(SqlNode table) {
    if (table.getKind() == SqlKind.EXTEND) {
      table = ((SqlCall) table).getOperandList().get(0);
    }
    SqlIdentifier tableIdentifier = (SqlIdentifier) table;
    return new NamespaceKey(tableIdentifier.names);
  }

  private static void addColumn(SqlNodeList nodes, SqlParserPos pos, String name, SqlTypeName type) {
    nodes.add(new SqlIdentifier(name, pos));
    nodes.add(new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type, -1, null, pos), pos));
  }

  public static boolean isInsertOperation(final CreateTableEntry createTableEntry) {
    return !ACCELERATOR_STORAGEPLUGIN_NAME.equals(createTableEntry.getPlugin().getId().getName())
      && createTableEntry instanceof CreateParquetTableEntry
      && (createTableEntry.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.INSERT); //TODO: Add CREATE for CTAS with DX-48616
  }

  public static boolean isInsertOperation(final WriterOptions writerOptions) {
    return writerOptions.getTableFormatOptions().getIcebergSpecificOptions()
      .getIcebergTableProps().getIcebergOpType() == IcebergCommandType.INSERT; //TODO: Add CREATE for CTAS with DX-48616
  }

  public static RelDataType evaluateOutputRowType(final RelOptCluster cluster, final TableModify.Operation operation) {
    return cluster.getTypeFactory().builder()
      .add(DML_OUTPUT_COLUMN_NAMES.get(operation),
        SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
          SqlTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.BIGINT),
          true))
      .build();
  }

  /**
   * Returns the validated table path, if one exists.
   *
   * Note: Due to the way the tables get cached, we have to use Catalog.getTableNoResolve, rather than
   * using Catalog.getTable.
   */
  public static NamespaceKey getTablePath(Catalog catalog, NamespaceKey path) {
    NamespaceKey resolvedPath = catalog.resolveToDefault(path);
    DremioTable table = resolvedPath != null ? catalog.getTableNoResolve(resolvedPath) : null;
    if (table != null) {
      // If the returned table type is a `ViewTable`, there's a chance that we got back a view that actually
      // doesn't exist due to bug DX-52808. We should check if the view also gets returned when the path is
      // not resolved. If we find it, that's the correct view.
      if (table instanceof ViewTable) {
        DremioTable maybeViewTable = catalog.getTableNoResolve(path);
        if (maybeViewTable != null) {
          return maybeViewTable.getPath();
        }
      }

      // Since we didn't find a View using just `path`, return the table discovered with the resolved path.
      return table.getPath();
    }

    // Since the table was undiscovered with the resolved path, use `path` and try again.
    table = catalog.getTableNoResolve(path);
    if (table != null) {
      return table.getPath();
    }

    throw UserException.validationError().message("Table [%s] does not exist.", path).buildSilently();
  }
}
