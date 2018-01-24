/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Util;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.store.ischema.tables.ColumnsTable;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.google.common.base.Joiner;

public class DescribeTableHandler implements SqlDirectHandler<DescribeTableHandler.DescribeResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DescribeTableHandler.class);

  private SchemaPlus defaultSchema;

  public DescribeTableHandler(SchemaPlus defaultSchema) {
    super();
    this.defaultSchema = defaultSchema;
  }

  @Override
  public List<DescribeResult> toResult(String sql, SqlNode sqlNode) throws RelConversionException, ForemanSetupException {
    final SqlDescribeTable node = SqlNodeUtil.unwrap(sqlNode, SqlDescribeTable.class);

    try {
      final SqlIdentifier table = node.getTable();
      final List<String> schemaPathGivenInCmd = Util.skipLast(table.names);
      final SchemaPlus schema = SchemaUtilities.findSchema(defaultSchema, schemaPathGivenInCmd);

      if (schema == null) {
        SchemaUtilities.throwSchemaNotFoundException(defaultSchema,
            SchemaUtilities.SCHEMA_PATH_JOINER.join(schemaPathGivenInCmd));
      }

      if (SchemaUtilities.isRootSchema(schema)) {
        throw UserException.validationError()
            .message("No schema selected.")
            .build(logger);
      }

      final String tableName = Util.last(table.names);

      // find resolved schema path
      final String schemaPath = SchemaUtilities.unwrapAsSchemaInstance(schema).getFullSchemaName();

      final Table tableObject = schema.getTable(tableName);
      if (tableObject == null) {
        throw UserException.validationError()
            .message("Unknown table [%s] in schema [%s]", tableName, schemaPath)
            .build(logger);
      }

      final RelDataType type = tableObject.getRowType(new JavaTypeFactoryImpl());
      List<DescribeResult> columns = new ArrayList<>();
      String column = null;
      final SqlIdentifier col = node.getColumn();
      if(col != null){
        final List<String> names = col.names;
        if(names.size() > 1){
          throw UserException.validationError().message("You can only describe single component paths. You tried to describe [%s].", Joiner.on('.').join(names)).build(logger);
        }
        column = col.getSimple();
      }

      for(RelDataTypeField field : type.getFieldList()){
        ColumnsTable.Column c = new ColumnsTable.Column("dremio", schema.getName(), tableName, field);
        if(column == null || column.equals(field.getName())){
          DescribeResult r = new DescribeResult(field.getName(), c.DATA_TYPE);
          columns.add(r);
        }
      }
      return columns;
    } catch (Exception ex) {
      throw UserException.planError(ex)
          .message("Error while rewriting DESCRIBE query: %d", ex.getMessage())
          .build(logger);
    }
  }

  public static class DescribeResult {
    public final String COLUMN_NAME;
    public final String DATA_TYPE;
    public final String IS_NULLABLE = "YES";

    public DescribeResult(String columnName, String dataType) {
      super();
      COLUMN_NAME = columnName;
      DATA_TYPE = dataType;
    }


  }


  @Override
  public Class<DescribeResult> getResultType() {
    return DescribeResult.class;
  }

}
