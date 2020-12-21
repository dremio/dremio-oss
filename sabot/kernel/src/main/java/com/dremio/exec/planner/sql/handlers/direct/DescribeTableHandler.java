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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.store.ischema.Column;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Joiner;

public class DescribeTableHandler implements SqlDirectHandler<DescribeTableHandler.DescribeResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DescribeTableHandler.class);

  private final DremioCatalogReader catalog;

  public DescribeTableHandler(DremioCatalogReader catalog) {
    super();
    this.catalog = catalog;
  }

  @Override
  public List<DescribeResult> toResult(String sql, SqlNode sqlNode) throws RelConversionException, ForemanSetupException {
    final SqlDescribeTable node = SqlNodeUtil.unwrap(sqlNode, SqlDescribeTable.class);

    try {
      final SqlIdentifier tableId = node.getTable();
      final NamespaceKey path = new NamespaceKey(tableId.names);
      DremioPrepareTable table = catalog.getTable(tableId.names);
      if(table == null) {
        throw UserException.validationError()
        .message("Unknown table [%s]", path)
        .build(logger);
      }

      final RelDataType type = table.getRowType();
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
        Column c = new Column("dremio", path.getParent().toUnescapedString(), path.getLeaf(), field);
        if(column == null || column.equals(field.getName())){
          Integer precision = c.NUMERIC_PRECISION;
          Integer scale = c.NUMERIC_SCALE;
          DescribeResult r = new DescribeResult(field.getName(), c.DATA_TYPE, precision, scale);
          columns.add(r);
        }
      }
      return columns;
    } catch (Exception ex) {
      throw UserException.planError(ex)
          .message("Error while rewriting DESCRIBE query: %s", ex.getMessage())
          .build(logger);
    }
  }

  public static class DescribeResult {
    public final String COLUMN_NAME;
    public final String DATA_TYPE;
    public final String IS_NULLABLE = "YES";
    public final Integer NUMERIC_PRECISION;
    public final Integer NUMERIC_SCALE;

    public DescribeResult(String columnName, String dataType, Integer nPrecision, Integer nScale) {
      super();
      COLUMN_NAME = columnName;
      DATA_TYPE = dataType;
      NUMERIC_PRECISION = nPrecision;
      NUMERIC_SCALE = nScale;
    }


  }


  @Override
  public Class<DescribeResult> getResultType() {
    return DescribeResult.class;
  }

}
