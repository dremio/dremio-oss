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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.exec.store.ischema.Column;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

public class DescribeTableHandler implements SqlDirectHandler<DescribeTableHandler.DescribeResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DescribeTableHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final DremioCatalogReader catalog;
  protected final QueryContext context;

  public DescribeTableHandler(DremioCatalogReader catalog, QueryContext context) {
    super();
    this.catalog = catalog;
    this.context = context;
  }

  @Override
  public List<DescribeResult> toResult(String sql, SqlNode sqlNode) throws RelConversionException, ForemanSetupException {
    final SqlDescribeTable node = SqlNodeUtil.unwrap(sqlNode, SqlDescribeTable.class);

    try {
      final SqlIdentifier tableId = node.getTable();
      final NamespaceKey path = new NamespaceKey(tableId.names);
      final DremioPrepareTable table = catalog.getTableUnchecked(tableId.names);
      final RelDataType type;
      if (table == null || table.getTable() == null) {
        throw UserException.validationError()
          .message("Unknown table [%s]", path)
          .build(logger);
      } else {
        type = table.getRowType();
      }

      List<DescribeResult> columns = new ArrayList<>();
      String columnName = null;
      final SqlIdentifier sqlColumn = node.getColumn();
      if (sqlColumn != null) {
        final List<String> names = sqlColumn.names;
        if (names.size() > 1) {
          throw UserException.validationError().message("You can only describe single component paths. You tried to describe [%s].", Joiner.on('.').join(names)).build(logger);
        }
        columnName = sqlColumn.getSimple();
      }

      final Map<String, List<ColumnExtendedProperty>> extendedPropertyColumns = catalog.getColumnExtendedProperties(table.getTable());

      for (RelDataTypeField field : type.getFieldList()) {
        Column column = new Column("dremio", path.getParent().toUnescapedString(), path.getLeaf(), field);
        if (columnName == null || columnName.equals(field.getName())) {
          final Integer precision = column.NUMERIC_PRECISION;
          final Integer scale = column.NUMERIC_SCALE;
          final List<ColumnExtendedProperty> columnExtendedProperties = getColumnExtendedProperties(column.COLUMN_NAME, extendedPropertyColumns);
          final String extendedPropertiesString = columnExtendedPropertiesToString(columnExtendedProperties);
          String columnPolicies = getColumnPoliciesForColumn(path, column.COLUMN_NAME);
          DescribeResult describeResult = new DescribeResult(field.getName(), column.DATA_TYPE, precision, scale, extendedPropertiesString, columnPolicies);
          columns.add(describeResult);
        }
      }
      return columns;
    } catch (AccessControlException e) {
      throw UserException.permissionError().message("Not authorized to describe table.").build(logger);
    } catch (Exception ex) {
      throw UserException.planError(ex)
          .message("Error while rewriting DESCRIBE query: %s", ex.getMessage())
          .build(logger);
    }
  }

  private List<ColumnExtendedProperty> getColumnExtendedProperties(String columnName, Map<String, List<ColumnExtendedProperty>> extendedPropertyColumns) {
    if (extendedPropertyColumns == null || !extendedPropertyColumns.containsKey(columnName)) {
      return Collections.emptyList();
    } else {
      return extendedPropertyColumns.get(columnName);
    }
  }

  private String columnExtendedPropertiesToString(List<ColumnExtendedProperty> columnExtendedProperties) {
    if (columnExtendedProperties == null) {
      return "[]";
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(columnExtendedProperties);
    } catch (JsonProcessingException jpe) {
      logger.warn("Unable to JSON encode column extended properties.", jpe);
      return "[]";
    }
  }

  @Nullable
  protected String getColumnPoliciesForColumn(NamespaceKey key, String columnName) throws NamespaceException {
    return null;
  }

  public static DescribeTableHandler create(DremioCatalogReader dremioCatalogReader, QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseDescribeTableHandler");
      final Constructor<?> ctor = cl.getConstructor(DremioCatalogReader.class, QueryContext.class);
      return (DescribeTableHandler) ctor.newInstance(dremioCatalogReader, context);
    } catch (ClassNotFoundException e) {
      return new DescribeTableHandler(dremioCatalogReader, context);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  public static class DescribeResult {
    public final String COLUMN_NAME;
    public final String DATA_TYPE;
    public final String IS_NULLABLE = "YES";
    public final Integer NUMERIC_PRECISION;
    public final Integer NUMERIC_SCALE;
    public final String EXTENDED_PROPERTIES;
    public final String MASKING_POLICY;

    public DescribeResult(String columnName,
      String dataType,
      Integer numericPrecision,
      Integer numericScale,
      String extendedProperties,
      String columnPolicies) {
      super();
      COLUMN_NAME = columnName;
      DATA_TYPE = dataType;
      NUMERIC_PRECISION = numericPrecision;
      NUMERIC_SCALE = numericScale;
      EXTENDED_PROPERTIES = extendedProperties;
      MASKING_POLICY = columnPolicies;
    }
  }

  @Override
  public Class<DescribeResult> getResultType() {
    return DescribeResult.class;
  }

}
