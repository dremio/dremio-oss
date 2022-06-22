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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class TestDescribeTableHandler {
  private DescribeTableHandler describeTableHandler;
  @Mock private QueryContext queryContext;
  @Mock private DremioCatalogReader catalog;
  @Mock private DremioPrepareTable table;

  @Before
  public void setup() {
    describeTableHandler = new DescribeTableHandler(catalog, queryContext);

    final DremioTable dremioTable = mock(DremioTable.class);
    when(catalog.getTableUnchecked(anyList())).thenReturn(table);
    when(table.getTable()).thenReturn(dremioTable);
    when(table.getRowType()).thenReturn(createTableSchema());
    when(catalog.getColumnExtendedProperties(any(DremioTable.class))).thenReturn(generateExtendedProperties());
  }

  private RelDataType createTableSchema() {
    return new RelRecordType(StructKind.FULLY_QUALIFIED, generateFields(), false);
  }

  private List<RelDataTypeField> generateFields() {
    return Lists.newArrayList(
      new RelDataTypeFieldImpl("col1", 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)),
      new RelDataTypeFieldImpl("col2", 1, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BOOLEAN)),
      new RelDataTypeFieldImpl("col3", 2, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 5, 2))
    );
  }

  private Map<String, List<ColumnExtendedProperty>> generateExtendedProperties() {
    final Map<String, List<ColumnExtendedProperty>> columnMap = new HashMap<>();
    List<ColumnExtendedProperty> extendedProperties = new ArrayList<>();

    extendedProperties.add(new ColumnExtendedProperty("one", AttributeValue.of("foo")));
    columnMap.put("col2", extendedProperties);

    extendedProperties = new ArrayList<>();
    extendedProperties.add(new ColumnExtendedProperty("two", AttributeValue.of("bar")));
    extendedProperties.add(new ColumnExtendedProperty("three", AttributeValue.of("baz")));
    columnMap.put("col3", extendedProperties);

    return columnMap;
  }

  @Test
  public void testToResult() throws Exception {
    SqlDescribeTable describeTable = new SqlDescribeTable(SqlParserPos.ZERO, new SqlIdentifier(Collections.singletonList("mytable"), SqlParserPos.ZERO), null);
    final List<DescribeTableHandler.DescribeResult> actualResults = describeTableHandler.toResult("foo", describeTable);

    final List<DescribeTableHandler.DescribeResult> expectedResults = Lists.newArrayList(
      new DescribeTableHandler.DescribeResult(
        "col1",
        "CHARACTER VARYING",
        null,
        null,
        "[]",
        "[]"),
      new DescribeTableHandler.DescribeResult(
        "col2",
        "BOOLEAN",
        null,
        null,
        "[{\"key\":\"one\",\"value\":\"foo\"}]",
        "[]"),

      new DescribeTableHandler.DescribeResult(
        "col3",
        "DECIMAL",
        5,
        2,
        "[{\"key\":\"two\",\"value\":\"bar\"},{\"key\":\"three\",\"value\":\"baz\"}]",
        "[]")
    );

    assertEquals(expectedResults.size(), actualResults.size());
    for (int i = 0;i < expectedResults.size();i++) {
      verifyDescribeResult(expectedResults.get(i), actualResults.get(i));
    }
  }

  private void verifyDescribeResult(DescribeTableHandler.DescribeResult expected, DescribeTableHandler.DescribeResult actual) {
    assertEquals(expected.COLUMN_NAME, actual.COLUMN_NAME);
    assertEquals(expected.DATA_TYPE, actual.DATA_TYPE);
    assertEquals(expected.IS_NULLABLE, actual.IS_NULLABLE);
    assertEquals(expected.NUMERIC_PRECISION, actual.NUMERIC_PRECISION);
    assertEquals(expected.NUMERIC_SCALE, actual.NUMERIC_SCALE);
    assertEquals(expected.EXTENDED_PROPERTIES, actual.EXTENDED_PROPERTIES);
  }
}
