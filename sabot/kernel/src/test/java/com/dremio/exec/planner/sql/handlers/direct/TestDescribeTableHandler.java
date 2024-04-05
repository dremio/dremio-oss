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
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDescribeDremioTable;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.Lists;
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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestDescribeTableHandler extends BaseTestQuery {
  private DescribeTableHandler describeTableHandler;
  private static List<String> TABLE = Collections.singletonList("mytable");
  @Mock private QueryContext queryContext;
  @Mock private Catalog catalog;
  @Mock private DremioTable table;
  @Mock private DatasetConfig datasetConfig;
  @Mock private OptionManager mockOptions;
  @Mock private UserSession session;

  @Before
  public void setup() {
    BaseTestQuery.setSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER, "true");
    describeTableHandler = new DescribeTableHandler(catalog, queryContext, session);

    when(catalog.getTable(new NamespaceKey(TABLE))).thenReturn(table);
    // In the actual code, it will add sourceName at the front. Since it does not have source name
    // in this unit test, we ignore this method.
    when(catalog.resolveSingle(new NamespaceKey(TABLE))).thenReturn(new NamespaceKey(TABLE));
    when(session.getSessionVersionForSource(TABLE.get(0))).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(TABLE)
                .tableVersionContext(new TableVersionContext(TableVersionType.NOT_SPECIFIED, ""))
                .build()))
        .thenReturn(table);
    when(table.getRowType(any())).thenReturn(createTableSchema());
    when(table.getDatasetConfig()).thenReturn(datasetConfig);
    when(datasetConfig.getFullPathList()).thenReturn(TABLE);
    when(catalog.getColumnExtendedProperties(any(DremioTable.class)))
        .thenReturn(generateExtendedProperties());
    when(queryContext.getOptions()).thenReturn(mockOptions);
    when(mockOptions.getOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER)).thenReturn(true);
  }

  @After
  public void reset() {
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER.getOptionName());
  }

  private RelDataType createTableSchema() {
    return new RelRecordType(StructKind.FULLY_QUALIFIED, generateFields(), false);
  }

  private List<RelDataTypeField> generateFields() {
    return Lists.newArrayList(
        new RelDataTypeFieldImpl(
            "col1", 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR)),
        new RelDataTypeFieldImpl(
            "col2", 1, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BOOLEAN)),
        new RelDataTypeFieldImpl(
            "col3", 2, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, 5, 2)));
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
    SqlDescribeDremioTable describeTable =
        new SqlDescribeDremioTable(
            SqlParserPos.ZERO,
            new SqlIdentifier(TABLE, SqlParserPos.ZERO),
            SqlTableVersionSpec.NOT_SPECIFIED,
            null);
    final List<DescribeTableHandler.DescribeResult> actualResults =
        describeTableHandler.toResult("foo", describeTable);

    final List<DescribeTableHandler.DescribeResult> expectedResults =
        Lists.newArrayList(
            new DescribeTableHandler.DescribeResult(
                "col1", "CHARACTER VARYING", null, null, "[]", "[]", null),
            new DescribeTableHandler.DescribeResult(
                "col2", "BOOLEAN", null, null, "[{\"key\":\"one\",\"value\":\"foo\"}]", "[]", null),
            new DescribeTableHandler.DescribeResult(
                "col3",
                "DECIMAL",
                5,
                2,
                "[{\"key\":\"two\",\"value\":\"bar\"},{\"key\":\"three\",\"value\":\"baz\"}]",
                "[]",
                null));

    assertEquals(expectedResults.size(), actualResults.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      verifyDescribeResult(expectedResults.get(i), actualResults.get(i));
    }
  }

  private void verifyDescribeResult(
      DescribeTableHandler.DescribeResult expected, DescribeTableHandler.DescribeResult actual) {
    assertEquals(expected.COLUMN_NAME, actual.COLUMN_NAME);
    assertEquals(expected.DATA_TYPE, actual.DATA_TYPE);
    assertEquals(expected.IS_NULLABLE, actual.IS_NULLABLE);
    assertEquals(expected.NUMERIC_PRECISION, actual.NUMERIC_PRECISION);
    assertEquals(expected.NUMERIC_SCALE, actual.NUMERIC_SCALE);
    assertEquals(expected.EXTENDED_PROPERTIES, actual.EXTENDED_PROPERTIES);
    assertEquals(expected.SORT_ORDER_PRIORITY, actual.SORT_ORDER_PRIORITY);
  }
}
