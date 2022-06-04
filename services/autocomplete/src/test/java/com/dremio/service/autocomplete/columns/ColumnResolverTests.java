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
package com.dremio.service.autocomplete.columns;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryTokenizer;
import com.dremio.service.autocomplete.catalog.mock.MockCatalog;
import com.dremio.service.autocomplete.catalog.mock.MockDremioQueryParser;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTable;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTableFactory;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for context aware auto completions.
 */
public final class ColumnResolverTests {
  private static final ColumnResolver COLUMN_RESOLVER = createColumnResolver(
    new ImmutableMap.Builder<String, ImmutableList<MockSchemas.ColumnSchema>>()
      .put("EMP", MockSchemas.EMP)
      .put("DEPT", MockSchemas.DEPT)
      .build());

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(ColumnResolverTests::executeTest)
      .add(
        "SIMPLE FROM CLAUSE",
        "FROM EMP")
      .add(
        "FROM CLAUSE WITH COMMAS",
        "FROM EMP, DEPT")
      .add(
        "FROM CLAUSE WITH JOINS",
        "FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO")
      .runTests();
  }

  private static Map<List<String>, List<ColumnForBaseline>> executeTest(String fromClause) {
    List<DremioToken> fromClauseTokens = SqlQueryTokenizer.tokenize(fromClause);
    Map<List<String>, Set<Column>> columnResolution = COLUMN_RESOLVER.resolve(ImmutableList.copyOf(fromClauseTokens));
    Map<List<String>, List<ColumnForBaseline>> baselineResult = new HashMap<>();
    for (List<String> path : columnResolution.keySet()) {
      List<ColumnForBaseline> columnsForBaseline = new ArrayList<>();
      for (Column column : columnResolution.get(path)) {
        ColumnForBaseline columnForBaseline = new ColumnForBaseline(column.getName(), column.getType().getSqlTypeName());
        columnsForBaseline.add(columnForBaseline);
      }

      Collections.sort(columnsForBaseline, Comparator.comparing(columnForBaseline -> columnForBaseline.name));

      baselineResult.put(path, columnsForBaseline);
    }

    return baselineResult;
  }

  public static final class ColumnForBaseline {
    private final String name;
    private final SqlTypeName sqlTypeName;

    public ColumnForBaseline(String name, SqlTypeName sqlTypeName) {
      this.name = name;
      this.sqlTypeName = sqlTypeName;
    }

    public String getName() {
      return name;
    }

    public SqlTypeName getSqlTypeName() {
      return sqlTypeName;
    }
  }

  public static ColumnResolver createColumnResolver(ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {
    Preconditions.checkNotNull(tableSchemas);

    MockColumnReader columnReader = createColumnReader(tableSchemas);
    DremioQueryParser queryParser = createQueryParser(tableSchemas);
    ColumnResolver columnResolver = new ColumnResolver(columnReader, queryParser);

    return columnResolver;
  }

  private static MockColumnReader createColumnReader(ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {
    ImmutableMap.Builder<ImmutableList<String>, ImmutableSet<Column>> tables = new ImmutableMap.Builder<>();
    for (String tableName : tableSchemas.keySet()) {
      ImmutableSet.Builder<Column> columnBuilder = new ImmutableSet.Builder<>();
      for (MockSchemas.ColumnSchema columnSchema : tableSchemas.get(tableName)) {
        columnBuilder.add(Column.create(columnSchema.getName(), columnSchema.getSqlTypeName()));
      }

      tables.put(ImmutableList.of(tableName), columnBuilder.build());
    }

    return new MockColumnReader(tables.build());
  }

  private static DremioQueryParser createQueryParser(ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {
    OperatorTable operatorTable = createOperatorTable();
    MockCatalog catalog = createCatalog(tableSchemas);

    DremioQueryParser queryParser = new MockDremioQueryParser(operatorTable, catalog, "user1");
    return queryParser;
  }

  private static OperatorTable createOperatorTable() {
    SabotConfig config = SabotConfig.create();
    ScanResult scanResult = ClassPathScanner.fromPrescan(config);
    FunctionImplementationRegistry functionImplementationRegistry = new FunctionImplementationRegistry(
      config,
      scanResult);
    OperatorTable operatorTable = new OperatorTable(functionImplementationRegistry);

    return operatorTable;
  }

  private static MockCatalog createCatalog(ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {
    ImmutableList.Builder<MockDremioTable> mockDremioTableBuilder = new ImmutableList.Builder<>();
    for (String tableName : tableSchemas.keySet()) {
      MockDremioTable mockDremioTable = MockDremioTableFactory.createFromSchema(
        new NamespaceKey(tableName),
        JavaTypeFactoryImpl.INSTANCE,
        tableSchemas.get(tableName));
      mockDremioTableBuilder.add(mockDremioTable);
    }

    MockCatalog catalog = new MockCatalog(JavaTypeFactoryImpl.INSTANCE, mockDremioTableBuilder.build());
    return catalog;
  }
}
