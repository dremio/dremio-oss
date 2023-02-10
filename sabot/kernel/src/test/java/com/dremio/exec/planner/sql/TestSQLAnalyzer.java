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
package com.dremio.exec.planner.sql;

import static org.apache.calcite.sql.validate.SqlMonotonicity.CONSTANT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Tests {@link com.dremio.exec.planner.sql.SQLAnalyzer}
 */
public class TestSQLAnalyzer {

  private SQLAnalyzer sqlAnalyzer;

  protected static final String TEST_CATALOG = "TEST_CATALOG";
  protected static final String TEST_SCHEMA = "TEST_SCHEMA";
  protected static final String TEST_TABLE = "TEST_TABLE";
  protected static final List<String> FROM_KEYWORDS = Arrays.asList("(", "LATERAL", "TABLE", "UNNEST");

  @BeforeEach
  public void setup() {
    // Create and Mock dependencies
    final SabotContext sabotContext = mock(SabotContext.class);
    final FunctionImplementationRegistry functionImplementationRegistry = mock(FunctionImplementationRegistry.class);
    final JavaTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;

    // Stub necessary methods
    when(sabotContext.getFunctionImplementationRegistry()).thenReturn(functionImplementationRegistry);

    // Utilize custom catalog reader implementation to return specific suggestions,
    // without requiring server startup
    MockCatalogReader mockCatalogReader = new MockCatalogReader(typeFactory, false);
    SqlValidatorWithHints validator =  new SqlAdvisorValidator(new OperatorTable(sabotContext.getFunctionImplementationRegistry()),
      mockCatalogReader, typeFactory, SqlAdvisorValidator.Config.DEFAULT.withSqlConformance(DremioSqlConformance.INSTANCE));
    sqlAnalyzer = new SQLAnalyzer(validator);
  }

  /**
   * Values to be used for parameterized tests. Contains the following:
   * - Query to be used for suggestion with ^ indicating position of the cursor.
   * - The expected number of returned suggestions given the MockCatalogReader implementation.
   * - Boolean indicating if suggestion values should be checked.
   */
  private static Stream<Arguments> data() {
    return Stream.of(
      // Cursor after 'from'
      Arguments.of(
        "Select * from ^",
        7,
        true),
      // Cursor after 'T'
      Arguments.of(
        "select * from T^",
        4,
        true),
      // Cursor after 'I'
      Arguments.of(
        "select * from I^",
        0,
        true),
      // Cursor before 'dummy a'
      Arguments.of(
        "select a.colOne, b.colTwo from ^dummy a, TEST_SCHEMA.dummy b",
        7,
        true),
      // Cursor after 'from'
      Arguments.of(
        "select a.colOne, b.colTwo from ^",
        7,
        true),
      // Cursor after 'from'
      Arguments.of(
        "select a.colOne, b.colTwo from ^, TEST_SCHEMA.dummy b",
        7,
        true),
      // Cursor after 'from' before 'a'
      Arguments.of(
        "select a.colOne, b.colTwo from ^a",
        7,
        true),
      // Cursor before 'TEST_SCHEMA'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a, ^TEST_SCHEMA.dummy b",
        7,
        true),
      // Cursor after 'TEST_SCHEMA.'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a, TEST_SCHEMA.^",
        3,
        true),
      // Cursor after 'group'
      Arguments.of(
        "select a.colOne, b.colTwo from emp group ^",
        1,
        false),
      // Cursor before 'dummy a'
      Arguments.of(
        "select a.colOne, b.colTwo from ^dummy a join TEST_SCHEMA.dummy b "
          + "on a.colOne=b.colOne where colTwo=1",
        7,
        true),
      // Cursor after 'from' before 'a'
      Arguments.of(
        "select a.colOne, b.colTwo from ^ a join sales.dummy b",
        7,
        true),
      // Cursor before 'TEST_SCHEMA.dummy b'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a join ^TEST_SCHEMA.dummy b "
          + "on a.colTwo=b.colTwo where colOne=1",
        7,
        true),
      // Cursor after 'TEST_SCHEMA.'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a join TEST_SCHEMA.^",
        3,
        true),
      // Cursor after 'TEST_SCHEMA.'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a join TEST_SCHEMA.^ on",
        3,
        true),
      // Cursor after 'TEST_SCHEMA.'
      Arguments.of(
        "select a.colOne, b.colTwo from dummy a join TEST_SCHEMA.^ on a.colTwo=",
        108,
        false),
      // Cursor after 'TEST_CATALOG.TEST_SCHEMA'
      Arguments.of(
        "select * from dummy join TEST_CATALOG.TEST_SCHEMA ^",
        32,
        false)
    );
  }

  /**
   * Check that the returned suggestions list contains the expected hints.
   *
   * @param suggestions The list of query hints
   */
  private void assertSuggestions(List<SqlMoniker> suggestions) {
    for (SqlMoniker hint : suggestions) {
      switch (hint.getType()) {
        case CATALOG:
          assertEquals(TEST_CATALOG ,hint.getFullyQualifiedNames().get(0));
          break;
        case SCHEMA:
          assertEquals(TEST_SCHEMA ,hint.getFullyQualifiedNames().get(0));
          break;
        case TABLE:
          assertEquals(TEST_TABLE ,hint.getFullyQualifiedNames().get(0));
          break;
        case KEYWORD:
          assertTrue(FROM_KEYWORDS.contains(hint.getFullyQualifiedNames().get(0)));
          break;
        default:
          Assertions.fail();
      }
    }
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("data")
  public void testSuggestion(String sql, int expectedSuggestionCount, boolean checkSuggestions) {
    final StringAndPos stringAndPos = SqlParserUtil.findPos(sql);
    List<SqlMoniker> suggestions = sqlAnalyzer.suggest(stringAndPos.sql, stringAndPos.cursor);
    assertEquals(expectedSuggestionCount, suggestions.size());
    if (checkSuggestions) {
      assertSuggestions(suggestions);
    }
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("data")
  public void testValidation(String sql, int expectedSuggestionCount, boolean checkSuggestions) {
    List<SqlAdvisor.ValidateErrorInfo> validationErrors = sqlAnalyzer.validate("select * from");
    assertEquals(1, validationErrors.size());
    assertEquals(10, validationErrors.get(0).getStartColumnNum());
    assertEquals(13, validationErrors.get(0).getEndColumnNum());
  }

  /**
   * Custom catalog reader to replace {@link org.apache.calcite.prepare.CalciteCatalogReader} in the
   * instantiation of the SQL validator.
   */
  class MockCatalogReader implements Prepare.CatalogReader {

    protected final RelDataTypeFactory typeFactory;
    private final boolean caseSensitive;
    private ImmutableList<ImmutableList<String>> schemaPaths;

    /**
     * Creates a MockCatalogReader.
     */
    public MockCatalogReader(RelDataTypeFactory typeFactory,
                             boolean caseSensitive) {
      this(typeFactory, caseSensitive, ImmutableList.of(ImmutableList.of()));
    }

    private MockCatalogReader(RelDataTypeFactory typeFactory,
        boolean caseSensitive, ImmutableList<ImmutableList<String>> schemaPaths) {
      this.typeFactory = typeFactory;
      this.caseSensitive = caseSensitive;
      this.schemaPaths = schemaPaths;
    }

    @Override
    public Prepare.PreparingTable getTableForMember(List<String> names) {
      return getTable(names);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override
    public void registerRules(RelOptPlanner planner) throws Exception {
      // Do nothing
    }

    @Override
    public Prepare.CatalogReader withSchemaPath(List<String> schemaPath) {
      ImmutableList<String> immutableSchemaPath = ImmutableList.copyOf(schemaPath);
      return new MockCatalogReader(typeFactory, caseSensitive,
          ImmutableList.<ImmutableList<String>> copyOf(Iterables.concat(this.schemaPaths, ImmutableList.of(immutableSchemaPath))));
    }

    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
      // Create table used for suggestion
      if (names.contains("TEST_TABLE")) {
        MockTable mockTable = new MockTable("TEST_TABLE", this);
        mockTable.addColumn("colOne", typeFactory.createSqlType(SqlTypeName.INTEGER));
        mockTable.addColumn("colTwo", typeFactory.createSqlType(SqlTypeName.INTEGER));
        return mockTable;
      }

      return null;
    }

    @Override
    public RelDataType getNamedType(SqlIdentifier typeName) {
      return null;
    }

    @Override
    public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
      final List<SqlMoniker> result = new ArrayList<>();
      result.add(new SqlMonikerImpl(Arrays.asList("TEST_CATALOG"), SqlMonikerType.CATALOG));
      result.add(new SqlMonikerImpl(Arrays.asList("TEST_SCHEMA"), SqlMonikerType.SCHEMA));
      result.add(new SqlMonikerImpl(Arrays.asList("TEST_TABLE"), SqlMonikerType.TABLE));
      return result;
    }

    @Override
    public List<List<String>> getSchemaPaths() {
      return (List<List<String>>) (Object) schemaPaths;
    }

    @Override
    public RelDataTypeField field(RelDataType rowType, String alias) {
      return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
    }

    @Override
    public boolean matches(String string, String name) {
      return Util.matches(caseSensitive, string, name);
    }

    @Override
    public RelDataType createTypeFromProjection(RelDataType type, List<String> columnNameList) {
      return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, caseSensitive);
    }

    @Override
    public boolean isCaseSensitive() {
      return false;
    }

    @Override
    public CalciteSchema getRootSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CalciteConnectionConfig getConfig() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SqlNameMatcher nameMatcher() {
      return SqlNameMatchers.withCaseSensitive(caseSensitive);
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
                                        List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
      // Do nothing
    }

    @Override
    public List<SqlOperator> getOperatorList() {
      return null;
    }

    /**
     * Mock implementation of
     * {@link org.apache.calcite.prepare.Prepare.PreparingTable}.
     */
    public class MockTable implements Prepare.PreparingTable {
      protected final MockCatalogReader catalogReader;
      protected final List<Map.Entry<String, RelDataType>> columnList =
        new ArrayList<>();
      protected RelDataType rowType;
      protected final List<String> names;

      public MockTable(String name, MockCatalogReader catalogReader) {
        this.names = ImmutableList.of("TEST_CATALOG", "TEST_SCHEMA", name);
        this.catalogReader = catalogReader;
      }

      @Override
      public RelOptSchema getRelOptSchema() {
        return catalogReader;
      }

      @Override
      public RelNode toRel(ToRelContext context) {
        return LogicalTableScan.create(context.getCluster(), this, ImmutableList.of());
      }

      @Override
      public List<RelCollation> getCollationList() {
        return null;
      }

      @Override
      public RelDistribution getDistribution() {
        return RelDistributions.ANY;
      }

      @Override
      public boolean isKey(ImmutableBitSet columns) {
        return false;
      }

      @Override
      public List<ImmutableBitSet> getKeys() {
        return ImmutableList.of();
      }

      @Override
      public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
          return clazz.cast(this);
        }
        return null;
      }

      @Override
      public RelDataType getRowType() {
        return rowType;
      }

      @Override
      public List<String> getQualifiedName() {
        return names;
      }

      @Override
      public double getRowCount() {
        return 0;
      }

      @Override
      public SqlMonotonicity getMonotonicity(String columnName) {
        return CONSTANT;
      }

      @Override
      public SqlAccessType getAllowedAccess() {
        return SqlAccessType.ALL;
      }

      @Override
      public boolean supportsModality(SqlModality modality) {
        return false;
      }

      @Override
      public boolean isTemporal() {
        return false;
      }

      @Override
      public Expression getExpression(Class clazz) {
        throw new UnsupportedOperationException();
      }

      public void addColumn(String name, RelDataType type) {
        columnList.add(Pair.of(name, type));
      }

      @Override
      public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return this;
      }

      @Override
      public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
      }

      @Override
      public List<ColumnStrategy> getColumnStrategies() {
        return ImmutableList.of();
      }

      @Override
      public boolean columnHasDefaultValue(RelDataType rowType, int ordinal, InitializerContext initializerContext) {
        return false;
      }
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
      if (aClass.isInstance(this)) {
        return aClass.cast(this);
      }
      return null;
    }
  }
}
