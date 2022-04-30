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
package com.dremio.service.autocomplete.functions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.Pair;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryTokenizer;
import com.dremio.service.autocomplete.catalog.mock.MockCatalog;
import com.dremio.service.autocomplete.catalog.mock.MockDremioQueryParser;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTable;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTableFactory;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.columns.DremioQueryParser;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for function parameter resolution.
 */
public class ParameterResolverTests {
  public static final SqlFunctionDictionary SQL_FUNCTION_DICTIONARY = new SqlFunctionDictionary(
    new ImmutableList.Builder<SqlFunction>()
      .add(ZeroArgFunction.INSTANCE)
      .add(OneArgBooleanFunction.INSTANCE)
      .add(OneArgNumericFunction.INSTANCE)
      .add(TwoArgNumericFunction.INSTANCE)
      .add(UnstableReturnTypeFunction.INSTANCE)
      .add(VaradicFunction.INSTANCE)
      .add(OverloadedFunction.INSTANCE)
      .build());

  private static final ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> TABLE_SCHEMAS = new ImmutableMap.Builder<String, ImmutableList<MockSchemas.ColumnSchema>>()
    .put("MOCK_TABLE", MockSchemas.MIXED_TYPE_SCHEMA)
    .build();

  private static final ImmutableSet<Column> COLUMNS = createColumns(TABLE_SCHEMAS);

  private static final ImmutableList<DremioToken> FROM_CLAUSE_TOKENS = new ImmutableList.Builder<DremioToken>()
    .add(new DremioToken(ParserImplConstants.FROM, "FROM"))
    .add(new DremioToken(ParserImplConstants.IDENTIFIER, "MOCK_TABLE"))
    .build();

  private static final ParameterResolver PARAMETER_RESOLVER = createParameterResolver(
    SQL_FUNCTION_DICTIONARY,
    TABLE_SCHEMAS);

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(ParameterResolverTests::executeTest)
      .add("NO FUNCTION", "SELECT ")
      .add("COMPLETED FUNCTION", "ZERO_ARG_FUNCTION()")
      .add("NO PARAMETER FUNCTION", "ZERO_ARG_FUNCTION(")
      .add("ONE PARAMETER FUNCTION AND ONE SIGNATURE", "ONE_ARG_BOOLEAN_FUNCTION(")
      .add("ONE PARAMETER BUT MULTIPLE SIGNATURES", "ONE_ARG_NUMERIC_FUNCTION(")
      .add("ONE PARAMETER FUNCTION WITH PARAMETER FILLED IN", "ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN")
      .add("ONE PARAMETER FUNCTION WITH LITERAL", "ONE_ARG_NUMERIC_FUNCTION(42")
      .add("TWO PARAMETER FUNCTION", "TWO_ARG_NUMERIC_FUNCTION(")
      .add("TWO PARAMETER FUNCTION WITH ONE PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, ")
      .add("TWO PARAMETER FUNCTION WITH ONE LITERAL IN", "TWO_ARG_NUMERIC_FUNCTION(42, ")
      .add("TWO PARAMETER FUNCTION WITH TWO PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, INTEGER_COLUMN")
      .add("TWO PARAMETER FUNCTION WITH TWO LITERAL FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(42, 1337")
      .add("TWO PARAMETER FUNCTION WITH PARAMETER THEN LITERAL FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, 1337")
      .add("TWO PARAMETER FUNCTION WITH LITERAL THEN PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(1337, INTEGER_COLUMN")
      .add("NESTED FUNCTION WITH INCOMPLETE FUNCTION", "ONE_ARG_BOOLEAN_FUNCTION(ONE_ARG_BOOLEAN_FUNCTION(")
      .add("NESTED FUNCTION WITH COMPLETE FUNCTION", "TWO_ARG_NUMERIC_FUNCTION(ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN), ")
      .add("NESTED FUNCTION WITH COMPLETE FUNCTION 2", "TWO_ARG_NUMERIC_FUNCTION(ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN), INTEGER_COLUMN ")
      .add("NESTED FUNCTION WITH UNSTABLE RETURN TYPE 1", "TWO_ARG_NUMERIC_FUNCTION(UNSTABLE_RETURN_TYPE_FUNCTION(INTEGER_COLUMN), INTEGER_COLUMN ")
      .add("NESTED FUNCTION WITH UNSTABLE RETURN TYPE 2", "TWO_ARG_NUMERIC_FUNCTION(UNSTABLE_RETURN_TYPE_FUNCTION(DOUBLE_COLUMN), INTEGER_COLUMN ")
      .add("VARADIC FUNCTION NO PARAMETERS", "VARADIC_FUNCTION(")
      .add("VARADIC FUNCTION ONE PARAMETER", "VARADIC_FUNCTION(VARCHAR_COLUMN, ")
      .add("VARADIC FUNCTION TWO PARAMETER BUT 3RD IS OPTIONAL", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN")
      .add("VARADIC FUNCTION TWO PARAMETER BUT 3RD IS REQUIRED", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN, ")
      .add("VARADIC FUNCTION WITH MORE THAN MIN NUMBER OF PARAMETERS", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN, VARCHAR_COLUMN, ")
      .add("OVERLOADED FUNCTION WITH NO PARAMETERS", "OVERLOADED_FUNCTION( ")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 1", "OVERLOADED_FUNCTION(BINARY_COLUMN, ")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 2", "OVERLOADED_FUNCTION(DATE_COLUMN, ")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 3", "OVERLOADED_FUNCTION(BOOLEAN_COLUMN, ")
      .add("COMPLEX CALL 1", "TWO_ARG_NUMERIC_FUNCTION(2 + 2, ")
      .add("COMPLEX CALL 2", "TWO_ARG_NUMERIC_FUNCTION(2 + ONE_ARG_NUMERIC_FUNCTION(2), ")
      .runTests();
  }

  private static Output executeTest(String queryCorpus) {
    assert queryCorpus != null;

    ImmutableList<DremioToken> tokens = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(queryCorpus));
    Optional<Pair<ParameterResolver.Resolutions, ParameterResolver.DebugInfo>> resolutions = PARAMETER_RESOLVER.resolveWithDebugInfo(
      tokens,
      COLUMNS,
      FROM_CLAUSE_TOKENS);
    if (!resolutions.isPresent()) {
      return null;
    }

    return Output.create(resolutions.get().left, resolutions.get().right);
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

  private static OperatorTable createOperatorTable(SqlFunctionDictionary sqlFunctionDictionary) {
    SabotConfig config = SabotConfig.create();
    ScanResult scanResult = ClassPathScanner.fromPrescan(config);
    FunctionImplementationRegistry functions = new FunctionImplementationRegistry(
      config,
      scanResult);
    OperatorTable operatorTable = new OperatorTable(functions);
    for (String functionName : sqlFunctionDictionary.getKeys()) {
      operatorTable.add(functionName, sqlFunctionDictionary.tryGetValue(functionName).get());
    }

    return operatorTable;
  }

  private static SqlValidatorImpl createSqlValidator(OperatorTable operatorTable, MockCatalog catalog) {
    SqlValidatorImpl sqlValidator = new SqlAdvisorValidator(
      operatorTable,
      new DremioCatalogReader(catalog, JavaTypeFactoryImpl.INSTANCE),
      JavaTypeFactoryImpl.INSTANCE,
      SqlValidator.Config.DEFAULT);

    return sqlValidator;
  }

  private static ImmutableSet<Column> createColumns(ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {
    ImmutableSet.Builder<Column> builder = new ImmutableSet.Builder<>();
    for (String tableName : tableSchemas.keySet()) {
      ImmutableList<MockSchemas.ColumnSchema> tableSchema = tableSchemas.get(tableName);
      for (MockSchemas.ColumnSchema columnSchema : tableSchema) {
        builder.add(new Column(columnSchema.getName(), JavaTypeFactoryImpl.INSTANCE.createSqlType(columnSchema.getSqlTypeName())));
      }
    }

    return builder.build();
  }

  public static ParameterResolver createParameterResolver(
    SqlFunctionDictionary sqlFunctionDictionary,
    ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> tableSchemas) {

    MockCatalog catalog = createCatalog(tableSchemas);
    OperatorTable operatorTable = createOperatorTable(sqlFunctionDictionary);
    SqlValidatorImpl sqlValidator = createSqlValidator(operatorTable, catalog);
    DremioQueryParser queryParser = new MockDremioQueryParser(operatorTable, catalog, "user1");
    ParameterResolver parameterResolver = new ParameterResolver(
      sqlFunctionDictionary,
      sqlValidator,
      new MockScope(sqlValidator),
      queryParser);

    return parameterResolver;
  }

  /**
   * Output type for the baseline file.
   */
  public static final class Output {
    private final List<String> columns;
    private final List<String> functions;
    private final ParameterResolver.DebugInfo debugInfo;

    public Output(
      List<String> columns,
      List<String> functions,
      ParameterResolver.DebugInfo debugInfo) {
      this.columns = columns;
      this.functions = functions;
      this.debugInfo = debugInfo;
    }

    public static Output create(ParameterResolver.Resolutions resolutions, ParameterResolver.DebugInfo debugInfo) {
      return new Output(
        resolutions.getColumns().stream().map(column -> column.getName()).sorted().collect(Collectors.toList()),
        resolutions.getFunctions().stream().map(functionSpec -> functionSpec.getName()).sorted().collect(Collectors.toList()),
        debugInfo);
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<String> getFunctions() {
      return functions;
    }

    public ParameterResolver.DebugInfo getDebugInfo() {
      return debugInfo;
    }
  }

  private static final class MockScope extends DremioEmptyScope {

    public MockScope(SqlValidatorImpl validator) {
      super(validator);
    }

    @Override
    public RelDataType resolveColumn(String name, SqlNode ctx) {
      SqlIdentifier identifier = (SqlIdentifier) ctx;
      DremioCatalogReader dremioCatalogReader = (DremioCatalogReader) this.validator.getCatalogReader();

      RelDataType tableSchema = dremioCatalogReader
        .getTableSchema(identifier.names.subList(0, identifier.names.size() - 1))
        .get();

      return tableSchema;
    }
  }
}
