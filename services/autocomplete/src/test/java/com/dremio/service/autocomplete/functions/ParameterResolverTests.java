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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.junit.Test;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.OperatorTableFactory;
import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.catalog.mock.MockMetadataCatalog;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.autocomplete.catalog.mock.NodeMetadata;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.parsing.ValidatingParser;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for function parameter resolution.
 */
public final class ParameterResolverTests {
  public static final ImmutableList<SqlOperator> FUNCTIONS = new ImmutableList.Builder<SqlOperator>()
    .add(ZeroArgFunction.INSTANCE)
    .add(OneArgBooleanFunction.INSTANCE)
    .add(OneArgNumericFunction.INSTANCE)
    .add(TwoArgNumericFunction.INSTANCE)
    .add(UnstableReturnTypeFunction.INSTANCE)
    .add(VaradicFunction.INSTANCE)
    .add(
      OverloadedFunction.TWO_ARG_1,
      OverloadedFunction.TWO_ARG_2,
      OverloadedFunction.THREE_ARG_1,
      OverloadedFunction.THREE_ARG_2)
    .add(
      OptionalArgFunction.ONE_ARG,
      OptionalArgFunction.TWO_ARG,
      OptionalArgFunction.THREE_ARG_1,
      OptionalArgFunction.THREE_ARG_2)
    .build();

  private static final ImmutableSet<Column> COLUMNS = createColumns(MockSchemas.MIXED_TYPE_SCHEMA);

  private static final ImmutableSet<ColumnAndTableAlias> COLUMN_AND_PATHS = ImmutableSet.copyOf(
    COLUMNS
      .stream()
      .map(column -> ColumnAndTableAlias.createWithTable(column, "MOCK_TABLE"))
      .collect(Collectors.toList()));

  private static final FromClause FROM_CLAUSE = FromClause.parse(new ImmutableList.Builder<DremioToken>()
    .add(DremioToken.createFromParserKind(ParserImplConstants.FROM))
    .add(new DremioToken(ParserImplConstants.IDENTIFIER, "MOCK_TABLE"))
    .build());

  private static final ParameterResolver PARAMETER_RESOLVER = createParameterResolver(
    OperatorTableFactory.create(FUNCTIONS),
    new MockMetadataCatalog(
      new MockMetadataCatalog.CatalogData(
        ImmutableList.of(),
        NodeMetadata.pathNode(
          new Node(
            "@dremio",
            Node.Type.HOME),
          NodeMetadata.dataset(
            new Node(
              "MOCK_TABLE",
              Node.Type.PHYSICAL_SOURCE),
            COLUMNS)))));

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(ParameterResolverTests::executeTest)
      .add("NO FUNCTION", "SELECT ^")
      .add("COMPLETED FUNCTION", "ZERO_ARG_FUNCTION(^)")
      .add("NO PARAMETER FUNCTION", "ZERO_ARG_FUNCTION(^")
      .add("ONE PARAMETER FUNCTION AND ONE SIGNATURE", "ONE_ARG_BOOLEAN_FUNCTION(^")
      .add("ONE PARAMETER BUT MULTIPLE SIGNATURES", "ONE_ARG_NUMERIC_FUNCTION(^")
      .add("ONE PARAMETER FUNCTION WITH PARAMETER FILLED IN", "ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN^")
      .add("ONE PARAMETER FUNCTION WITH LITERAL", "ONE_ARG_NUMERIC_FUNCTION(42^")
      .add("TWO PARAMETER FUNCTION", "TWO_ARG_NUMERIC_FUNCTION(^")
      .add("TWO PARAMETER FUNCTION WITH ONE PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, ^")
      .add("TWO PARAMETER FUNCTION WITH ONE LITERAL IN", "TWO_ARG_NUMERIC_FUNCTION(42, ^")
      .add("TWO PARAMETER FUNCTION WITH TWO PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, INTEGER_COLUMN^")
      .add("TWO PARAMETER FUNCTION WITH TWO LITERAL FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(42, 1337^")
      .add("TWO PARAMETER FUNCTION WITH PARAMETER THEN LITERAL FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN, 1337^")
      .add("TWO PARAMETER FUNCTION WITH LITERAL THEN PARAMETER FILLED IN", "TWO_ARG_NUMERIC_FUNCTION(1337, INTEGER_COLUMN^")
      .add("NESTED FUNCTION WITH INCOMPLETE FUNCTION", "ONE_ARG_BOOLEAN_FUNCTION(ONE_ARG_BOOLEAN_FUNCTION(^")
      .add("NESTED FUNCTION WITH COMPLETE FUNCTION", "TWO_ARG_NUMERIC_FUNCTION(ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN), ^")
      .add("NESTED FUNCTION WITH COMPLETE FUNCTION 2", "TWO_ARG_NUMERIC_FUNCTION(ONE_ARG_NUMERIC_FUNCTION(INTEGER_COLUMN), INTEGER_COLUMN^")
      .add("NESTED FUNCTION WITH UNSTABLE RETURN TYPE 1", "TWO_ARG_NUMERIC_FUNCTION(UNSTABLE_RETURN_TYPE_FUNCTION(INTEGER_COLUMN), INTEGER_COLUMN^")
      .add("NESTED FUNCTION WITH UNSTABLE RETURN TYPE 2", "TWO_ARG_NUMERIC_FUNCTION(UNSTABLE_RETURN_TYPE_FUNCTION(DOUBLE_COLUMN), INTEGER_COLUMN^")
      .add("VARADIC FUNCTION NO PARAMETERS", "VARADIC_FUNCTION(^")
      .add("VARADIC FUNCTION ONE PARAMETER", "VARADIC_FUNCTION(VARCHAR_COLUMN, ^")
      .add("VARADIC FUNCTION TWO PARAMETER BUT 3RD IS OPTIONAL", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN^")
      .add("VARADIC FUNCTION TWO PARAMETER BUT 3RD IS REQUIRED", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN, ^")
      .add("VARADIC FUNCTION WITH MORE THAN MIN NUMBER OF PARAMETERS", "VARADIC_FUNCTION(VARCHAR_COLUMN, VARCHAR_COLUMN, VARCHAR_COLUMN, ^")
      .add("OVERLOADED FUNCTION WITH NO PARAMETERS", "OVERLOADED_FUNCTION(^")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 1", "OVERLOADED_FUNCTION(BINARY_COLUMN, ^")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 2", "OVERLOADED_FUNCTION(DATE_COLUMN, ^")
      .add("OVERLOADED FUNCTION WITH ONE PARAMETER 3", "OVERLOADED_FUNCTION(BOOLEAN_COLUMN, ^")
      .add("COMPLEX CALL 1", "TWO_ARG_NUMERIC_FUNCTION(2 + 2, ^")
      .add("COMPLEX CALL 2", "TWO_ARG_NUMERIC_FUNCTION(2 + ONE_ARG_NUMERIC_FUNCTION(2), ^")
      .add("MIDDLE OF A FUNCTION", "OVERLOADED_FUNCTION(BOOLEAN_COLUMN, ^, BINARY_COLUMN)")
      .add("MIDDLE OF A FUNCTION 2", "OVERLOADED_FUNCTION(BOOLEAN_COLUMN, ^, DATE_COLUMN)")
      .add("FUNCTION WITH OPTIONAL ARGUMENT", "OPTIONAL_ARGUMENT_FUNCTION(BINARY_COLUMN, BOOLEAN_COLUMN, ^)")
      .runTests();
  }

  private static Output executeTest(String queryCorpus) {
    assert queryCorpus != null;

    StringAndPos stringAndPos = SqlParserUtil.findPos(queryCorpus);
    ImmutableList<DremioToken> tokens = Cursor.tokenizeWithCursor(
      stringAndPos.sql,
      stringAndPos.cursor);
    Optional<ParameterResolver.Result> result = PARAMETER_RESOLVER.resolve(
      tokens,
      COLUMN_AND_PATHS,
      FROM_CLAUSE.getTableReferences());
    return result.map(Output::create).orElse(null);
  }

  private static ImmutableSet<Column> createColumns(ImmutableList<MockSchemas.ColumnSchema> tableSchema) {
    ImmutableSet.Builder<Column> builder = new ImmutableSet.Builder<>();
    for (MockSchemas.ColumnSchema columnSchema : tableSchema) {
      builder.add(Column.typedColumn(columnSchema.getName(), columnSchema.getSqlTypeName()));
    }
    return builder.build();
  }

  private static ParameterResolver createParameterResolver(
    SqlOperatorTable operatorTable,
    SimpleCatalog<?> catalog) {
    DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      operatorTable,
      catalogReader);
    SqlValidatorAndScopeFactory.Result validatorAndScope = SqlValidatorAndScopeFactory.create(
      operatorTable,
      catalog);
    SqlNodeParser parser = ValidatingParser.create(
      chainedOperatorTable,
      catalog);
    FunctionDictionary functionDictionary = FunctionDictionary.create(chainedOperatorTable
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof  SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .map(sqlFunction -> FunctionFactory.create(sqlFunction))
      .collect(toList()));

    return new ParameterResolver(
      functionDictionary,
      new ParameterTypeExtractor(validatorAndScope.getSqlValidator(),
      validatorAndScope.getScope(),
      parser));
  }

  /**
   * Output type for the baseline file.
   */
  public static final class Output {
    private final List<String> columns;
    private final List<String> functions;
    private final FunctionContext functionContext;

    public Output(
      List<String> columns,
      List<String> functions,
      FunctionContext functionContext) {
      this.columns = columns;
      this.functions = functions;
      this.functionContext = functionContext;
    }

    public static Output create(ParameterResolver.Result result) {
      return new Output(
        result.getResolutions().getColumns().stream().map(columnAndTableAlias -> columnAndTableAlias.getColumn().getName()).sorted().collect(Collectors.toList()),
        result.getResolutions().getFunctions().stream().map(Function::getName).sorted().collect(Collectors.toList()),
        result.getFunctionContext());
    }

    public List<String> getColumns() {
      return columns;
    }

    public List<String> getFunctions() {
      return functions;
    }

    public FunctionContext getFunctionContext() {
      return functionContext;
    }
  }
}
