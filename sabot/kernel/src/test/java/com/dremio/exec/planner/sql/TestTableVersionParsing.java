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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Util;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionOperator;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacro;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.tablefunctions.TableMacroNames;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class TestTableVersionParsing {

  private Map<List<String>, VersionedTableMacro> tableMacroMocks;

  private static final List<String> TIME_TRAVEL_MACRO_NAME = TableMacroNames.TIME_TRAVEL;
  private static final List<String> TABLE_HISTORY_MACRO_NAME = ImmutableList.of("table_history");

  @Test
  public void testTableWithSnapshotVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.SNAPSHOT_ID, TableVersionOperator.BEFORE, "1"));

    parseAndValidate("SELECT * FROM my.table1 BEFORE SNAPSHOT '1'", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithBranchVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.BRANCH, TableVersionOperator.AT, "branch1"));

    parseAndValidate("SELECT * FROM my.table1 AT BRANCH branch1", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithTagVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TAG, TableVersionOperator.AT, "tag1"));

     parseAndValidate("SELECT * FROM my.table1 AT TAG tag1", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithReferenceVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.REFERENCE, TableVersionOperator.AT, "ref1"));

    parseAndValidate("SELECT * FROM my.table1 AT REF ref1", ImmutableList.of(expected));
    parseAndValidate("SELECT * FROM my.table1 AT REFERENCE ref1", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithCommitVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.COMMIT_HASH_ONLY, TableVersionOperator.AT, "hash1"));

    parseAndValidate("SELECT * FROM my.table1 AT COMMIT hash1", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithLiteralTimestampVersion() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(2022, Calendar.JANUARY, 1, 1, 1, 1);
    cal.set(Calendar.MILLISECOND, 111);
    long timestampInMillis = cal.getTimeInMillis();

    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TIMESTAMP, TableVersionOperator.BEFORE, timestampInMillis));

    parseAndValidate("SELECT * FROM my.table1 BEFORE TIMESTAMP '2022-01-01 01:01:01.111'",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableWithTimestampExpressionVersion() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(2022, Calendar.JANUARY, 11, 1, 1, 1);
    cal.set(Calendar.MILLISECOND, 111);
    long timestampInMillis = cal.getTimeInMillis();

    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TIMESTAMP, TableVersionOperator.BEFORE, timestampInMillis));

    parseAndValidate("SELECT * FROM my.table1 BEFORE " +
      "TIMESTAMPADD(day, 10, TIMESTAMP '2022-01-01 01:01:01.111')", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithInvalidTimestampExpressionVersionFails() {
    Assert.assertThrows(CalciteContextException.class, () -> parseAndValidate(
      "SELECT * FROM dfs_hadoop.tmp.iceberg BEFORE 1.254 * 87.9", ImmutableList.of()));
  }

  @Test
  public void testMalformedVersionContextFails() {
    Assert.assertThrows(SqlParseException.class, () -> parseAndValidate(
      "SELECT * FROM dfs_hadoop.tmp.iceberg AT AS foo", ImmutableList.of()));
  }

  @Test
  public void testTableAliasingWithVersionContext() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.SNAPSHOT_ID, TableVersionOperator.BEFORE, "1"));

    parseAndValidate("SELECT table1.id FROM my.table1 BEFORE SNAPSHOT '1'", ImmutableList.of(expected));
  }

  @Test
  public void testWithClauseWithVersionContext() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.SNAPSHOT_ID, TableVersionOperator.BEFORE, "1"));

    parseAndValidate("WITH cte1 AS (SELECT table1.id FROM my.table1 BEFORE SNAPSHOT '1') " +
      "SELECT cte1.id FROM cte1", ImmutableList.of(expected));
  }

  @Test
  public void testJoinWithDifferentVersionContexts() throws Exception {
    TableMacroInvocation leftExpected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.SNAPSHOT_ID, TableVersionOperator.BEFORE, "1"));
    TableMacroInvocation rightExpected = new TableMacroInvocation(
      TIME_TRAVEL_MACRO_NAME,
      ImmutableList.of("my.table2"),
      new TableVersionContext(TableVersionType.BRANCH, TableVersionOperator.AT, "branch1"));

    parseAndValidate("SELECT l.*, r.id FROM " +
      "my.table1 BEFORE SNAPSHOT '1' AS l INNER JOIN " +
      "my.table2 AT BRANCH branch1 AS r ON l.id = r.id",
      ImmutableList.of(leftExpected, rightExpected));
  }

  @Test
  public void testTableMacroWithSnapshotVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.SNAPSHOT_ID, TableVersionOperator.BEFORE, "1"));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) BEFORE SNAPSHOT '1'",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithBranchVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.BRANCH, TableVersionOperator.AT, "branch1"));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) AT BRANCH branch1",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithTagVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TAG, TableVersionOperator.AT, "tag1"));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) AT TAG tag1",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithReferenceVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.REFERENCE, TableVersionOperator.AT, "ref1"));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) AT REF ref1", ImmutableList.of(expected));
    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) AT REFERENCE ref1", ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithCommitVersion() throws Exception {
    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.COMMIT_HASH_ONLY, TableVersionOperator.AT, "hash1"));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) AT COMMIT hash1",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithLiteralTimestampVersion() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(2022, Calendar.JANUARY, 1, 1, 1, 1);
    cal.set(Calendar.MILLISECOND, 111);
    long timestampInMillis = cal.getTimeInMillis();

    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TIMESTAMP, TableVersionOperator.BEFORE, timestampInMillis));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) BEFORE TIMESTAMP '2022-01-01 01:01:01.111'",
      ImmutableList.of(expected));
  }

  @Test
  public void testTableMacroWithTimestampExpressionVersion() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(2022, Calendar.JANUARY, 11, 1, 1, 1);
    cal.set(Calendar.MILLISECOND, 111);
    long timestampInMillis = cal.getTimeInMillis();

    TableMacroInvocation expected = new TableMacroInvocation(
      TABLE_HISTORY_MACRO_NAME,
      ImmutableList.of("my.table1"),
      new TableVersionContext(TableVersionType.TIMESTAMP, TableVersionOperator.BEFORE, timestampInMillis));

    parseAndValidate("SELECT * FROM TABLE(table_history('my.table1')) BEFORE " +
      "TIMESTAMPADD(day, 10, TIMESTAMP '2022-01-01 01:01:01.111')", ImmutableList.of(expected));
  }

  @Test
  public void testTableWithSnapshotVersionFailsWithFeatureDisabled() throws Exception {
    Assert.assertThrows(UserException.class, () -> parseAndValidate(
      "SELECT * FROM my.table1 BEFORE SNAPSHOT '1'", ImmutableList.of(), false));
  }

  public void parseAndValidate(String sql, List<TableMacroInvocation> expectedTableMacroInvocations) throws Exception {
    parseAndValidate(sql, expectedTableMacroInvocations, true);
  }

  public void parseAndValidate(String sql, List<TableMacroInvocation> expectedTableMacroInvocations,
                               boolean enableTimeTravel) throws Exception {
    // Mocks required for validator and expression resolver
    final Prepare.CatalogReader catalogReader = mock(Prepare.CatalogReader.class);
    when(catalogReader.nameMatcher()).thenReturn(SqlNameMatchers.withCaseSensitive(true));
    final RexBuilder rexBuilder = new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);
    final RelOptTable.ViewExpander viewExpander = mock(RelOptTable.ViewExpander.class);
    final RelOptPlanner planner = mock(RelOptPlanner.class);
    final RelOptCluster cluster = mock(RelOptCluster.class);
    when(cluster.getPlanner()).thenReturn(planner);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    final ContextInformation contextInformation = mock(ContextInformation.class);
    final ConvertletTable convertletTable = new ConvertletTable(contextInformation, false);
    final OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec()
      .addOption(ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL, enableTimeTravel));

    // Setup a mock per table macro so that we can associate expecteds to a specific macro invocation
    initializeTableMacroMocks();

    // Mocks for table macro lookup
    final SqlOperatorTable operatorTable = mock(SqlOperatorTable.class);
    doAnswer(invocation -> {
      lookupOperatorOverloads(invocation.getArgument(0), invocation.getArgument(1),
        invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4));
      return null;
    }).when(operatorTable).lookupOperatorOverloads(any(), any(), any(), any(), any());

    SqlValidatorImpl validator = new SqlValidatorImpl(
      new SqlValidatorImpl.FlattenOpCounter(),
      SqlOperatorTables.chain(new DremioCompositeSqlOperatorTable(), operatorTable),
      catalogReader,
      JavaTypeFactoryImpl.INSTANCE,
      DremioSqlConformance.INSTANCE,
      optionResolver);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(viewExpander, validator, catalogReader,
      cluster, convertletTable, SqlToRelConverter.Config.DEFAULT);
    VersionedTableExpressionResolver resolver = new VersionedTableExpressionResolver(validator, rexBuilder);

    // Parse
    ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 128, true);
    SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode rootNode = parser.parseStmt();

    // Resolve version context expressions to literals
    resolver.resolve(sqlToRelConverter, rootNode);

    // Call validator
    SqlNode validatedRootNode = validator.validate(rootNode);

    Assert.assertNotNull(validatedRootNode);

    for (TableMacroInvocation expected : expectedTableMacroInvocations) {
      VersionedTableMacro macro = tableMacroMocks.get(expected.name);
      verify(macro).apply(expected.arguments, expected.tableVersionContext);
    }
  }

  private void initializeTableMacroMocks() {
    final List<FunctionParameter> functionParameters = new ReflectiveFunctionBase.ParameterListBuilder()
      .add(String.class, "table_name").build();
    final TranslatableTable table = mock(TranslatableTable.class);
    when(table.getRowType(any())).thenAnswer(invocation -> {
      RelDataTypeFactory typeFactory = invocation.getArgument(0);
      return typeFactory.createStructType(
        ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)), ImmutableList.of("id"));
    });

    tableMacroMocks = ImmutableMap.of(
      TIME_TRAVEL_MACRO_NAME, mock(VersionedTableMacro.class),
      TABLE_HISTORY_MACRO_NAME, mock(VersionedTableMacro.class)
    );

    tableMacroMocks.values().forEach(m -> {
      when(m.getParameters()).thenReturn(functionParameters);
      when(m.apply(any(), any())).thenReturn(table);
    });
  }

  private void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
                                       List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    if (category != SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION) {
      return;
    }

    VersionedTableMacro tableMacro = tableMacroMocks.get(opName.names);
    if (tableMacro != null) {
      List<RelDataType> argTypes = new ArrayList<>();
      List<SqlTypeFamily> typeFamilies = new ArrayList<>();
      for (FunctionParameter o : tableMacro.getParameters()) {
        final RelDataType type = o.getType(JavaTypeFactoryImpl.INSTANCE);
        argTypes.add(type);
        typeFamilies.add(
          Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
      }
      final IntPredicate isParameterAtIndexOptional = index ->
        tableMacro.getParameters().get(index).isOptional();
      final FamilyOperandTypeChecker typeChecker =
        OperandTypes.family(typeFamilies, isParameterAtIndexOptional::test);
      final List<RelDataType> paramTypes = toSql(argTypes);
      SqlVersionedTableMacro operator = new SqlVersionedTableMacro(opName, ReturnTypes.CURSOR,
        InferTypes.explicit(argTypes), typeChecker, paramTypes, tableMacro);

      operatorList.add(operator);
    }
  }

  private List<RelDataType> toSql(List<RelDataType> types) {
    return types.stream().map(this::toSql).collect(Collectors.toList());
  }

  private RelDataType toSql(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
      && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
      == Object.class) {
      return JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(
        JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.ANY), true);
    }
    return JavaTypeFactoryImpl.INSTANCE.toSql(type);
  }

  @SuppressWarnings("NewClassNamingConvention")
  private static class TableMacroInvocation {
    private final List<String> name;
    private final List<Object> arguments;
    private final TableVersionContext tableVersionContext;

    public TableMacroInvocation(List<String> name, List<Object> arguments,
                                TableVersionContext tableVersionContext) {
      this.name = name;
      this.arguments = arguments;
      this.tableVersionContext = tableVersionContext;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || this.getClass() != obj.getClass()) {
        return false;
      }

      if (this == obj) {
        return true;
      }

      TableMacroInvocation other = (TableMacroInvocation) obj;
      return Objects.equals(name, other.name) &&
        Objects.equals(arguments, other.arguments) &&
        Objects.equals(tableVersionContext, other.tableVersionContext);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, arguments, tableVersionContext);
    }
  }
}
