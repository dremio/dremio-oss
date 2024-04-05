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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.planner.sql.parser.SqlUnresolvedVersionedTableMacro;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacroCall;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.TimestampString;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestVersionedTableExpressionResolver {

  private static SqlToRelConverter sqlToRelConverter;
  private static VersionedTableExpressionResolver resolver;
  private static final SqlOperator DUMMY_OP = mock(SqlOperator.class);
  private static final SqlNode DUMMY_NODE = mock(SqlNode.class);

  @BeforeClass
  public static void setup() {
    final FunctionImplementationRegistry functionImplementationRegistry =
        mock(FunctionImplementationRegistry.class);
    final Prepare.CatalogReader catalogReader = mock(Prepare.CatalogReader.class);
    when(catalogReader.nameMatcher()).thenReturn(SqlNameMatchers.withCaseSensitive(true));
    final RexBuilder rexBuilder = new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);
    final RelOptTable.ViewExpander viewExpander = mock(RelOptTable.ViewExpander.class);
    final RelOptPlanner planner = mock(RelOptPlanner.class);
    final RelOptCluster cluster = mock(RelOptCluster.class);
    when(cluster.getPlanner()).thenReturn(planner);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    final ContextInformation contextInformation = mock(ContextInformation.class);
    final ConvertletTable convertletTable = new ConvertletTable(false);
    final OptionResolver optionResolver =
        OptionResolverSpecBuilder.build(
            new OptionResolverSpec().addOption(ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL, true));
    final SqlValidator.Config conf =
        SqlValidator.Config.DEFAULT
            .withSqlConformance(DremioSqlConformance.INSTANCE)
            .withAmbiguousColumnAllowed(
                optionResolver.getOption(PlannerSettings.ALLOW_AMBIGUOUS_COLUMN));

    SqlValidatorImpl validator =
        new SqlValidatorImpl(
            new SqlValidatorImpl.FlattenOpCounter(),
            DremioCompositeSqlOperatorTable.create(functionImplementationRegistry),
            catalogReader,
            JavaTypeFactoryImpl.INSTANCE,
            conf,
            optionResolver);

    sqlToRelConverter =
        new SqlToRelConverter(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            convertletTable,
            SqlToRelConverter.Config.DEFAULT);
    resolver = new VersionedTableExpressionResolver(validator, rexBuilder, contextInformation);
  }

  @Test
  public void testResolve() {
    final SqlCall expr =
        createTimestampAddCall(
            TimeUnit.DAY, "10", createTimestampLiteral(2022, Calendar.JANUARY, 1, 1, 1, 1));
    TableVersionContext expected = getExpected(2022, Calendar.JANUARY, 11, 1, 1, 1);
    SqlVersionedTableMacroCall call = createVersionedTableMacroCall(expr);

    resolver.resolve(sqlToRelConverter, call);

    Assert.assertEquals(
        expected,
        call.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
  }

  @Test
  public void testMultipleResolvesInNodeList() {
    final SqlCall expr1 =
        createTimestampAddCall(
            TimeUnit.DAY, "10", createTimestampLiteral(2022, Calendar.JANUARY, 1, 1, 1, 1));
    TableVersionContext expected1 = getExpected(2022, Calendar.JANUARY, 11, 1, 1, 1);
    SqlVersionedTableMacroCall call1 = createVersionedTableMacroCall(expr1);

    final SqlCall expr2 =
        createTimestampAddCall(
            TimeUnit.MONTH, "2", createTimestampLiteral(2022, Calendar.JANUARY, 1, 0, 0, 0));
    TableVersionContext expected2 = getExpected(2022, Calendar.MARCH, 1, 0, 0, 0);
    SqlVersionedTableMacroCall call2 = createVersionedTableMacroCall(expr2);

    SqlNodeList nodeList =
        createNodeList(
            createCall(DUMMY_OP, DUMMY_NODE),
            call1,
            DUMMY_NODE,
            createNodeList(call2, DUMMY_NODE),
            DUMMY_NODE);

    resolver.resolve(sqlToRelConverter, nodeList);

    Assert.assertEquals(
        expected1,
        call1.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
    Assert.assertEquals(
        expected2,
        call2.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
  }

  @Test
  public void testMultipleResolvesInCalls() {
    final SqlCall expr1 =
        createTimestampAddCall(
            TimeUnit.DAY, "10", createTimestampLiteral(2022, Calendar.JANUARY, 1, 1, 1, 1));
    TableVersionContext expected1 = getExpected(2022, Calendar.JANUARY, 11, 1, 1, 1);
    SqlVersionedTableMacroCall call1 = createVersionedTableMacroCall(expr1);

    final SqlCall expr2 =
        createTimestampAddCall(
            TimeUnit.MONTH, "2", createTimestampLiteral(2022, Calendar.JANUARY, 1, 0, 0, 0));
    TableVersionContext expected2 = getExpected(2022, Calendar.MARCH, 1, 0, 0, 0);
    SqlVersionedTableMacroCall call2 = createVersionedTableMacroCall(expr2);

    SqlCall rootCall =
        createCall(
            DUMMY_OP,
            createCall(DUMMY_OP, DUMMY_NODE),
            createCall(DUMMY_OP, DUMMY_NODE, call1),
            DUMMY_NODE,
            call2,
            DUMMY_NODE);

    resolver.resolve(sqlToRelConverter, rootCall);

    Assert.assertEquals(
        expected1,
        call1.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
    Assert.assertEquals(
        expected2,
        call2.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
  }

  @Test
  public void testResolveIsIdempotent() {
    final SqlCall expr =
        createTimestampAddCall(
            TimeUnit.DAY, "10", createTimestampLiteral(2022, Calendar.JANUARY, 1, 1, 1, 1));
    TableVersionContext expected = getExpected(2022, Calendar.JANUARY, 11, 1, 1, 1);
    SqlVersionedTableMacroCall call = createVersionedTableMacroCall(expr);

    resolver.resolve(sqlToRelConverter, call);
    resolver.resolve(sqlToRelConverter, call);

    Assert.assertEquals(
        expected,
        call.getSqlTableVersionSpec().getTableVersionSpec().getResolvedTableVersionContext());
  }

  private SqlCall createCall(SqlOperator operator, SqlNode... operands) {
    return new SqlBasicCall(operator, operands, SqlParserPos.ZERO);
  }

  private SqlNodeList createNodeList(SqlNode... operands) {
    return new SqlNodeList(Arrays.asList(operands), SqlParserPos.ZERO);
  }

  private SqlVersionedTableMacroCall createVersionedTableMacroCall(SqlNode expr) {
    SqlUnresolvedVersionedTableMacro func =
        new SqlUnresolvedVersionedTableMacro(
            new SqlIdentifier("name", SqlParserPos.ZERO),
            new SqlTableVersionSpec(SqlParserPos.ZERO, TableVersionType.TIMESTAMP, expr, null));
    return new SqlVersionedTableMacroCall(func, new SqlNode[] {}, SqlParserPos.ZERO);
  }

  private SqlTimestampLiteral createTimestampLiteral(
      int year, int month, int day, int hour, int minute, int second) {
    return SqlLiteral.createTimestamp(
        new TimestampString(year, month + 1, day, hour, minute, second), 3, SqlParserPos.ZERO);
  }

  private SqlCall createTimestampAddCall(
      TimeUnit timeUnit, String value, SqlTimestampLiteral timestamp) {
    return new SqlBasicCall(
        SqlStdOperatorTable.TIMESTAMP_ADD,
        new SqlNode[] {
          SqlLiteral.createSymbol(timeUnit, SqlParserPos.ZERO),
          SqlLiteral.createExactNumeric(value, SqlParserPos.ZERO),
          timestamp
        },
        SqlParserPos.ZERO);
  }

  private long getTimestampMillis(int year, int month, int day, int hour, int minute, int second) {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(year, month, day, hour, minute, second);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();
  }

  private TableVersionContext getExpected(
      int year, int month, int day, int hour, int minute, int second) {
    return new TableVersionContext(
        TableVersionType.TIMESTAMP, getTimestampMillis(year, month, day, hour, minute, second));
  }
}
