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
package com.dremio.exec.planner.logical;

import static com.dremio.test.dsl.RexDsl.dateInput;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.gt;
import static com.dremio.test.dsl.RexDsl.gte;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.literalDate;
import static com.dremio.test.dsl.RexDsl.literalTimestamp;
import static com.dremio.test.dsl.RexDsl.lt;
import static com.dremio.test.dsl.RexDsl.lte;
import static com.dremio.test.dsl.RexDsl.notEq;
import static com.dremio.test.dsl.RexDsl.timestampInput;
import static com.dremio.test.scaffolding.ScaffoldingRel.DATE_TYPE;

import com.dremio.common.collections.ImmutableEntry;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.rewrite.sargable.DateTimeFunctionRexShuttle;
import com.dremio.exec.planner.logical.rewrite.sargable.SARGableRexUtils;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.Checker;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.GoldenFileTestBuilder;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test for {@link DateTimeFunctionRexShuttle}. */
public final class DateTimeFunctionRexShuttleTest {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  @ParameterizedTest
  @ValueSource(strings = {"YEAR", "MONTH", "DAY"})
  public void testSARGableDateTruncTransformDate(String timeUnit) {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString, timeUnit)
        .add("DATE_TRUNC", eq(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .add("DATE_TRUNC", notEq(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .add("DATE_TRUNC", gt(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .add("DATE_TRUNC", gte(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .add("DATE_TRUNC", lt(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .add("DATE_TRUNC", lte(dateTrunc(timeUnit, dateInput(0)), literalDate("2020-08-01")))
        .runTests();
  }

  @ParameterizedTest
  @ValueSource(strings = {"HOUR", "MINUTE", "SECOND"})
  public void testSARGableDateTruncTransformTime(String timeUnit) {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString, timeUnit)
        .add(
            "DATE_TRUNC",
            eq(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .add(
            "DATE_TRUNC",
            notEq(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .add(
            "DATE_TRUNC",
            gt(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .add(
            "DATE_TRUNC",
            gte(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .add(
            "DATE_TRUNC",
            lt(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .add(
            "DATE_TRUNC",
            lte(dateTrunc(timeUnit, timestampInput(1)), literalTimestamp("2020-08-01 00:00:00")))
        .runTests();
  }

  @Test
  public void testSARGableDateAddTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("DATE_ADD", eq(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_ADD", notEq(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_ADD", gt(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_ADD", gte(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_ADD", lt(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_ADD", lte(dateAdd(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            eq(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            notEq(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            gt(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            gte(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            lt(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_ADD",
            lte(
                dateAdd(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .runTests();
  }

  @Test
  public void testSARGableDateSubTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("DATE_SUB", eq(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_SUB", notEq(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_SUB", gt(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_SUB", gte(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_SUB", lt(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add("DATE_SUB", lte(dateSub(dateInput(0), literal(1)), literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            eq(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            notEq(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            gt(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            gte(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            lt(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .add(
            "DATE_SUB",
            lte(
                dateSub(dateInput(0), literalInterval(1L, TimeUnit.MONTH)),
                literalDate("2020-08-01")))
        .runTests();
  }

  @Test
  public void testSARGableDateUDiffTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("DATE_DIFF", eq(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATE_DIFF", notEq(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATE_DIFF", gt(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATE_DIFF", gte(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATE_DIFF", lt(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATE_DIFF", lte(dateUDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", eq(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", notEq(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", gt(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", gte(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", lt(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .add("DATEDIFF", lte(dateDiff(dateInput(0), literalDate("2020-08-01")), literal(1)))
        .runTests();
  }

  @ParameterizedTest
  @ValueSource(strings = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"})
  public void testSARGableExtractTransform(String timeUnit) {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString, timeUnit)
        .add("EXTRACT", eq(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("EXTRACT", notEq(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("EXTRACT", gt(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("EXTRACT", gte(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("EXTRACT", lt(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("EXTRACT", lte(extract(literal(timeUnit), dateInput(0)), literal(2020)))
        .runTests();
  }

  @ParameterizedTest
  @ValueSource(strings = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"})
  public void testSARGableDatePartTransform(String timeUnit) {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString, timeUnit)
        .add("DATE_PART", eq(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("DATE_PART", notEq(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("DATE_PART", gt(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("DATE_PART", gte(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("DATE_PART", lt(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .add("DATE_PART", lte(datePart(literal(timeUnit), dateInput(0)), literal(2020)))
        .runTests();
  }

  @Test
  public void testSARGableNextDayTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("NEXT_DAY", eq(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .add("NEXT_DAY", notEq(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .add("NEXT_DAY", gt(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .add("NEXT_DAY", gte(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .add("NEXT_DAY", lt(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .add("NEXT_DAY", lte(nextDay(dateInput(0), literal("SUN")), literalDate("2020-08-05")))
        .runTests();
  }

  @Test
  public void testSARGableConvertTimezoneTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add(
            "CONVERT_TIMEZONE",
            eq(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            notEq(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            gt(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            gte(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            lt(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            lte(
                convertTimezone(
                    literal("America/Los_Angeles"), literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            eq(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            notEq(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            gt(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            gte(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            lt(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .add(
            "CONVERT_TIMEZONE",
            lte(
                convertTimezone(literal("America/New_York"), dateInput(0)),
                literalDate("2020-08-05")))
        .runTests();
  }

  @Test
  public void testSARGableMonthsBetweenTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        // MONTHS_BETWEEN(date_timestamp_expression1 string, date_timestamp_expression2 string) →
        // float
        .add(
            "MONTHS_BETWEEN", eq(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", eq(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN", eq(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", eq(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            notEq(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            notEq(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            notEq(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            notEq(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN", gt(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", gt(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN", gt(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", gt(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            gte(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            gte(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            gte(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            gte(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN", lt(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", lt(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN", lt(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN", lt(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            lte(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            lte(monthsBetween(literal("2020-08-05"), dateInput(0)), literal(1.5F)))
        .add(
            "MONTHS_BETWEEN",
            lte(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.0F)))
        .add(
            "MONTHS_BETWEEN",
            lte(monthsBetween(dateInput(0), literal("2020-08-05")), literal(1.5F)))
        .runTests();
  }

  @Test
  public void testSARGableCastTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("CAST", eq(cast(DATE_TYPE, dateInput(0)), literalDate("2020-08-05")))
        .add("CAST", eq(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .add("CAST", notEq(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .add("CAST", gt(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .add("CAST", gte(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .add("CAST", lt(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .add("CAST", lte(cast(DATE_TYPE, timestampInput(1)), literalDate("2020-08-05")))
        .runTests();
  }

  @Test
  public void testSARGableToDateTransform() {
    new GoldenFileTestBuilder<>(this::sargableTransform, Objects::toString)
        .add("TO_DATE", eq(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .add("TO_DATE", notEq(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .add("TO_DATE", gt(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .add("TO_DATE", gte(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .add("TO_DATE", lt(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .add("TO_DATE", lte(toDate(timestampInput(1)), literalDate("2020-08-05")))
        .runTests();
  }

  private static RexNode dateTrunc(String strInterval, RexNode dateExpr) {
    return rexBuilder.makeCall(
        SARGableRexUtils.DATE_TRUNC, rexBuilder.makeLiteral(strInterval), dateExpr);
  }

  private static RexNode dateAdd(RexNode dateExpr, RexNode literal) {
    return rexBuilder.makeCall(SARGableRexUtils.DATE_ADD, dateExpr, literal);
  }

  private static RexNode dateSub(RexNode dateExpr, RexNode literal) {
    return rexBuilder.makeCall(SARGableRexUtils.DATE_SUB, dateExpr, literal);
  }

  private static RexNode dateUDiff(RexNode dateExpr1, RexNode dateExpr2) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "DATE_DIFF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER,
            null,
            Checker.of(2),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        dateExpr1,
        dateExpr2);
  }

  private static RexNode extract(RexNode literal, RexNode dateExpr) {
    return rexBuilder.makeCall(SARGableRexUtils.EXTRACT, literal, dateExpr);
  }

  private static RexNode dateDiff(RexNode dateExpr1, RexNode dateExpr2) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "DATEDIFF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER,
            null,
            Checker.of(2),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        dateExpr1,
        dateExpr2);
  }

  private static RexNode datePart(RexNode literal, RexNode dateExpr) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "DATE_PART",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER,
            null,
            Checker.of(2),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        literal,
        dateExpr);
  }

  private static RexNode nextDay(RexNode dateExpr, RexNode literal) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "NEXT_DAY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE,
            null,
            Checker.of(2),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        dateExpr,
        literal);
  }

  private static RexNode convertTimezone(RexNode literal1, RexNode dateExpr) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "CONVERT_TIMEZONE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE,
            null,
            Checker.of(3),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        literal1,
        dateExpr);
  }

  private static RexNode convertTimezone(RexNode literal1, RexNode literal2, RexNode dateExpr) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "CONVERT_TIMEZONE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE,
            null,
            Checker.of(3),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        literal1,
        literal2,
        dateExpr);
  }

  private static RexNode monthsBetween(RexNode dateExpr1, RexNode dateExpr2) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "MONTHS_BETWEEN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE,
            null,
            Checker.of(2),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        dateExpr1,
        dateExpr2);
  }

  private static RexNode cast(RelDataType dateType, RexNode dateExpr) {
    return rexBuilder.makeCast(dateType, dateExpr);
  }

  private static RexNode toDate(RexNode dateExpr) {
    return rexBuilder.makeCall(
        new SqlFunction(
            "TO_DATE",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE,
            null,
            Checker.of(1),
            SqlFunctionCategory.USER_DEFINED_FUNCTION),
        dateExpr);
  }

  public static RexNode literalInterval(Long num, TimeUnit timeUnit) {
    return rexBuilder.makeIntervalLiteral(
        BigDecimal.valueOf(num * timeUnit.multiplier.longValue()),
        new SqlIntervalQualifier(timeUnit, null, SqlParserPos.ZERO));
  }

  private String sargableTransform(RexNode filterExpr) {
    LogicalFilter filter =
        (LogicalFilter)
            relBuilder
                .values(createSimpleRowType("a", SqlTypeName.DATE, "b", SqlTypeName.TIMESTAMP))
                .filter(filterExpr)
                .build();
    RelOptCluster relOptCluster = filter.getCluster();
    DateTimeFunctionRexShuttle replacer = new DateTimeFunctionRexShuttle(relOptCluster);
    LogicalFilter rewrittenNode = (LogicalFilter) filter.accept(replacer);
    return rewrittenNode.getCondition().toString();
  }

  private static RelDataType createSimpleRowType(
      String name, SqlTypeName typeName, String name2, SqlTypeName typeName2) {
    return typeFactory.createStructType(
        ImmutableList.<Map.Entry<String, RelDataType>>of(
            new ImmutableEntry<>(name, typeFactory.createSqlType(typeName)),
            new ImmutableEntry<>(name2, typeFactory.createSqlType(typeName2))));
  }

  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner =
        new HepPlanner(
            new HepProgramBuilder().build(), context, false, null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return RelBuilder.proto(context).create(cluster, null);
  }
}
