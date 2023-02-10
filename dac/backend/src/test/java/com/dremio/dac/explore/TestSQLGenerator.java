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
package com.dremio.dac.explore;

import static com.dremio.dac.proto.model.dataset.DataType.DATE;
import static com.dremio.dac.proto.model.dataset.DataType.DATETIME;
import static com.dremio.dac.proto.model.dataset.DataType.TIME;
import static com.dremio.dac.proto.model.dataset.MeasureType.Sum;
import static com.dremio.dac.proto.model.dataset.NumberToDateFormat.EPOCH;
import static com.dremio.dac.proto.model.dataset.NumberToDateFormat.EXCEL;
import static com.dremio.dac.proto.model.dataset.NumberToDateFormat.JULIAN;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpConvertCase;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldReplaceRange;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FilterDefinition;
import com.dremio.dac.proto.model.dataset.FilterPattern;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.FilterValue;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.proto.model.dataset.Measure;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.OrderDirection;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceSelectionType;
import com.dremio.dac.proto.model.dataset.ReplaceType;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;

/**
 * SQLGenerator tests
 */
public class TestSQLGenerator {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private CatalogService catalogService;

  private From nameDSRef = new FromTable("myspace.parentDS").wrap();
  private From sqlDSRef = new FromSQL("select * from myspace.parentDS").wrap();

  @Test
  public void testGenSQL() {

    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression value = new ExpColumnReference("parentFoo").wrap();

    validate(
            "SELECT parentFoo AS foo\nFROM myspace.parentDS",
            state.setColumnsList(asList(new Column("foo", value))));

    validate(
            "SELECT parentFoo AS foo, parentFoo AS bar\nFROM myspace.parentDS",
            state.setColumnsList(asList(new Column("foo", value), new Column("bar", value))));
  }

  @Test
  public void testStar() {

    validate(
        "SELECT *\nFROM myspace.parentDS",
        new VirtualDatasetState()
            .setFrom(nameDSRef));

    validate(
        sqlDSRef.getSql().getSql(),
        new VirtualDatasetState()
            .setFrom(sqlDSRef));
  }

  @Test
  public void selectConstant() {
    final String query = "SELECT 1729 AS special";
    final From sqlDSRef = new FromSQL(query).wrap();

    validate(query,
        new VirtualDatasetState()
            .setFrom(sqlDSRef));

    validate(sqlDSRef.getSql().getSql(),
        new VirtualDatasetState()
            .setFrom(sqlDSRef));
  }

  @Test
  public void selectConstantNested() {
    final String query = "SELECT * FROM (SELECT 87539319 AS special ORDER BY 1 LIMIT 1)";
    final From sqlDSRef = new FromSQL(query).wrap();

    validate(query,
        new VirtualDatasetState()
            .setFrom(sqlDSRef));

    validate(sqlDSRef.getSql().getSql(),
        new VirtualDatasetState()
            .setFrom(sqlDSRef));
  }

  @Test
  public void testTitleCase() {

    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression column = new ExpColumnReference("parentFoo").wrap();
    Expression title = new ExpConvertCase(ConvertCase.TITLE_CASE, column).wrap();

    validate(
            "SELECT TITLE(parentFoo) AS foo\nFROM myspace.parentDS",
            state.setColumnsList(asList(new Column("foo", title)))
    );
  }

  private void validate(String expectedSQL, VirtualDatasetState state) {
    String sql = SQLGenerator.generateSQL(state);
    assertEquals(expectedSQL, sql);
  }

  private void validateForTranformRelatedCalls(String expectedSQL, VirtualDatasetState state) {
    String sql = SQLGenerator.generateSQL(state, true, catalogService);
    assertEquals(expectedSQL, sql);
  }

  @Test
  public void testUser() {
    // user is a special keyword
    // https://issues.apache.org/jira/browse/DRILL-3435

    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression column = new ExpColumnReference("user").wrap();
    Expression title = new ExpConvertCase(ConvertCase.TITLE_CASE, column).wrap();

    validate(
            "SELECT TITLE(parentDS.\"user\") AS foo\nFROM myspace.parentDS",
            state.setColumnsList(asList(new Column("foo", title)))
    );
  }

  @Test
  public void testMultipleFilters() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression column = new ExpColumnReference("count").wrap();
    validateForTranformRelatedCalls(
      "SELECT count\n" +
        "FROM myspace.parentDS\n" +
        " WHERE (5 <= count AND 20 > count) AND (10 >= count OR 15 < count)",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(
          new Filter(column,
            new FilterDefinition(FilterType.Range)
              .setRange(new FilterRange().setLowerBound("5").setUpperBound("20").setDataType(DataType.INTEGER).setLowerBoundInclusive(true))
          ).setKeepNull(false),
          new Filter(column,
            new FilterDefinition(FilterType.Range)
              .setRange(new FilterRange().setLowerBound("10").setUpperBound("15").setDataType(DataType.INTEGER).setLowerBoundInclusive(true))
          ).setKeepNull(false).setExclude(true)
        ))
    );
  }

  @Test
  public void testFilterRange() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression column = new ExpColumnReference("count").wrap();
    validateForTranformRelatedCalls(
      "SELECT count\nFROM myspace.parentDS\n " +
        "WHERE 5 <= count",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Range)
        .setRange(new FilterRange().setLowerBound("5").setDataType(DataType.INTEGER).setLowerBoundInclusive(true)))
          .setKeepNull(false)))
    );

    validateForTranformRelatedCalls(
      "SELECT count\nFROM myspace.parentDS\n " +
        "WHERE 5 < count",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Range)
          .setRange(new FilterRange().setLowerBound("5").setDataType(DataType.INTEGER)))
          .setKeepNull(false)))
    );

    validateForTranformRelatedCalls(
      "SELECT count\nFROM myspace.parentDS\n " +
        "WHERE 5 < count AND 10 >= count",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Range)
          .setRange(new FilterRange().setLowerBound("5").setUpperBound("10").setUpperBoundInclusive(true)
            .setDataType(DataType.INTEGER)))
          .setKeepNull(false)))
    );

    validateForTranformRelatedCalls(
      "SELECT count\nFROM myspace.parentDS\n " +
        "WHERE 5 < count AND 10 > count",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Range)
          .setRange(new FilterRange().setLowerBound("5").setUpperBound("10")
            .setDataType(DataType.INTEGER)))
          .setKeepNull(false)))
    );

    validateForTranformRelatedCalls(
      "SELECT count\nFROM myspace.parentDS\n " +
        "WHERE 10 > count",
      state
        .setColumnsList(asList(new Column("count", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Range)
          .setRange(new FilterRange().setUpperBound("10").setUpperBoundInclusive(false)
            .setDataType(DataType.INTEGER)))
          .setKeepNull(false)))
    );
  }

  @Test
  public void testExcludeText() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression column = new ExpColumnReference("user").wrap();

    validateForTranformRelatedCalls(
        "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
            "WHERE parentDS.\"user\" NOT IN ('foo', 'bar')",
        state
            .setColumnsList(asList(new Column("foo", column)))
            .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
                .setValue(new FilterValue(DataType.TEXT).setValuesList(asList("foo", "bar")))
            ).setExclude(true).setKeepNull(false)))
    );

    validateForTranformRelatedCalls(
            "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
                    "WHERE regexp_like(parentDS.\"user\", '.*?\\Qbar\\E.*?') OR parentDS.\"user\" IS NULL ",
            state
                    .setColumnsList(asList(new Column("foo", column)))
                    .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Pattern)
                                    .setPattern(new FilterPattern(
                                            new ReplacePatternRule(ReplaceSelectionType.CONTAINS)
                                                    .setIgnoreCase(false)
                                                    .setSelectionPattern("bar")
                                    ))).setExclude(false).setKeepNull(true))
                    )
    );
  }

  @Test
  public void testExcludeValue() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression column = new ExpColumnReference("user").wrap();

    validateForTranformRelatedCalls(
        "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
            "WHERE parentDS.\"user\" NOT IN ('foo', 'bar')",
        state
            .setColumnsList(asList(new Column("foo", column)))
            .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
                .setValue(new FilterValue(DataType.DATE).setValuesList(asList("foo", "bar")))).setExclude(true).setKeepNull(false)
            ))
    );

    validateForTranformRelatedCalls(
        "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
            "WHERE parentDS.\"user\" = 'foo'",
        state
            .setColumnsList(asList(new Column("foo", column)))
            .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
                .setValue(new FilterValue(DataType.DATE).setValuesList(asList("foo")))).setExclude(false).setKeepNull(false)
            ))
    );

    validateForTranformRelatedCalls(
        "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
            "WHERE parentDS.\"user\" <> 'bar' OR parentDS.\"user\" IS NULL ",
        state
            .setColumnsList(asList(new Column("foo", column)))
            .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
                .setValue(new FilterValue(DataType.DATE).setValuesList(asList("bar")))).setExclude(true).setKeepNull(true)
            ))
    );
  }

  @Test
  public void testJoin() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    validateForTranformRelatedCalls(
        "SELECT *\n" +
            "FROM myspace.parentDS\n" +
            " INNER JOIN \"space\".foo AS join_1 ON parentDS.bar = join_1.bar",
        state.setJoinsList(asList(new Join(JoinType.Inner, "\"space\".foo", "join_1")
                .setJoinConditionsList(asList(new JoinCondition("bar", "bar")))))
    );

    validateForTranformRelatedCalls(
        "SELECT *\n" +
            "FROM myspace.parentDS\n" +
            " LEFT JOIN \"space\".foo AS join_1 ON parentDS.foo = join_1.bar\n" +
            " FULL JOIN \"space\".bar AS join_2 ON parentDS.\"user\" = join_2.\"user\"",
        state.setJoinsList(asList(
                new Join(JoinType.LeftOuter, "\"space\".foo", "join_1")
                        .setJoinConditionsList(asList(new JoinCondition("foo", "bar"))),
                new Join(JoinType.FullOuter, "\"space\".bar", "join_2")
                        .setJoinConditionsList(asList(new JoinCondition("user", "user")))
        ))
    );
  }

  @Test
  public void testNumberToDate() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 = new FieldConvertNumberToDate()
            .setDesiredType(DATETIME)
            .setFormat(EXCEL);
    Expression exp1 = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();
    validate("SELECT TO_TIMESTAMP((bar - 25569) * 86400) AS foo\n" +
            "FROM myspace.parentDS", state.setColumnsList(asList(new Column("foo", exp1))));

    FieldTransformationBase transf2 = new FieldConvertNumberToDate()
            .setDesiredType(DATE)
            .setFormat(JULIAN);
    Expression exp2 = new ExpFieldTransformation(transf2.wrap(), exp0).wrap();
    validate("SELECT TO_DATE((bar - 2440587.5) * 86400) AS foo\n" +
            "FROM myspace.parentDS", state.setColumnsList(asList(new Column("foo", exp2))));

    FieldTransformationBase transf3 = new FieldConvertNumberToDate()
            .setDesiredType(TIME)
            .setFormat(EPOCH);
    Expression exp3 = new ExpFieldTransformation(transf3.wrap(), exp0).wrap();
    validate("SELECT TO_TIME(bar) AS foo\n" +
            "FROM myspace.parentDS", state.setColumnsList(asList(new Column("foo", exp3))));
  }

  @Test
  public void testFromJSON() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 = new FieldConvertFromJSON();
    Expression exp1 = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();
    validate("SELECT convert_from(bar, 'JSON') AS foo\n" +
            "FROM myspace.parentDS", state.setColumnsList(asList(new Column("foo", exp1))));
  }

  @Test
  public void testTableNameEnquoting() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromTable("myhome.tables.1234.test").wrap());
    validate("SELECT *\nFROM myhome.\"tables\".\"1234\".test", state);
  }

  @Test
  public void testTableNameWithAliasQuoting() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromTable("myhome.tables.1234.test").setAlias("my.tbl").wrap());
    validate("SELECT *\nFROM myhome.\"tables\".\"1234\".test AS \"my.tbl\"", state);
  }

  @Test
  public void testSubQuery() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromSQL("SELECT * FROM cp.\"region.json\"").wrap());
    validate("SELECT * FROM cp.\"region.json\"", state);
  }

  @Test
  public void testSubQueryWithSemicolon() {
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(new FromSQL("SELECT * FROM cp.\"region.json\";").wrap());
    validate("SELECT * FROM cp.\"region.json\";", state);
  }

  @Test
  public void testSubQuerySelectColumns() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromSQL("SELECT * FROM cp.\"region.json\"").setAlias("region").wrap())
        .setColumnsList(asList(new Column("foo", new ExpColumnReference("my.foo").wrap())));
    validate("SELECT \"my.foo\" AS foo\nFROM (\n  SELECT * FROM cp.\"region.json\"\n) region", state);
  }

  @Test
  public void testSubQuerySelectColumnsWithSemicolon() {
    String expectedSql = "SELECT \"my.foo\" AS foo\nFROM (\n  SELECT * FROM cp.\"region.json\"\n) region";

    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(new FromSQL("SELECT * FROM cp.\"region.json\";").setAlias("region").wrap())
      .setColumnsList(asList(new Column("foo", new ExpColumnReference("my.foo").wrap())));
    validate(expectedSql, state);

    state.setFrom(new FromSQL("SELECT * FROM cp.\"region.json\" ; ; ").setAlias("region").wrap());
    validate(expectedSql, state);
  }

  @Test
  public void testSubQueryNameWithAliasEnquoting() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromTable("myhome.tables.1234.test").setAlias("my.tbl").wrap());
    validate("SELECT *\nFROM myhome.\"tables\".\"1234\".test AS \"my.tbl\"", state);
  }

  @Test
  public void testOrderBy() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromTable("myhome.tables.1234.test").wrap())
        // user is a reserved keyword, make sure it is escaped in generated SQL
        .setOrdersList(asList(new Order("user", OrderDirection.ASC)));
    validateForTranformRelatedCalls("SELECT *\nFROM myhome.\"tables\".\"1234\".test\nORDER BY \"user\" ASC", state);
  }

  @Test
  public void testMultipleOrderBys() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(new FromTable("myhome.tables.1234.test").wrap())
        // home.dir contains a special identifier character, make sure it is escaped in generated SQL
        .setOrdersList(asList(new Order("home.dir", OrderDirection.DESC), new Order("join_date", OrderDirection.ASC)));
    validateForTranformRelatedCalls("SELECT *\nFROM myhome.\"tables\".\"1234\".test\nORDER BY \"home.dir\" DESC, join_date ASC", state);
  }

  @Test
  public void testReplaceValueDateTypeCol() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04", "1989-05-31"))
            .setReplacementType(DATE)
            .setReplacementValue("2016-11-05")
            .setReplaceType(ReplaceType.VALUE);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = DATE '1970-07-04' THEN DATE '2016-11-05'\n" +
        "  WHEN bar = DATE '1989-05-31' THEN DATE '2016-11-05'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceValueTimestampTypeCol() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04 05:04:00", "1989-05-31 08:11:43"))
            .setReplacementType(DATETIME)
            .setReplacementValue("2016-11-05 1:03:23")
            .setReplaceType(ReplaceType.VALUE);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = TIMESTAMP '1970-07-04 05:04:00' THEN TIMESTAMP '2016-11-05 1:03:23'\n" +
        "  WHEN bar = TIMESTAMP '1989-05-31 08:11:43' THEN TIMESTAMP '2016-11-05 1:03:23'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceValueTimeTypeCol() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("05:04:00", "08:11:43"))
            .setReplacementType(TIME)
            .setReplacementValue("1:03:23")
            .setReplaceType(ReplaceType.VALUE);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = TIME '05:04:00' THEN TIME '1:03:23'\n" +
        "  WHEN bar = TIME '08:11:43' THEN TIME '1:03:23'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceValueIncludingNullDateTypeCol() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04", "1989-05-31"))
            .setReplacementType(DATE)
            .setReplacementValue("2016-11-05")
            .setReplaceNull(true)
            .setReplaceType(ReplaceType.VALUE);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = DATE '1970-07-04' THEN DATE '2016-11-05'\n" +
        "  WHEN bar = DATE '1989-05-31' THEN DATE '2016-11-05'\n" +
        "  WHEN bar IS NULL THEN DATE '2016-11-05'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceValueWithNullDateTypeCol() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04", "1989-05-31"))
            .setReplacementType(DATE)
            .setReplaceNull(true)
            .setReplaceType(ReplaceType.NULL);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = DATE '1970-07-04' THEN NULL\n" +
        "  WHEN bar = DATE '1989-05-31' THEN NULL\n" +
        "  WHEN bar IS NULL THEN NULL\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplacementValueGivenAsNull() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04", "1989-05-31"))
            .setReplacementType(DATE)
            .setReplaceType(ReplaceType.VALUE)
            .setReplacementValue(null);

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = DATE '1970-07-04' THEN NULL\n" +
        "  WHEN bar = DATE '1989-05-31' THEN NULL\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceNullInList() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldTransformationBase transf1 =
        new FieldReplaceValue()
            .setReplacedValuesList(asList("1970-07-04", null))
            .setReplacementType(DATE)
            .setReplaceType(ReplaceType.VALUE)
            .setReplacementValue("1970-01-01");

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    String expQuery = "SELECT CASE\n" +
        "  WHEN bar = DATE '1970-07-04' THEN DATE '1970-01-01'\n" +
        "  WHEN bar IS NULL THEN DATE '1970-01-01'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));
  }

  @Test
  public void testReplaceInvalidReplacedValues() {
    boolean exThrown = false;
    try {
      VirtualDatasetState state = new VirtualDatasetState()
          .setFrom(nameDSRef);
      Expression exp0 = new ExpColumnReference("bar").wrap();

      FieldTransformationBase transf1 =
          new FieldReplaceValue()
              .setReplacedValuesList(Collections.<String>emptyList())
              .setReplacementType(DATE)
              .setReplaceType(ReplaceType.VALUE)
              .setReplacementValue("2016-11-05");

      Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

      SQLGenerator.generateSQL(state.setColumnsList(asList(new Column("foo", exp))), true, catalogService);
      fail("not expected to reach here");
    } catch (UserException e) {
      exThrown = true;
      assertEquals("select at least one value to replace", e.getMessage());
    }

    assertTrue("expected a UserException", exThrown);
  }

  @Test
  public void testReplaceRange() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);
    Expression exp0 = new ExpColumnReference("bar").wrap();

    FieldReplaceRange transf1 =
        new FieldReplaceRange()
            .setLowerBound(null) // for -infinity
            .setUpperBound(null) // for +infinity
            .setReplacementType(DATE)
            .setReplaceType(ReplaceType.VALUE)
            .setReplacementValue("2016-11-05");

    Expression exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // Expecting it to replace everything with given value
    String expQuery = "SELECT DATE '2016-11-05' AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // range includes everything except nulls
    transf1.setKeepNull(true);
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // Expecting it to replace every non-null value with given value
    expQuery = "SELECT CASE\n" +
        "  WHEN bar IS NOT NULL THEN DATE '2016-11-05'\n" +
        "  ELSE NULL\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // range includes everything and replace them with null
    transf1.setKeepNull(false);
    transf1.setReplacementValue(null);
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    expQuery = "SELECT CASE\n" +
        "  WHEN 1 = 0 THEN bar\n" +
        "  ELSE NULL\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // has lower bound
    transf1.setReplacementValue("2016-11-05");
    transf1.setLowerBound("1971-01-01");
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // expecting a lower bound condition
    expQuery = "SELECT CASE\n" +
        "  WHEN DATE '1971-01-01' < bar THEN DATE '2016-11-05'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    transf1.setReplacementValue("2016-11-05");
    transf1.setLowerBound("1971-01-01");
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // expecting a lower bound condition inclusive
    transf1.setLowerBoundInclusive(true);
    expQuery = "SELECT CASE\n" +
      "  WHEN DATE '1971-01-01' <= bar THEN DATE '2016-11-05'\n" +
      "  ELSE bar\n" +
      "END AS foo\n" +
      "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // has upper bound
    transf1.setLowerBound(null);
    transf1.setUpperBound("2016-05-10");
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // expecting an upper bound condition
    expQuery = "SELECT CASE\n" +
        "  WHEN DATE '2016-05-10' > bar THEN DATE '2016-11-05'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // expecting an upper bound condition inclusive
    transf1.setUpperBoundInclusive(true);
    expQuery = "SELECT CASE\n" +
      "  WHEN DATE '2016-05-10' >= bar THEN DATE '2016-11-05'\n" +
      "  ELSE bar\n" +
      "END AS foo\n" +
      "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // has upper and lower bounds
    transf1.setLowerBound("1971-01-01");
    transf1.setLowerBoundInclusive(false);
    transf1.setUpperBound("2016-05-10");
    transf1.setUpperBoundInclusive(null);
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // expecting upper and lower bounds in condition
    expQuery = "SELECT CASE\n" +
        "  WHEN DATE '1971-01-01' < bar AND DATE '2016-05-10' > bar THEN DATE '2016-11-05'\n" +
        "  ELSE bar\n" +
        "END AS foo\n" +
        "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

    // has upper and lower bounds
    transf1.setLowerBound("1971-01-01");
    transf1.setUpperBound("2016-05-10");
    exp = new ExpFieldTransformation(transf1.wrap(), exp0).wrap();

    // expecting upper and lower bounds in condition inclusive
    transf1.setLowerBoundInclusive(true);
    transf1.setUpperBoundInclusive(true);
    expQuery = "SELECT CASE\n" +
      "  WHEN DATE '1971-01-01' <= bar AND DATE '2016-05-10' >= bar THEN DATE '2016-11-05'\n" +
      "  ELSE bar\n" +
      "END AS foo\n" +
      "FROM myspace.parentDS";
    validateForTranformRelatedCalls(expQuery, state.setColumnsList(asList(new Column("foo", exp))));

  }

  private TransformResult transform(TransformBase tb, VirtualDatasetState state) {
    QueryExecutor executor = new QueryExecutor(null, null, null){
      @Override
      public List<String> getColumnList(String username, DatasetPath path, List<SourceVersionReference> sourceVersionReferenceList) {
        return asList("bar", "baz");
      }
    };

    TransformActor actor = new TransformActor(state, false, "test_user", executor){
      @Override
      protected QueryMetadata getMetadata(SqlQuery query) {
        return Mockito.mock(QueryMetadata.class);
      }

      @Override
      protected boolean hasMetadata() {
        return true;
      }

      @Override
      protected QueryMetadata getMetadata() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<BatchSchema> getBatchSchema() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<ParentDatasetInfo>> getParents() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<FieldOrigin>> getFieldOrigins() {
        throw new IllegalStateException("not implemented");
      }

      @Override
      protected Optional<List<ParentDataset>> getGrandParents() {
        throw new IllegalStateException("not implemented");
      }
    };

    return tb.accept(actor);
  }

  @Test
  public void testFilterSummed() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression a = new ExpColumnReference("a").wrap();
    Expression b = new ExpColumnReference("b").wrap();

    state.setColumnsList(asList(new Column("a", a), new Column("b", b)));

    TransformResult sumTransform = transform(new TransformGroupBy()
      .setColumnsDimensionsList(asList(new Dimension("a")))
      .setColumnsMeasuresList(asList(new Measure(Sum).setColumn("b"))), state);
    state = sumTransform.getNewState();

    String expSumQuery =
      "SELECT a, SUM(b) AS Sum_b\n" +
      "FROM myspace.parentDS\n" +
      "GROUP BY a";
    validateForTranformRelatedCalls(expSumQuery, state);

    TransformResult keeponlyTransform = transform( new TransformFilter("Sum_b", new FilterDefinition(FilterType.Range)
      .setRange(new FilterRange(DataType.INTEGER).setLowerBound("1"))), state);
    VirtualDatasetState filtState = keeponlyTransform.getNewState();

    String expQuery =
      "SELECT a, Sum_b\n" +
      "FROM (\n" +
      "  SELECT a, SUM(b) AS Sum_b\n" +
      "  FROM myspace.parentDS\n" +
      "  GROUP BY a\n" +
      ") nested_0\n" +
      " WHERE 1 < Sum_b";
    validateForTranformRelatedCalls(expQuery, filtState);
  }

  @Test
  public void testNullFilterExcludeKeepOnly() {
    VirtualDatasetState state = new VirtualDatasetState()
        .setFrom(nameDSRef);

    Expression column = new ExpColumnReference("user").wrap();

    //exclude null and multiple non-null values
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NOT NULL AND parentDS.\"user\" NOT IN ('foo', 'bar')",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList(null, "foo", "bar")))).setExclude(true)
        ))
    );

    //exclude null and a single non-null value
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NOT NULL AND parentDS.\"user\" <> 'foo'",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList(null, "foo")))).setExclude(true)
        ))
    );

    //exclude just null (and no other values)
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NOT NULL",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList((String)null)))).setExclude(true)
        ))
    );

    //keeponly null and multiple non-null values
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NULL OR parentDS.\"user\" IN ('foo', 'bar')",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList(null, "foo", "bar")))).setExclude(false)
        ))
    );

    //keeponly null and a single non-null value
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NULL OR parentDS.\"user\" = 'foo'",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList(null, "foo")))).setExclude(false)
        ))
    );

    //keeponly just null (and no other values)
    validateForTranformRelatedCalls(
      "SELECT parentDS.\"user\" AS foo\nFROM myspace.parentDS\n " +
        "WHERE parentDS.\"user\" IS NULL",
      state
        .setColumnsList(asList(new Column("foo", column)))
        .setFiltersList(asList(new Filter(column, new FilterDefinition(FilterType.Value)
          .setValue(new FilterValue(DataType.DATE).setValuesList(asList((String)null)))).setExclude(false)
        ))
    );
  }

  public static Column makeColumnSelection(String colName, String colAlias, String table) {
    Expression a = new ExpColumnReference(colName).setTable(table).wrap();
    return new Column(colAlias, a);
  }

  @Test
  public void testJoinSelectionCollision() {
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(new FromTable("\"space\".\"t1\"").setAlias("t1").wrap());

    validateForTranformRelatedCalls(
      "SELECT t1.id AS id, t2.id AS id0, t1.a AS a, t2.A AS A_0\n" +
        "FROM space.t1 AS t1\n" +
        " INNER JOIN \"space\".t2 AS t2 ON t1.id = t2.id",
      state.setJoinsList(asList(makeJoin(JoinType.Inner, "\"space\".t2", "t2", "id")))
        .setColumnsList(asList(
        makeColumnSelection("id", "id", "t1"),
        makeColumnSelection("id", "id0", "t2"),
        makeColumnSelection("a", "a", "t1"),
        makeColumnSelection("A", "A", "t2")
      ))
    );
  }

  private Join makeJoin(JoinType type, String table, String tableAlias, String field) {
    return new Join(type, table, tableAlias)
      .setJoinConditionsList(asList(new JoinCondition(field, field)));
  }

  private static class ColumnSpec {
    private final String table;
    private final String colName;
    private final String desiredColAlias;
    private final String expectedColAlias;

    public ColumnSpec(String table, String colName, String desiredColAlias, String expectedColAlias) {
      this.table = table;
      this.colName = colName;
      this.desiredColAlias = desiredColAlias;
      this.expectedColAlias = expectedColAlias;
    }

    public Column makeColumnSelection() {
      return TestSQLGenerator.makeColumnSelection(colName, desiredColAlias, table);
    }

    public String makeSelectionString() {
      return String.format(", %s.%s AS %s", table, colName, expectedColAlias);
    }
  }

  private void validateJoinColumnCollisionTest(String t1, String t2, String t3, List<ColumnSpec> selections) {
    VirtualDatasetState state = new VirtualDatasetState()
      .setFrom(new FromTable(String.format("\"space\".\"%s\"", t1)).setAlias(t1).wrap());

    String sColumns = String.format("%s.id AS id, %s.id AS id0, %s.id AS id1", t1, t2, t3);

    List<Column> colSelections = new LinkedList<>(asList(
      makeColumnSelection("id", "id", t1),
      makeColumnSelection("id", "id0", t2),
      makeColumnSelection("id", "id1", t3)
    ));

    for (ColumnSpec col:
      selections) {
      colSelections.add(col.makeColumnSelection());
      sColumns += col.makeSelectionString();
    }

    validateForTranformRelatedCalls(
      "SELECT " +
        String.format("%s\n",sColumns) +
        String.format("FROM space.%s AS %s\n", t1, t1) +
        String.format(" INNER JOIN \"space\".%s AS %s ON %s.id = %s.id\n", t2, t2, t1, t2) +
        String.format(" LEFT JOIN \"space\".%s AS %s ON %s.id = %s.id", t3, t3, t1, t3),
      state.setJoinsList(asList(
        makeJoin(JoinType.Inner, "\"space\"." + t2, t2, "id"),
        makeJoin(JoinType.LeftOuter, "\"space\"." + t3, t3, "id")
      ))
        .setColumnsList(colSelections)
    );
  }

  @Test
  public void testColumnCollisionDoesntAlterAliasBaseWhenItEndsInNumbers() {
    validateJoinColumnCollisionTest("t1", "t2", "t3", asList(
      new ColumnSpec("t1", "A", "a", "a"),
      new ColumnSpec("t2", "A", "A1993", "A1993"),
      new ColumnSpec("t3", "A", "a1993", "a1993_0"),
      new ColumnSpec("t1", "b", "A1993", "A1993_1")
    ));
  }

  @Test
  public void testColumnCollisionOnlyRenamesIfWholeConflicts() {
    validateJoinColumnCollisionTest("t1", "t2", "t3", asList(
      new ColumnSpec("t1", "a", "a", "a"),
      new ColumnSpec("t2", "A", "A1994", "A1994"),
      new ColumnSpec("t3", "a", "A1993", "A1993")
    ));
  }

  @Test
  public void testColumnCollisionRemembersRenames() {
    validateJoinColumnCollisionTest("t1", "t2", "t3", asList(
      new ColumnSpec("t1", "a", "a", "a"),
      new ColumnSpec("t2", "A", "A1993", "A1993"),
      new ColumnSpec("t3", "A", "A1993", "A1993_0"),
      new ColumnSpec("t1", "a_93", "A1993_0", "A1993_0_0"),
      new ColumnSpec("t2", "a_93", "a1993_0", "a1993_0_1"),
      new ColumnSpec("t3", "a_93", "A1993_0_0", "A1993_0_0_0")
    ));
  }

  @Test
  public void testColumnCollisionResolvesUnfriendlyNaming() {
    validateJoinColumnCollisionTest("t1", "t2", "t3", asList(
      new ColumnSpec("t1", "a", "a", "a"),
      new ColumnSpec("t2", "A", "A1993", "A1993"),
      new ColumnSpec("t3", "A", "a1993_0","a1993_0"),
      new ColumnSpec("t1", "B", "A1993_1", "A1993_1"),
      new ColumnSpec("t2", "B", "A1993_2", "A1993_2"),
      new ColumnSpec("t3", "B", "a1993", "a1993_3")
    ));
  }
}
