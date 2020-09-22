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
package com.dremio.exec.physical.impl.limit;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Lists;

public class TestEarlyLimit0Optimization extends BaseTestQuery {

  private static final String viewName = "limitZeroEmployeeView";
  private static final String stddevTypesViewName = "student_csv_v";

  private static String wrapLimit0(final String query) {
    return "SELECT * FROM (" + query + ") LZT LIMIT 0";
  }

  @ClassRule
  public static TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void createView() throws Exception {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
    test("USE dfs_test");
    test(String.format("CREATE OR REPLACE VIEW %s AS SELECT " +
        "CAST(employee_id AS INT) AS employee_id, " +
        "CAST(full_name AS VARCHAR(25)) AS full_name, " +
        "CAST(position_id AS INTEGER) AS position_id, " +
        "CAST(department_id AS BIGINT) AS department_id," +
        "CAST(birth_date AS DATE) AS birth_date, " +
        "CAST(hire_date AS TIMESTAMP) AS hire_date, " +
        "CAST(salary AS DOUBLE) AS salary, " +
        "CAST(salary AS FLOAT) AS fsalary, " +
        "CAST((CASE WHEN marital_status = 'S' THEN true ELSE false END) AS BOOLEAN) AS single, " +
        "CAST(education_level AS VARCHAR(60)) AS education_level," +
        "CAST(gender AS CHAR) AS gender " +
        "FROM cp.\"employee.json\" " +
        "ORDER BY employee_id " +
        "LIMIT 1;", viewName));
    // { "employee_id":1,"full_name":"Sheri Nowmer","first_name":"Sheri","last_name":"Nowmer","position_id":1,
    // "position_title":"President","store_id":0,"department_id":1,"birth_date":"1961-08-26",
    // "hire_date":"1994-12-01 00:00:00.0","end_date":null,"salary":80000.0000,"supervisor_id":0,
    // "education_level":"Graduate Degree","marital_status":"S","gender":"F","management_role":"Senior Management" }

    test(String.format("create or replace view %s as select " +
            "cast(columns[0] as integer) student_id, " +
            "cast(columns[1] as varchar(30)) name, " +
            "cast(columns[2] as int) age, " +
            "cast(columns[3] as double) gpa, " +
            "cast(columns[4] as bigint) studentnum, " +
            "cast(columns[5] as timestamp) create_time " +
            "from cp.\"textinput/students.csv\"",
        stddevTypesViewName));
  }

  @AfterClass
  public static void tearDownView() throws Exception {
    test("DROP VIEW " + viewName + ";");
    test("DROP VIEW " + stddevTypesViewName + ";");
  }

  @Before
  public void setOption() throws Exception {
    test("SET \"%s\" = true;", ExecConstants.EARLY_LIMIT0_OPT_KEY);
  }

  @After
  public void resetOption() throws Exception {
    test("RESET \"%s\";", ExecConstants.EARLY_LIMIT0_OPT_KEY);
  }

  // -------------------- SIMPLE QUERIES --------------------

  @Test
  @Ignore("DX-2490")
  public void infoSchema() throws Exception {
    testBuilder()
        .sqlQuery(String.format("DESCRIBE %s", viewName))
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("employee_id", "INTEGER", "YES")
        .baselineValues("full_name", "CHARACTER VARYING", "YES")
        .baselineValues("position_id", "INTEGER", "YES")
        .baselineValues("department_id", "BIGINT", "YES")
        .baselineValues("birth_date", "DATE", "YES")
        .baselineValues("hire_date", "TIMESTAMP", "YES")
        .baselineValues("salary", "DOUBLE", "YES")
        .baselineValues("fsalary", "FLOAT", "YES")
        .baselineValues("single", "BOOLEAN", "NO")
        .baselineValues("education_level", "CHARACTER VARYING", "YES")
        .baselineValues("gender", "CHARACTER", "YES")
        .go();
  }

  @Test
  public void simpleSelect() throws Exception {
    DateTimeFormatter formatter = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD");
    testBuilder()
        .sqlQuery(String.format("SELECT * FROM %s", viewName))
        .ordered()
        .baselineColumns("employee_id", "full_name", "position_id", "department_id", "birth_date", "hire_date",
            "salary", "fsalary", "single", "education_level", "gender")
        .baselineValues(1, "Sheri Nowmer", 1, 1L, formatter.parseLocalDateTime("1961-08-26"),
            formatter.parseLocalDateTime("1994-12-01"), 80000.0D, 80000.0F, true, "Graduate Degree", "F")
        .go();
  }

  @Test
  public void simpleSelectLimit0() throws Exception {
    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("employee_id"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("full_name"), Types.optional(MinorType.VARCHAR)),
        Pair.of(SchemaPath.getSimplePath("position_id"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("department_id"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("birth_date"), Types.optional(MinorType.DATE)),
        Pair.of(SchemaPath.getSimplePath("hire_date"), Types.optional(MinorType.TIMESTAMP)),
        Pair.of(SchemaPath.getSimplePath("salary"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("fsalary"), Types.optional(MinorType.FLOAT4)),
        Pair.of(SchemaPath.getSimplePath("single"), Types.required(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("education_level"), Types.optional(MinorType.VARCHAR)),
        Pair.of(SchemaPath.getSimplePath("gender"), Types.optional(MinorType.VARCHAR)));

    testBuilder()
        .sqlQuery(wrapLimit0(String.format("SELECT * FROM %s", viewName)))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized("SELECT * FROM " + viewName);
  }

  private static void checkThatQueryPlanIsOptimized(final String query) throws Exception {
    PlanTestBase.testPlanMatchingPatterns(wrapLimit0(query), new String[]{"Empty\\("});
  }

  // -------------------- AGGREGATE FUNC. QUERIES --------------------

  private static String getAggQuery(final String functionName) {
    return "SELECT " +
        functionName + "(employee_id) AS e, " +
        functionName + "(position_id) AS p, " +
        functionName + "(department_id) AS d, " +
        functionName + "(salary) AS s, " +
        functionName + "(fsalary) AS f " +
        "FROM " + viewName;
  }

  @Test
  public void sums() throws Exception {
    final String query = getAggQuery("SUM");

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("d"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("f"), Types.optional(MinorType.FLOAT8)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("e", "p", "d", "s", "f")
        .baselineValues(1L, 1L, 1L, 80000D, 80000D)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void counts() throws Exception {
    final String query = getAggQuery("COUNT");

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("e"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("d"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("s"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("f"), Types.required(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e", "p", "d", "s", "f")
        .ordered()
        .baselineValues(1L, 1L, 1L, 1L, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  private void minAndMaxTest(final String functionName) throws Exception {
    final String query = getAggQuery(functionName);

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("d"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("f"), Types.optional(MinorType.FLOAT4)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e", "p", "d", "s", "f")
        .ordered()
        .baselineValues(1, 1, 1L, 80_000D, 80_000F)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void mins() throws Exception {
    minAndMaxTest("MIN");
  }

  @Test
  public void maxs() throws Exception {
    minAndMaxTest("MAX");
  }

  @Test
  public void avgs() throws Exception {
    final String query = getAggQuery("AVG");

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("d"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("f"), Types.optional(MinorType.FLOAT8)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("e", "p", "d", "s", "f")
        .baselineValues(1D, 1D, 1D, 80_000D, 80_000D)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void measures() throws Exception {
    final String query = "SELECT " +
        "STDDEV_SAMP(employee_id) AS s, " +
        "STDDEV_POP(position_id) AS p, " +
        "AVG(position_id) AS a, " +
        "COUNT(position_id) AS c " +
        "FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("a"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("c"), Types.required(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s", "p", "a", "c")
        .baselineValues(null, 0.0D, 1.0D, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void measuresStddev() throws Exception {
    final String query = "SELECT " +
        "STDDEV_SAMP(employee_id) AS s, " +
        "STDDEV_POP(position_id) AS p, " +
        "STDDEV_SAMP(salary) AS ss, " +
        "STDDEV_POP(fsalary) AS fs " +
        "FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("ss"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("fs"), Types.required(MinorType.FLOAT8)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s", "p", "ss", "fs")
        .baselineValues(null, 0.0D, null, 0.0D)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void stddev() throws Exception {
    final String query = "select stddev(student_id),stddev(age),stddev(gpa),stddev(studentnum) from " + stddevTypesViewName + " where age > 30";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("EXPR$0"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("EXPR$1"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("EXPR$2"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("EXPR$3"), Types.required(MinorType.FLOAT8)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3")
        .baselineValues(14.620274929849652D, 13.017389869877539D, 0.8640723208214439D, 2.9986511701873456E11D)
        .go();
    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void mod() throws Exception {
    final String query = "select mod(student_id, 3), mod(age, 2), mod(studentnum, 1235) from " + stddevTypesViewName + " where student_id=10";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("EXPR$0"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("EXPR$1"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("EXPR$2"), Types.optional(MinorType.INT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2")
        .baselineValues(1, 0, 304)
        .go();
    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void position() throws Exception {
    final String query = "select position('i' in full_name) from " + viewName+ " where full_name like '%i%'";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("EXPR$0"), Types.optional(MinorType.INT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(5)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void charlength() throws Exception {
    final String query = "select char_length(full_name) from " + viewName+ " where full_name like '%i%'";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("EXPR$0"), Types.optional(MinorType.INT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(12)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void extractSecond() throws Exception {
    final String query = "SELECT " +
        "extract(SECOND FROM time '2:30:21.5') as \"second\", " +
        "extract(MINUTE FROM time '2:30:21.5') as \"minute\", " +
        "extract(HOUR FROM time '2:30:21.5') as \"hour\" " +
        "FROM sys.version";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("second"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("minute"), Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("hour"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour")
        .baselineValues(
            21L, // seconds
            30L, // minute
            2L) // hour
        .go();



    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void nullableCount() throws Exception {
    final String query = "SELECT " +
        "COUNT(CASE WHEN position_id = 1 THEN NULL ELSE position_id END) AS c FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("c"), Types.required(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("c")
        .baselineValues(0L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void nullableSumAndCount() throws Exception {
    final String query = "SELECT " +
        "COUNT(position_id) AS c, " +
        "SUM(CAST((CASE WHEN position_id = 1 THEN NULL ELSE position_id END) AS INT)) AS p " +
        "FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("c"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("c", "p")
        .baselineValues(1L, null)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void castSum() throws Exception {
    final String query = "SELECT CAST(SUM(position_id) AS INT) AS s FROM cp.\"employee.json\"";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.INT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s")
        .baselineValues(18422)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void sumCast() throws Exception {
    final String query = "SELECT SUM(CAST(position_id AS INT)) AS s FROM cp.\"employee.json\"";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s")
        .baselineValues(18422L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void sumsAndCounts1() throws Exception {
    final String query = "SELECT " +
        "COUNT(*) as cs, " +
        "COUNT(1) as c1, " +
        "COUNT(employee_id) as cc, " +
        "SUM(1) as s1," +
        "department_id " +
        " FROM " + viewName + " GROUP BY department_id";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("cs"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("c1"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("cc"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("s1"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("department_id"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cs", "c1", "cc", "s1", "department_id")
        .baselineValues(1L, 1L, 1L, 1L, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void sumsAndCounts2() throws Exception {
    final String query = "SELECT " +
        "SUM(1) as s1, " +
        "COUNT(1) as c1, " +
        "COUNT(*) as cs, " +
        "COUNT(CAST(n_regionkey AS INT)) as cc " +
        "FROM cp.\"tpch/nation.parquet\" " +
        "GROUP BY CAST(n_regionkey AS INT)";

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s1"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("c1"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("cs"), Types.required(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("cc"), Types.required(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("s1", "c1", "cs", "cc")
        .baselineValues(5L, 5L, 5L, 5L)
        .baselineValues(5L, 5L, 5L, 5L)
        .baselineValues(5L, 5L, 5L, 5L)
        .baselineValues(5L, 5L, 5L, 5L)
        .baselineValues(5L, 5L, 5L, 5L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);

  }

  @Test
  public void rank() throws Exception {
    final String query = "SELECT RANK() OVER(PARTITION BY employee_id ORDER BY employee_id) AS r FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("r"), Types.required(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("r")
        .baselineValues(1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  // -------------------- SCALAR FUNC. QUERIES --------------------

  @Test
  public void cast() throws Exception {
    final String query = "SELECT CAST(fsalary AS DOUBLE) AS d," +
        "CAST(employee_id AS BIGINT) AS e FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("d"), Types.optional(MinorType.FLOAT8)),
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("d", "e")
        .ordered()
        .baselineValues(80_000D, 1L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  public void concatTest(final String query) throws Exception {
    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("c"), Types.optional(MinorType.VARCHAR)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("c")
        .ordered()
        .baselineValues("Sheri NowmerGraduate Degree")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void concat() throws Exception {
    concatTest("SELECT CONCAT(full_name, education_level) AS c FROM " + viewName);
  }

  @Test
  public void concatOp() throws Exception {
    concatTest("SELECT full_name || education_level AS c FROM " + viewName);
  }

  @Test
  public void extract() throws Exception {
    final String query = "SELECT EXTRACT(YEAR FROM hire_date) AS e FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.BIGINT)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("e")
        .ordered()
        .baselineValues(1994L)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void binary() throws Exception {
    final String query = "SELECT " +
        "single AND true AS b, " +
        "full_name || education_level AS c, " +
        "position_id / position_id AS d, " +
        "position_id = position_id AS e, " +
        "position_id > position_id AS g, " +
        "position_id >= position_id AS ge, " +
        "position_id IN (0, 1) AS i, +" +
        "position_id < position_id AS l, " +
        "position_id <= position_id AS le, " +
        "position_id - position_id AS m, " +
        "position_id * position_id AS mu, " +
        "position_id <> position_id AS n, " +
        "single OR false AS o, " +
        "position_id + position_id AS p FROM " + viewName;

    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("b"), Types.required(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("c"), Types.optional(MinorType.VARCHAR)),
        Pair.of(SchemaPath.getSimplePath("d"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("e"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("g"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("ge"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("i"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("l"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("le"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("m"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("mu"), Types.optional(MinorType.INT)),
        Pair.of(SchemaPath.getSimplePath("n"), Types.optional(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("o"), Types.required(MinorType.BIT)),
        Pair.of(SchemaPath.getSimplePath("p"), Types.optional(MinorType.INT)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("b", "c", "d", "e", "g", "ge", "i", "l", "le", "m", "mu", "n", "o", "p")
        .ordered()
        .baselineValues(true, "Sheri NowmerGraduate Degree", 1, true, false, true, true, false, true,
            0, 1, false, true, 2)
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  public void substringTest(final String query) throws Exception {
    @SuppressWarnings("unchecked")
    final List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList(
        Pair.of(SchemaPath.getSimplePath("s"), Types.optional(MinorType.VARCHAR)));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("s")
        .ordered()
        .baselineValues("Sheri")
        .go();

    testBuilder()
        .sqlQuery(wrapLimit0(query))
        .schemaBaseLine(expectedSchema)
        .go();

    checkThatQueryPlanIsOptimized(query);
  }

  @Test
  public void substring() throws Exception {
    substringTest("SELECT SUBSTRING(full_name, 1, 5) AS s FROM " + viewName);
  }

  @Test
  public void substr() throws Exception {
    substringTest("SELECT SUBSTR(full_name, 1, 5) AS s FROM " + viewName);
  }


  /* Tests for limit 0 optimization when reduce expressions are fired.  In this case, we are using ReduceExpressionsRule on filters.
   */
  @Test
  public void testTextReduceFilterExpressionCsv() throws Exception {
    String query = "select * from cp.\"/store/text/classpath_storage_csv_test.csv\" where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});
  }

  @Test
  public void testTextReduceFilterExpressionJson() throws Exception {
    String query = "select * from cp.\"customer.json\" where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});

    query = "select lname, address2, fullname from cp.\"customer.json\" where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});
  }

  @Test
  public void testTextReduceFilterExpressionComplexParquet() throws Exception {
    String query = "select * from cp.\"parquet/complex.parquet\" where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});

    query = "select recipe from cp.\"parquet/complex.parquet\" c where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});
  }

  @Test
  public void testTextReduceFilterExpressionParquet() throws Exception {
    String query = "select * from cp.\"tpch/nation.parquet\" where 1=0";
    PlanTestBase.testPlanMatchingPatterns(query, new String[] {"Empty\\("});
  }



}
