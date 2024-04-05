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
package com.dremio.exec.fn.impl;

import static com.dremio.sabot.Fixtures.ts;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import org.joda.time.LocalDateTime;
import org.junit.Test;

public class TestSingleValueFunctions extends BaseTestQuery {

  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";
  private static final String NON_SCALAR_ERROR_MESSAGE =
      "Subqueries used in expressions must be scalar (must return a single value).";

  @Test
  public void testSingleValueWhereClauseInt() throws Exception {
    final String query =
        String.format(
            "select employee_id from dfs.\"%s/employees.json\" where employee_id"
                + "= (select distinct (employee_id) from dfs.\"%s/employees.json\" where first_name = 'Amy')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("employee_id")
        .baselineValues(1113L)
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseFloat() throws Exception {
    final String query =
        String.format(
            "select rating from dfs.\"%s/employees.json\" where rating"
                + "= (select distinct (rating) from dfs.\"%s/employees.json\" where first_name = 'William')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("rating")
        .baselineValues(79.06)
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseVarChar() throws Exception {
    final String query =
        String.format(
            "select first_name from dfs.\"%s/employees.json\" where first_name"
                + "= (select distinct (first_name) from dfs.\"%s/employees.json\" where first_name = 'William')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("first_name")
        .baselineValues("William")
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseVarBinary() throws Exception {
    final String query =
        String.format(
            "select first_name from dfs.\"%s/employees.json\" where (convert_to(first_name, 'utf8'))"
                + "= (select distinct (convert_to(first_name, 'utf8')) from dfs.\"%s/employees.json\" where first_name = 'William')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("first_name")
        .baselineValues("William")
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseDateMilli() throws Exception {
    final String query =
        "select to_date(a) as datemilli from (VALUES(123456)) as tbl(a) where to_date(a)"
            + "= (select distinct (to_date(b)) from (VALUES(123456)) as tbl(b))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("datemilli")
        .baselineValues(ts("1970-01-02T00:00:00.000"))
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseTimeStampMilli() throws Exception {
    final String query =
        "select cast(a as timestamp) ts from (VALUES(time '02:12:23')) as tbl(a) where cast(a as timestamp)"
            + "= (select distinct (cast(b as timestamp)) from (VALUES(time '02:12:23')) as tbl(b))";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ts")
        .baselineValues(new LocalDateTime(1970, 01, 01, 2, 12, 23))
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseIntervalYear() throws Exception {
    final String query =
        String.format(
            "select stringinterval as interval_year from dfs.\"%s/test_simple_interval.json\" where cast(stringinterval as interval year)"
                + "= (select distinct (cast(stringinterval as interval year)) from dfs.\"%s/test_simple_interval.json\" where stringinterval = 'P2Y2M1DT1H20M35S')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("interval_year")
        .baselineValues("P2Y2M1DT1H20M35S")
        .baselineValues("P2Y2M1DT1H20M35.897S")
        .baselineValues("P2Y2M")
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseIntervalDay() throws Exception {
    final String query =
        String.format(
            "select stringinterval as interval_day from dfs.\"%s/test_simple_interval.json\" where cast(stringinterval as interval day)"
                + "= (select distinct (cast(stringinterval as interval day)) from dfs.\"%s/test_simple_interval.json\" where stringinterval = 'P2Y2M1DT1H20M35S')",
            TEST_RES_PATH, TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("interval_day")
        .baselineValues("P2Y2M1DT1H20M35S")
        .build()
        .run();
  }

  @Test
  public void testSingleValueWhereClauseIntNonScalar() throws Exception {
    final String query =
        String.format(
            "select employee_id from dfs.\"%s/employees.json\" where employee_id"
                + "= (select distinct (employee_id) from dfs.\"%s/employees.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseFloatNonScalar() throws Exception {
    final String query =
        String.format(
            "select rating from dfs.\"%s/employees.json\" where rating"
                + "= (select distinct (rating) from dfs.\"%s/employees.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseVarCharNonScalar() throws Exception {
    final String query =
        String.format(
            "select first_name from dfs.\"%s/employees.json\" where first_name"
                + "= (select distinct (first_name) from dfs.\"%s/employees.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseVarBinaryNonScalar() throws Exception {
    final String query =
        String.format(
            "select first_name from dfs.\"%s/employees.json\" where (convert_to(first_name, 'utf8'))"
                + "= (select distinct (convert_to(first_name, 'utf8')) from dfs.\"%s/employees.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseDateMilliNonScalar() throws Exception {
    final String query =
        "select to_date(a) as datemilli from (VALUES(123456)) as tbl(a) where to_date(a)"
            + "= (select distinct (to_date(b)) from (VALUES(123456), (654321)) as tbl(b))";

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseTimeStampMilliNonScalar() throws Exception {
    final String query =
        "select cast(a as timestamp) ts from (VALUES(time '02:12:23')) as tbl(a) where cast(a as timestamp)"
            + "= (select distinct (cast(b as timestamp)) from (VALUES(time '02:12:23'), (time '03:13:24')) as tbl(b))";

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseIntervalYearNonScalar() throws Exception {
    final String query =
        String.format(
            "select stringinterval as interval_year from dfs.\"%s/test_simple_interval.json\" where cast(stringinterval as interval year)"
                + "= (select distinct (cast(stringinterval as interval year)) from dfs.\"%s/test_simple_interval.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueWhereClauseIntervalDayNonScalar() throws Exception {
    final String query =
        String.format(
            "select stringinterval as interval_day from dfs.\"%s/test_simple_interval.json\" where cast(stringinterval as interval day)"
                + "= (select distinct (cast(stringinterval as interval day)) from dfs.\"%s/test_simple_interval.json\")",
            TEST_RES_PATH, TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseNull() throws Exception {
    final String query =
        String.format(
            "select (select rating from dfs.\"%s/employees_with_null.json\" where first_name = 'Sandy')",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(null)
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseInt() throws Exception {
    final String query =
        String.format(
            "select (select distinct (employee_id) from dfs.\"%s/employees.json\" where first_name = 'Amy') employee_id",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("employee_id")
        .baselineValues(1113L)
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseFloat() throws Exception {
    final String query =
        String.format(
            "select (select distinct (rating) from dfs.\"%s/employees.json\" where first_name = 'William') rating",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("rating")
        .baselineValues(79.06)
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseVarChar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (first_name) from dfs.\"%s/employees.json\" where first_name = 'William') first_name",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("first_name")
        .baselineValues("William")
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseVarBinary() throws Exception {
    final String query =
        String.format(
            "select (select distinct (convert_to(first_name, 'utf8')) from dfs.\"%s/employees.json\" where first_name = 'William') name",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("name")
        .baselineValues("William".getBytes())
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseDateMilli() throws Exception {
    final String query =
        "select (select distinct (to_date(a)) from (VALUES(123456)) as tbl(a)) datemilli";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("datemilli")
        .baselineValues(ts("1970-01-02T00:00:00.000"))
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseTimeStampMilli() throws Exception {
    final String query =
        "select (select distinct (cast(a as timestamp)) from (VALUES(time '02:12:23')) as tbl(a)) ts";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ts")
        .baselineValues(new LocalDateTime(1970, 01, 01, 2, 12, 23))
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseIntervalYear() throws Exception {
    final String query =
        String.format(
            "select cast((select distinct (cast(stringinterval as interval year)) from dfs.\"%s/test_simple_interval.json\" where stringinterval = 'P2Y2M1DT1H20M35S') as varchar(65536)) as interval_year",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("interval_year")
        .baselineValues("2 years 2 months ")
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseIntervalDay() throws Exception {
    final String query =
        String.format(
            "select cast((select distinct (cast(stringinterval as interval day)) from dfs.\"%s/test_simple_interval.json\" where stringinterval = 'P2Y2M1DT1H20M35S') as varchar(65536)) as interval_day",
            TEST_RES_PATH);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("interval_day")
        .baselineValues("1 day 1:20:35.0")
        .build()
        .run();
  }

  @Test
  public void testSingleValueSelectClauseNullAndNull() throws Exception {
    final String query =
        String.format(
            "select (select (position_id) from dfs.\"%s/employees_with_null.json\" where first_name in ('William', 'Bh'))",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseNonNullAndNull() throws Exception {
    final String query =
        String.format(
            "select (select (position_id) from dfs.\"%s/employees_with_null.json\" where first_name in ('Aaron', 'Bh'))",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseNullAndNonNull() throws Exception {
    final String query =
        String.format(
            "select (select (position_id) from dfs.\"%s/employees_with_null.json\" where first_name in ('Bh', 'Aaron'))",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseIntNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (employee_id) from dfs.\"%s/employees.json\") employee_id",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseFloatNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (rating) from dfs.\"%s/employees.json\") rating",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseVarCharNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (first_name) from dfs.\"%s/employees.json\") first_name",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  // problem
  @Test
  public void testSingleValueSelectClauseVarBinaryNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (convert_to(first_name, 'utf8')) from dfs.\"%s/employees.json\") name",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseDateMilliNonScalar() throws Exception {
    final String query =
        "select (select distinct (to_date(a)) from (VALUES(123456), (654321)) as tbl(a)) datemilli";

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseTimeStampMilliNonScalar() throws Exception {
    final String query =
        "select (select distinct (cast(a as timestamp)) from (VALUES(time '02:12:23'), (time '03:13:24')) as tbl(a)) ts";

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseIntervalYearNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (cast(stringinterval as interval year)) from dfs.\"%s/test_simple_interval.json\") as interval_year",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }

  @Test
  public void testSingleValueSelectClauseIntervalDayNonScalar() throws Exception {
    final String query =
        String.format(
            "select (select distinct (cast(stringinterval as interval day)) from dfs.\"%s/test_simple_interval.json\") as interval_day",
            TEST_RES_PATH);

    errorMsgTestHelper(query, NON_SCALAR_ERROR_MESSAGE);
  }
}
