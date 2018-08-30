/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

public class TestCastFunctions extends BaseTestQuery {

  @Test
  public void testVarbinaryToDate() throws Exception {
    errorMsgTestHelper("select count(*) as cnt from cp.\"employee.json\" where (cast(convert_to(birth_date, 'utf8') as date)) = date '1961-08-26'",
        "Cast function cannot convert value of type VARBINARY(65536) to type DATE");
  }

  @Test // DRILL-2827
  public void testImplicitCastStringToBoolean() throws Exception {
    String boolTable= FileUtils.getResourceAsFile("/store/json/booleanData.json").toURI().toString();

    String query = String.format(
        "(select * from dfs_test.\"%s\" where key = 'true' or key = 'false')", boolTable);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build().run();
  }

  @Test // DRILL-2808
  public void testCastByConstantFolding() throws Exception {
    final String query = "SELECT count(DISTINCT employee_id) as col1, " +
        "count((to_number(date_diff(now(), cast(birth_date AS date)),'####'))) as col2 \n" +
        "FROM cp.\"employee.json\"";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col1", "col2")
        .baselineValues(1155l, 1155l)
        .build()
        .run();
  }

  @Test // DRILL-3769
  public void testToDateForTimeStamp() throws Exception {
    final String query = "select to_date(to_timestamp(-1)) as col \n" +
        "from (values(1))";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(new LocalDateTime(1969, 12, 31, 0, 0))
        .build()
        .run();
  }

  @Test
  public void timeToTimestampCast() throws Exception {
    final String query = "select cast(t as timestamp) ts from (values(time '02:12:23'), (time '14:23:23')) as tbl(t)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ts")
        .baselineValues(new LocalDateTime(1970, 01, 01, 2, 12, 23))
        .baselineValues(new LocalDateTime(1970, 01, 01, 14, 23, 23))
        .build()
        .run();
  }

  @Test
  public void testFailedCast() throws Exception {
    final String query = "select cast('trueX' as boolean) from (values(1))";

    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.FUNCTION));
    test(query);
  }

  @Test
  public void testToIntervalYearCast() throws Exception {
    String query = "select cast(a as interval year) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.years(4))
        .build()
        .run();

    query = "select cast(4 as interval year) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.years(4))
        .build()
        .run();

    // Test math expression case for cast functions
    query = "select cast((a + b) as interval year) as col from (values(4, 4)) as tbl(a, b)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.years(8))
        .build()
        .run();
  }

  @Test
  public void testToIntervalMonthCast() throws Exception {
    String query = "select cast(a as interval month) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.months(4))
        .build()
        .run();

    query = "select cast(4 as interval month) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.months(4))
        .build()
        .run();
  }

  @Test
  public void testToIntervalDayCast() throws Exception {
    String query = "select cast(a as interval day) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.days(4))
        .build()
        .run();

    query = "select cast(4 as interval day) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.days(4))
        .build()
        .run();
  }

  @Test
  public void testToIntervalHourCast() throws Exception {
    String query = "select cast(a as interval hour) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.hours(4))
        .build()
        .run();

    query = "select cast(4 as interval hour) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.hours(4))
        .build()
        .run();
  }

  @Test
  public void testToIntervalMinuteCast() throws Exception {
    String query = "select cast(a as interval minute) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.minutes(4))
        .build()
        .run();

    query = "select cast(4 as interval minute) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.minutes(4))
        .build()
        .run();
  }

  @Test
  public void testToIntervalSecondCast() throws Exception {
    String query = "select cast(a as interval second) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.seconds(4))
        .build()
        .run();

    query = "select cast(4 as interval second) as col from (values(4)) as tbl(a)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(Period.seconds(4))
        .build()
        .run();
  }
}
