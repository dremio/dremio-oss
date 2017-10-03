/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.rpc.RpcException;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;

import static org.apache.arrow.vector.util.DateUtility.formatTimeStampMilli;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNewDateFunctions extends BaseTestQuery {
  LocalDateTime date;
  long unixTimeStamp = -1;

  private final String dateValues = "(values('1900-01-01'), ('3500-01-01'), ('2000-12-31'), ('2005-12-32'), ('2015-02-29'), (cast(null as varchar))) as t(date1)";

  @Before
  public void before() {
    DateTimeZone.setDefault(DateTimeZone.forID("America/Los_Angeles"));
  }

  @Test
  public void testDateToChar() throws Exception {
    final String dateValues = "(values(date '1900-01-01'), (date '3500-01-01'), (date '2000-12-31'), (date '2005-12-30'), (date '2015-02-28'), (cast(null as date))) as t(date1)";
    testBuilder()
      .sqlQuery("select to_char(date1, 'YYYY-MM-DD') res1 from " + dateValues)
      .unOrdered()
      .baselineColumns("res1")
      .baselineValues("1900-01-01")
      .baselineValues("3500-01-01")
      .baselineValues("2000-12-31")
      .baselineValues("2005-12-30")
      .baselineValues("2015-02-28")
      .baselineValues(null)
      .go();
    try {
      testBuilder()
        .sqlQuery("select to_char(date1, 'invalid format') res1 from " + dateValues)
        .expectsEmptyResultSet().go();
      fail("expected exception on invalid date format was not thrown.");
    } catch (RpcException ex) {
      assertTrue("Failed to match exception message", ex.getMessage().contains("Invalid date format string 'invalid format'"));
    }
  }

  @Test
  public void testIsDate() throws Exception {
    testBuilder()
        .sqlQuery("select isDate(date1) res1 from " + dateValues)
        .unOrdered()
        .baselineColumns("res1")
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .build()
        .run();
  }

  @Test
  public void testIsDate2() throws Exception {
    DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD");
    testBuilder()
        .sqlQuery("select case when isdate(date1) then cast(date1 as date) else null end res1 from " + dateValues)
        .unOrdered()
        .baselineColumns("res1")
        .baselineValues(formatter.parseLocalDateTime("1900-01-01"))
        .baselineValues(formatter.parseLocalDateTime("3500-01-01"))
        .baselineValues(formatter.parseLocalDateTime("2000-12-31"))
        .baselineValues(new Object[] {null})
        .baselineValues(new Object[] {null})
        .baselineValues(new Object[] {null})
        .build()
        .run();
  }

  @Test
  public void testUnixTimeStampForDate() throws Exception {
    DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH24:MI:SS");
    date = formatter.parseLocalDateTime("2009-03-20 11:30:01");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    date = formatter.parseLocalDateTime("2014-08-09 05:15:06");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('2014-08-09 05:15:06') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    date = formatter.parseLocalDateTime("1970-01-01 00:00:00");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('1970-01-01 00:00:00') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    // make sure we support 24 hour notation by default
    date = formatter.parseLocalDateTime("1970-01-01 23:12:12");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;
    testBuilder()
      .sqlQuery("select unix_timestamp('1970-01-01 23:12:12') from cp.`employee.json` limit 1")
      .ordered()
      .baselineColumns("EXPR$0")
      .baselineValues(unixTimeStamp)
      .build().run();
  }

  @Test
  public void testUnixTimeStampForDateWithPattern() throws Exception {
    DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH:MI:SS.FFF");
    date = formatter.parseLocalDateTime("2009-03-20 11:30:01.0");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;

    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01.0', 'YYYY-MM-DD HH:MI:SS.FFF') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD");
    date = formatter.parseLocalDateTime("2009-03-20");
    unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date) / 1000;

    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20', 'YYYY-MM-DD') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();
  }

  @Test
  public void testCurrentDate() throws Exception {
    testBuilder()
        .sqlQuery("select (extract(hour from current_date) = 0) as col from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testLocalTimestamp() throws Exception {
    testBuilder()
        .sqlQuery("select extract(day from localtimestamp) = extract(day from current_date) as col from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testBigIntToDateTimeTimeStamp() throws Exception {
    testBuilder()
        .sqlQuery("SELECT " +
            "  to_date(683) c11 " +
            ", to_date(1477440000) c12" +
            ", to_date(1477440683) c13" +
            ", to_time(683) c21" +
            ", to_time(1477440000) c22" +
            ", to_time(1477440683) c23" +
            ", to_timestamp(683) c31" +
            ", to_timestamp(1477440000) c32" +
            ", to_timestamp(1477440683) c33" +
            " FROM sys.version"
        ).unOrdered()
        .baselineColumns(
            "c11",
            "c12",
            "c13",
            "c21",
            "c22",
            "c23",
            "c31",
            "c32",
            "c33"
        ).baselineValues(
            newDateTime(0), // c11
            newDateTime(1477440000000L), // c12
            newDateTime(1477440000000L), // c13
            newDateTime(683000), // c21
            newDateTime(0), // c22
            newDateTime(683000), // c23
            newDateTime(683000), // c31
            newDateTime(1477440000000L), // c32
            newDateTime(1477440683000L) // c33
        ).go();
  }

  @Test
  public void testQuarter() throws Exception {
    testBuilder()
        .sqlQuery("select `quarter`(d) res1 from (VALUES({d '2017-01-02'}),({d '2000-05-07'}),(CAST(NULL as DATE))) tbl(d)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1L)
        .baselineValues(2L)
        .baselineValues((Long) null)
        .go();
  }

  @Test
  public void testYear() throws Exception {
    testBuilder()
        .sqlQuery("select `year`(d) res1 from (VALUES({d '2017-01-02'}),({d '2000-05-07'}),(CAST(NULL as DATE))) tbl(d)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(2017L)
        .baselineValues(2000L)
        .baselineValues((Long) null)
        .go();
  }

  @Test
  public void testCastTimestampToDate() throws Exception {
    String[] tzs = {"America/Los_Angeles", "UTC", "Europe/Paris", "Pacific/Auckland", "Australia/Melbourne"};
    for (String tz : tzs) {
      DateTimeZone.setDefault(DateTimeZone.forID(tz));
      testBuilder()
        .sqlQuery("SELECT CAST(`datetime0` AS DATE) res1 FROM (VALUES({ts '2016-01-02 00:46:36'})) tbl(datetime0)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(new LocalDateTime(2016, 1, 2, 0, 0))
        .go();
    }
  }

  @Test
  public void testDateMinusDate() throws Exception {
    testBuilder()
        .sqlQuery("SELECT (CAST(`datetime0` AS DATE) - CAST({d '2004-01-01'} AS DATE)) res1 FROM (VALUES({ts '2017-01-13 01:02:03'})) tbl(datetime0)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(Period.days(4761))
        .go();
  }

  @Test
  public void testDateMinusDateAsInteger() throws Exception {
    testBuilder()
        .sqlQuery("SELECT CAST((CAST(`datetime0` AS DATE) - CAST({d '2004-01-01'} AS DATE)) AS INTEGER) res1 FROM (VALUES({ts '2017-01-13 01:02:03'})) tbl(datetime0)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(4761)
        .go();
  }

  @Test
  public void testDateMinusInterval() throws Exception {
    testBuilder()
        .sqlQuery("SELECT ({d '2017-01-13'} - CAST(CAST(a AS VARCHAR) AS INTERVAL DAY)) res1 FROM (VALUES('P1D'),('P2D')) t(a)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(newDateTime(1484179200000L))
        .baselineValues(newDateTime(1484092800000L))
        .go();
  }

  @Test
  public void testDatePlusInterval() throws Exception {
    testBuilder()
        .sqlQuery("SELECT ({d '2017-01-13'} + CAST(CAST(a AS VARCHAR) AS INTERVAL DAY)) res1 FROM (VALUES('P1D'),('P2D')) t(a)")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(newDateTime(1484352000000L))
        .baselineValues(newDateTime(1484438400000L))
        .go();
  }

  @Test
  public void testToDateTypeWithReplaceOptions() throws Exception {
    testBuilder()
      .sqlQuery("SELECT " +
        " to_time('13:44:33', 'hh24:mi:ss', 1) b1," +
        " to_time('13a:44:33', 'hh24:mi:ss', 1) b2," +
        " to_date('2016-10-26', 'YYYY-MM-DD', 1) b3," +
        " to_date('2016-10-26a', 'YYYY-MM-DD', 1) b4," +
        " to_timestamp('1970-01-01 21:44:33', 'YYYY-MM-DD hh24:mi:ss', 1) b5," +
        " to_timestamp('1970-a01-01 21:44:33', 'YYYY-MM-DD hh24:mi:ss', 1) b6" +
        " FROM sys.version"
      ).ordered()
      .baselineColumns("b1", "b2", "b3", "b4", "b5", "b6")
      .baselineValues(
        newDateTime(49473000),
        null,
        newDateTime(1477440000000L),
        null,
        newDateTime(78273000),
        null
    ).go();

    try {
      testBuilder()
        .sqlQuery("SELECT to_time('13:44:3a3', 'hh24:mi:ss', 0) FROM sys.version")
        .expectsEmptyResultSet().go();
      fail("expected exception on invalid date format was not thrown.");
    } catch (RpcException ex) {
      assertTrue("Failed to match exception message", ex.getMessage().contains("Input text cannot be formatted to date"));
    }
  }

  @Test
  public void testCastVarCharToInterval() throws Exception {
    testBuilder()
        .sqlQuery("SELECT " +
            "cast(cast('PT2095081.905S' as varchar(256)) as interval second) c1, " +
            "cast(cast('PT2095082.905S' as varchar(256)) as interval second) c2, " +
            "cast(cast('PT2196778.608S' as varchar(256)) as interval second) c3, " +
            "cast(cast('P1DT2108681.905S' as varchar(256)) as interval second) c4, " +
            "cast(cast('P25DT25H2778.608S' as varchar(256)) as interval second) c5, " +
            "cast(cast('P25DT25H234M2778.608S' as varchar(256)) as interval second) c6, " +
            "cast(cast('P200DT25H234M782778.608S' as varchar(256)) as interval second) c7, " +
            "cast(cast('PT-2095081.905S' as varchar(256)) as interval second) c8, " +
            "cast(cast('PT-2095082.905S' as varchar(256)) as interval second) c9, " +
            "cast(cast('PT-2196778.608S' as varchar(256)) as interval second) c10, " +
            "cast(cast('P1DT-2108681.905S' as varchar(256)) as interval second) c11, " +
            "cast(cast('P25DT-25H2778.608S' as varchar(256)) as interval second) c12, " +
            "cast(cast('P-25DT25H234M2778.608S' as varchar(256)) as interval second) c13, " +
            "cast(cast('P200DT-25H234M782778.608S' as varchar(256)) as interval second) c14 " +
            "FROM (values(1))"
        ).unOrdered()
        .baselineColumns("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14")
        .baselineValues(
            newPeriod(24, 21481905), // c1
            newPeriod(24, 21482905), // c2
            newPeriod(25, 36778608), // c3
            newPeriod(25, 35081905), // c4
            newPeriod(26, 6378608), // c5
            newPeriod(26, 20418608), // c6
            newPeriod(210, 22818608), // c7
            newPeriod(-24, -21481905), // c8
            newPeriod(-24, -21482905), // c9
            newPeriod(-25, -36778608), // c10
            newPeriod(-23, -35081905), // c11
            newPeriod(24, -821392), // c12
            newPeriod(-24, 20418608), // c13
            newPeriod(208, 15618608) // c14
        )
        .go();
  }

  @Test
  public void testDateAddFunction() throws Exception {
    testBuilder()
      .sqlQuery("SELECT " +
        " date_add('2008-03-15', INTERVAL '10' DAY) AS VAL1," +
        " date_add('2008-03-15', INTERVAL '20' DAY) AS VAL2," +
        " date_add('2008-03-15', INTERVAL '20' MINUTE) AS VAL3," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '20' MINUTE) AS VAL4," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '2' HOUR) AS VAL5," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '40' SECOND) AS VAL6," +
        " date_add('2008-03-15', INTERVAL '3' YEAR) AS VAL7," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '3' YEAR) AS VAL8," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '3-2' YEAR TO MONTH) AS VAL9," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '2-6' YEAR TO MONTH) AS VAL10," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '10:22' MINUTE TO SECOND) AS VAL11," +
        " date_add(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '4 5:12:10' DAY TO SECOND) AS VAL12" +
        " FROM sys.version"
      ).unOrdered()
      .baselineColumns("VAL1", "VAL2", "VAL3", "VAL4", "VAL5", "VAL6", "VAL7", "VAL8", "VAL9",
        "VAL10", "VAL11", "VAL12")
      .baselineValues(
        formatTimeStampMilli.parseLocalDateTime("2008-03-25 00:00:00.000"), // VAL1
        formatTimeStampMilli.parseLocalDateTime("2008-04-04 00:00:00.000"), // VAL2
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 00:20:00.000"), // VAL3
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:54:21.000"), // VAL4
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 11:34:21.000"), // VAL5
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:35:01.000"), // VAL6
        formatTimeStampMilli.parseLocalDateTime("2011-03-15 00:00:00.000"), // VAL7
        formatTimeStampMilli.parseLocalDateTime("2011-03-15 09:34:21.000"), // VAL8
        formatTimeStampMilli.parseLocalDateTime("2011-05-15 09:34:21.000"), // VAL9
        formatTimeStampMilli.parseLocalDateTime("2010-09-15 09:34:21.000"), // VAL10
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:44:43.000"), // VAL11
        formatTimeStampMilli.parseLocalDateTime("2008-03-19 14:46:31.000")  // VAL12
        )
      .go();
  }

  @Test
  public void testDateSubFunction() throws Exception {
    testBuilder()
      .sqlQuery("SELECT " +
        " date_sub('2008-03-15', INTERVAL '10' DAY) AS VAL1," +
        " date_sub('2008-03-15', INTERVAL '20' DAY) AS VAL2," +
        " date_sub('2008-03-15', INTERVAL '20' MINUTE) AS VAL3," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '20' MINUTE) AS VAL4," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '2' HOUR) AS VAL5," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '40' SECOND) AS VAL6," +
        " date_sub('2008-03-15', INTERVAL '3' YEAR) AS VAL7," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '3' YEAR) AS VAL8," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '3-2' YEAR TO MONTH) AS VAL9," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '2-6' YEAR TO MONTH) AS VAL10," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '10:22' MINUTE TO SECOND) AS VAL11," +
        " date_sub(TIMESTAMP '2008-03-15 09:34:21', INTERVAL '4 5:12:10' DAY TO SECOND) AS VAL12" +
        " FROM sys.version"
      ).unOrdered()
      .baselineColumns("VAL1", "VAL2", "VAL3", "VAL4", "VAL5", "VAL6", "VAL7", "VAL8", "VAL9",
        "VAL10", "VAL11", "VAL12")
      .baselineValues(
        formatTimeStampMilli.parseLocalDateTime("2008-03-05 00:00:00.000"), // VAL1
        formatTimeStampMilli.parseLocalDateTime("2008-02-24 00:00:00.000"), // VAL2
        formatTimeStampMilli.parseLocalDateTime("2008-03-14 23:40:00.000"), // VAL3
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:14:21.000"), // VAL4
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 07:34:21.000"), // VAL5
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:33:41.000"), // VAL6
        formatTimeStampMilli.parseLocalDateTime("2005-03-15 00:00:00.000"), // VAL7
        formatTimeStampMilli.parseLocalDateTime("2005-03-15 09:34:21.000"), // VAL8
        formatTimeStampMilli.parseLocalDateTime("2005-01-15 09:34:21.000"), // VAL9
        formatTimeStampMilli.parseLocalDateTime("2005-09-15 09:34:21.000"), // VAL10
        formatTimeStampMilli.parseLocalDateTime("2008-03-15 09:23:59.000"), // VAL11
        formatTimeStampMilli.parseLocalDateTime("2008-03-11 04:22:11.000")  // VAL12
      )
      .go();
  }

  @Test
  public void datePart() throws Exception {
    final String query = "SELECT " +
        "date_part('minute', interval '1 2:30:45.100' day to second) e1," +
        "date_part('hour', interval '1 2:30:45.100' day to second) e2," +
        "date_part('day', interval '1 2:30:45.100' day to second) e3," +
        "date_part('year', interval '2-5' YEAR TO MONTH) e4, " +
        "date_part('month', interval '1-4' YEAR TO MONTH) e5, " +
        "date_part('year', interval '45' YEAR) e6, " +
        "date_part('month', interval '45' YEAR) e7 " +
        "from sys.version";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("e1", "e2", "e3", "e4", "e5", "e6", "e7")
        .baselineValues(30L, 2L, 1L, 2L, 4L, 45L, 0L)
        .go();
  }

  private static Period newPeriod(int days, int millis) {
    return new Period(0, 0, 0, days, 0, 0, 0, millis);
  }

  private static LocalDateTime newDateTime(long instant) {
    // simulating the behavior of {Time/Date/TimeStamp}Vector.getAccessor().getObject() behavior which constructs
    // UTC zone DateTime and converts it into local timezone.
    return new LocalDateTime(instant, DateTimeZone.UTC);
  }
}
