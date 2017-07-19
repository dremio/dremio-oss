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

  private static LocalDateTime newDateTime(long instant) {
    // simulating the behavior of {Time/Date/TimeStamp}Vector.getAccessor().getObject() behavior which constructs
    // UTC zone DateTime and converts it into local timezone.
    return new LocalDateTime(instant, DateTimeZone.UTC);
  }
}
