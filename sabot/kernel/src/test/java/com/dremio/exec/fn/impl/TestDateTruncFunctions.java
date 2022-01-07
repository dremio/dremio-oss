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

import static com.dremio.common.util.JodaDateUtility.formatDate;
import static com.dremio.common.util.JodaDateUtility.formatTime;
import static com.dremio.common.util.JodaDateUtility.formatTimeStampMilli;

import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestDateTruncFunctions extends BaseTestQuery {

  @Test
  public void dateTruncOnTime() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', time '2:30:21.5') as \"second\", " +
        "date_trunc('MINUTE', time '2:30:21.5') as \"minute\", " +
        "date_trunc('HOUR', time '2:30:21.5') as \"hour\", " +
        "date_trunc('DAY', time '2:30:21.5') as \"day\", " +
        "date_trunc('MONTH', time '2:30:21.5') as \"month\", " +
        "date_trunc('YEAR', time '2:30:21.5') as \"year\", " +
        "date_trunc('QUARTER', time '2:30:21.5') as \"quarter\", " +
        "date_trunc('DECADE', time '2:30:21.5') as \"decade\", " +
        "date_trunc('CENTURY', time '2:30:21.5') as \"century\", " +
        "date_trunc('MILLENNIUM', time '2:30:21.5') as \"millennium\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            formatTime.parseLocalDateTime("2:30:21.0"), // seconds
            formatTime.parseLocalDateTime("2:30:00.0"), // minute
            formatTime.parseLocalDateTime("2:00:00.0"), // hour
            formatTime.parseLocalDateTime("0:00:00.0"), // day
            formatTime.parseLocalDateTime("0:00:00.0"), // month
            formatTime.parseLocalDateTime("0:00:00.0"), // year
            formatTime.parseLocalDateTime("0:00:00.0"), // quarter
            formatTime.parseLocalDateTime("0:00:00.0"), // decade
            formatTime.parseLocalDateTime("0:00:00.0"), // century
            formatTime.parseLocalDateTime("0:00:00.0")) // millennium
        .go();
  }

  @Test
  public void dateTruncOnDateSimpleUnits() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', date '2011-2-3') as \"second\", " +
        "date_trunc('MINUTE', date '2011-2-3') as \"minute\", " +
        "date_trunc('HOUR', date '2011-2-3') as \"hour\", " +
        "date_trunc('DAY', date '2011-2-3') as \"day\", " +
        "date_trunc('WEEK', date '2011-2-3') as \"week\", " +
        "date_trunc('MONTH', date '2011-2-3') as \"month\", " +
        "date_trunc('YEAR', date '2011-2-3') as \"year\", " +
        "date_trunc('QUARTER', date '2011-5-3') as \"q1\", " +
        "date_trunc('QUARTER', date '2011-7-13') as \"q2\", " +
        "date_trunc('QUARTER', date '2011-9-13') as \"q3\", " +
        "date_trunc('DECADE', date '2011-2-3') as \"decade1\", " +
        "date_trunc('DECADE', date '2072-2-3') as \"decade2\", " +
        "date_trunc('DECADE', date '1978-2-3') as \"decade3\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "week" , "year", "q1", "q2", "q3", "decade1", "decade2", "decade3")
        .baselineValues(
            formatDate.parseLocalDateTime("2011-02-03"), // seconds
            formatDate.parseLocalDateTime("2011-02-03"), // minute
            formatDate.parseLocalDateTime("2011-02-03"), // hour
            formatDate.parseLocalDateTime("2011-02-03"), // day
            formatDate.parseLocalDateTime("2011-02-01"), // month
            formatDate.parseLocalDateTime("2011-01-31"), // week
            formatDate.parseLocalDateTime("2011-01-01"), // year
            formatDate.parseLocalDateTime("2011-04-01"), // quarter-1
            formatDate.parseLocalDateTime("2011-07-01"), // quarter-2
            formatDate.parseLocalDateTime("2011-07-01"), // quarter-3
            formatDate.parseLocalDateTime("2010-01-01"), // decade-1
            formatDate.parseLocalDateTime("2070-01-01"), // decade-2
            formatDate.parseLocalDateTime("1970-01-01")) // decade-3
        .go();
  }

  @Test
  public void dateTruncOnDateCentury() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('CENTURY', date '2011-2-3') as c1, " +
        "date_trunc('CENTURY', date '2000-2-3') as c2, " +
        "date_trunc('CENTURY', date '1901-11-3') as c3, " +
        "date_trunc('CENTURY', date '900-2-3') as c4, " +
        "date_trunc('CENTURY', date '0001-1-3') as c5 " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2", "c3", "c4", "c5")
        .baselineValues(
            formatDate.parseLocalDateTime("2001-01-01"), // c1
            formatDate.parseLocalDateTime("1901-01-01"), // c2
            formatDate.parseLocalDateTime("1901-01-01"), // c3
            formatDate.parseLocalDateTime("0801-01-01"), // c4
            formatDate.parseLocalDateTime("0001-01-01")) // c5
        .go();
  }

  @Test
  public void dateTruncOnDateMillennium() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('MILLENNIUM', date '2011-2-3') as \"m1\", " +
        "date_trunc('MILLENNIUM', date '2000-11-3') as \"m2\", " +
        "date_trunc('MILLENNIUM', date '1983-05-18') as \"m3\", " +
        "date_trunc('MILLENNIUM', date '990-11-3') as \"m4\", " +
        "date_trunc('MILLENNIUM', date '0001-11-3') as \"m5\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            formatDate.parseLocalDateTime("2001-01-01"), // m1
            formatDate.parseLocalDateTime("1001-01-01"), // m2
            formatDate.parseLocalDateTime("1001-01-01"), // m3
            formatDate.parseLocalDateTime("0001-01-01"), // m4
            formatDate.parseLocalDateTime("0001-01-01")) // m5
        .go();
  }

  @Test
  public void dateTruncOnTimeStampSimpleUnits() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', timestamp '2011-2-3 10:11:12.100') as \"second\", " +
        "date_trunc('MINUTE', timestamp '2011-2-3 10:11:12.100') as \"minute\", " +
        "date_trunc('HOUR', timestamp '2011-2-3 10:11:12.100') as \"hour\", " +
        "date_trunc('DAY', timestamp '2011-2-3 10:11:12.100') as \"day\", " +
        "date_trunc('WEEK', timestamp '2011-2-3 10:11:12.100') as \"week\", " +
        "date_trunc('MONTH', timestamp '2011-2-3 10:11:12.100') as \"month\", " +
        "date_trunc('YEAR', timestamp '2011-2-3 10:11:12.100') as \"year\", " +
        "date_trunc('QUARTER', timestamp '2011-5-3 10:11:12.100') as \"q1\", " +
        "date_trunc('QUARTER', timestamp '2011-7-13 10:11:12.100') as \"q2\", " +
        "date_trunc('QUARTER', timestamp '2011-9-13 10:11:12.100') as \"q3\", " +
        "date_trunc('DECADE', timestamp '2011-2-3 10:11:12.100') as \"decade1\", " +
        "date_trunc('DECADE', timestamp '2072-2-3 10:11:12.100') as \"decade2\", " +
        "date_trunc('DECADE', timestamp '1978-2-3 10:11:12.100') as \"decade3\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "week" , "year", "q1", "q2", "q3", "decade1", "decade2", "decade3")
        .baselineValues(
            formatTimeStampMilli.parseLocalDateTime("2011-02-03 10:11:12.0"), // seconds
            formatTimeStampMilli.parseLocalDateTime("2011-02-03 10:11:00.0"), // minute
            formatTimeStampMilli.parseLocalDateTime("2011-02-03 10:00:00.0"), // hour
            formatTimeStampMilli.parseLocalDateTime("2011-02-03 00:00:00.0"), // day
            formatTimeStampMilli.parseLocalDateTime("2011-02-01 00:00:00.0"), // month
            formatTimeStampMilli.parseLocalDateTime("2011-01-31 00:00:00.0"), // week
            formatTimeStampMilli.parseLocalDateTime("2011-01-01 00:00:00.0"), // year
            formatTimeStampMilli.parseLocalDateTime("2011-04-01 00:00:00.0"), // quarter-1
            formatTimeStampMilli.parseLocalDateTime("2011-07-01 00:00:00.0"), // quarter-2
            formatTimeStampMilli.parseLocalDateTime("2011-07-01 00:00:00.0"), // quarter-3
            formatTimeStampMilli.parseLocalDateTime("2010-01-01 00:00:00.0"), // decade-1
            formatTimeStampMilli.parseLocalDateTime("2070-01-01 00:00:00.0"), // decade-2
            formatTimeStampMilli.parseLocalDateTime("1970-01-01 00:00:00.0")) // decade-3
        .go();
  }

  @Test
  public void dateTruncOnTimeStampCentury() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('CENTURY', timestamp '2011-2-3 10:11:12.100') as c1, " +
        "date_trunc('CENTURY', timestamp '2000-2-3 10:11:12.100') as c2, " +
        "date_trunc('CENTURY', timestamp '1901-11-3 10:11:12.100') as c3, " +
        "date_trunc('CENTURY', timestamp '900-2-3 10:11:12.100') as c4, " +
        "date_trunc('CENTURY', timestamp '0001-1-3 10:11:12.100') as c5 " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2", "c3", "c4", "c5")
        .baselineValues(
            formatTimeStampMilli.parseLocalDateTime("2001-01-01 00:00:00.0"), // c1
            formatTimeStampMilli.parseLocalDateTime("1901-01-01 00:00:00.0"), // c2
            formatTimeStampMilli.parseLocalDateTime("1901-01-01 00:00:00.0"), // c3
            formatTimeStampMilli.parseLocalDateTime("0801-01-01 00:00:00.0"), // c4
            formatTimeStampMilli.parseLocalDateTime("0001-01-01 00:00:00.0")) // c5
        .go();
  }

  @Test
  public void dateTruncOnTimeStampMillennium() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('MILLENNIUM', timestamp '2011-2-3 10:11:12.100') as \"m1\", " +
        "date_trunc('MILLENNIUM', timestamp '2000-11-3 10:11:12.100') as \"m2\", " +
        "date_trunc('MILLENNIUM', timestamp '1983-05-18 10:11:12.100') as \"m3\", " +
        "date_trunc('MILLENNIUM', timestamp '990-11-3 10:11:12.100') as \"m4\", " +
        "date_trunc('MILLENNIUM', timestamp '0001-11-3 10:11:12.100') as \"m5\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            formatTimeStampMilli.parseLocalDateTime("2001-01-01 00:00:00.0"), // m1
            formatTimeStampMilli.parseLocalDateTime("1001-01-01 00:00:00.0"), // m2
            formatTimeStampMilli.parseLocalDateTime("1001-01-01 00:00:00.0"), // m3
            formatTimeStampMilli.parseLocalDateTime("0001-01-01 00:00:00.0"), // m4
            formatTimeStampMilli.parseLocalDateTime("0001-01-01 00:00:00.0")) // m5
        .go();
  }

  @Test
  public void dateTruncOnIntervalYear() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', interval '217-7' year(3) to month) as \"second\", " +
        "date_trunc('MINUTE', interval '217-7' year(3) to month) as \"minute\", " +
        "date_trunc('HOUR', interval '217-7' year(3) to month) as \"hour\", " +
        "date_trunc('DAY', interval '217-7' year(3) to month) as \"day\", " +
        "date_trunc('MONTH', interval '217-7' year(3) to month) as \"month\", " +
        "date_trunc('YEAR', interval '217-7' year(3) to month) as \"year\", " +
        "date_trunc('QUARTER', interval '217-7' year(3) to month) as \"quarter\", " +
        "date_trunc('DECADE', interval '217-7' year(3) to month) as \"decade\", " +
        "date_trunc('CENTURY', interval '217-7' year(3) to month) as \"century\", " +
        "date_trunc('MILLENNIUM', interval '217-7' year(3) to month) as \"millennium\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            new Period("P217Y7M").normalizedStandard(), // seconds
            new Period("P217Y7M").normalizedStandard(), // minute
            new Period("P217Y7M").normalizedStandard(), // hour
            new Period("P217Y7M").normalizedStandard(), // day
            new Period("P217Y7M").normalizedStandard(), // month
            new Period("P217Y").normalizedStandard(), // year
            new Period("P217Y6M").normalizedStandard(), // quarter
            new Period("P210Y").normalizedStandard(), // decade
            new Period("P200Y").normalizedStandard(), // century
            new Period("PT0S").normalizedStandard()) // millennium
        .go();
  }

  @Test
  public void dateTruncOnIntervalDay() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', interval '200 10:20:30.123' day(3) to second) as \"second\", " +
        "date_trunc('MINUTE', interval '200 10:20:30.123' day(3) to second) as \"minute\", " +
        "date_trunc('HOUR', interval '200 10:20:30.123' day(3) to second) as \"hour\", " +
        "date_trunc('DAY', interval '200 10:20:30.123' day(3) to second) as \"day\", " +
        "date_trunc('MONTH', interval '200 10:20:30.123' day(3) to second) as \"month\", " +
        "date_trunc('YEAR', interval '200 10:20:30.123' day(3) to second) as \"year\", " +
        "date_trunc('QUARTER', interval '200 10:20:30.123' day(3) to second) as \"quarter\", " +
        "date_trunc('DECADE', interval '200 10:20:30.123' day(3) to second) as \"decade\", " +
        "date_trunc('CENTURY', interval '200 10:20:30.123' day(3) to second) as \"century\", " +
        "date_trunc('MILLENNIUM', interval '200 10:20:30.123' day(3) to second) as \"millennium\" " +
        "FROM INFORMATION_SCHEMA.CATALOGS";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            new Period().plusDays(200).plusMillis(37230000), // seconds
            new Period().plusDays(200).plusMillis(37200000), // minute
            new Period().plusDays(200).plusMillis(36000000), // hour
            new Period().plusDays(200), // day
            new Period("PT0S"), // month
            new Period("PT0S"), // year
            new Period("PT0S"), // quarter
            new Period("PT0S"), // decade
            new Period("PT0S"), // century
            new Period("PT0S")) // millennium
        .go();
  }

  @Test
  public void testDateTrunc() throws Exception {
    String query = "select "
        + "date_trunc('MINUTE', time '2:30:21.5') as TIME1, "
        + "date_trunc('SECOND', time '2:30:21.5') as TIME2, "
        + "date_trunc('HOUR', timestamp '1991-05-05 10:11:12.100') as TS1, "
        + "date_trunc('SECOND', timestamp '1991-05-05 10:11:12.100') as TS2, "
        + "date_trunc('MONTH', date '2011-2-2') as DATE1, "
        + "date_trunc('YEAR', date '2011-2-2') as DATE2 "
        + "from cp.\"employee.json\" where employee_id < 2";

    LocalDateTime time1 = formatTime.parseLocalDateTime("2:30:00.0");
    LocalDateTime time2 = formatTime.parseLocalDateTime("2:30:21.0");
    LocalDateTime ts1 = formatTimeStampMilli.parseLocalDateTime("1991-05-05 10:00:00.0");
    LocalDateTime ts2 = formatTimeStampMilli.parseLocalDateTime("1991-05-05 10:11:12.0");
    LocalDateTime date1 = formatDate.parseLocalDateTime("2011-02-01");
    LocalDateTime date2 = formatDate.parseLocalDateTime("2011-01-01");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("TIME1", "TIME2", "TS1", "TS2", "DATE1", "DATE2")
        .baselineValues(time1, time2, ts1, ts2, date1, date2)
        .go();
  }

  @Test // DX-8689
  public void testDateTruncMultipleRows() throws Exception {
    String query = "SELECT date_trunc('QUARTER', dateCol) as res FROM (values" +
        "(date '2017-06-14')," +
        "(date '2017-06-14')," +
        "(date '2000-04-04')," +
        "(date '2005-02-01')," +
        "(date '2015-11-01')," +
        "(cast(null as date))) as t(dateCol)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("res")
        .baselineValues(formatDate.parseLocalDateTime("2017-04-01"))
        .baselineValues(formatDate.parseLocalDateTime("2017-04-01"))
        .baselineValues(formatDate.parseLocalDateTime("2000-04-01"))
        .baselineValues(formatDate.parseLocalDateTime("2005-01-01"))
        .baselineValues(formatDate.parseLocalDateTime("2015-10-01"))
        .baselineValues(new Object[]{null})
        .go();
  }
}
