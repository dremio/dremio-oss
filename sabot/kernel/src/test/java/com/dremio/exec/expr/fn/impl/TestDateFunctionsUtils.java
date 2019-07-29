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
package com.dremio.exec.expr.fn.impl;

import static com.dremio.exec.expr.fn.impl.DateFunctionsUtils.formatDate;
import static com.dremio.exec.expr.fn.impl.DateFunctionsUtils.formatTime;
import static com.dremio.exec.expr.fn.impl.DateFunctionsUtils.formatTimeStamp;
import static org.junit.Assert.assertEquals;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.joda.time.DateTimeZone;
import org.junit.Test;

public class TestDateFunctionsUtils {

  @Test
  public void stringToTime() {
    assertEquals(0, formatTime("2016-10-26", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD")));
    assertEquals(calculateTime("1970-01-01 12:11:23"), formatTime("2016-10-26 12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH24:MI:SS")));
    assertEquals(calculateTime("1970-01-01 12:11:23"), formatTime("12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SS")));

    assertEquals(calculateTime("1970-01-01 10:53:00"), formatTime("15:53:00+0500", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SSTZO")));
    assertEquals(calculateTime("1970-01-01 15:53:00"), formatTime("15:53:00", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SS")));
  }

  @Test
  public void stringToDate() {
    assertEquals(calculateTime("2016-10-26 00:00:00"), formatDate("2016-10-26 12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH24:MI:SS")));
    assertEquals(calculateTime("2016-10-26 00:00:00"), formatDate("2016-10-26", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD")));
    assertEquals(0L, formatDate("12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("HH:MI:SS")));
    assertEquals(0L, formatDate("23:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SS")));
    assertEquals(calculateTime("1970-01-02 00:00:00"), formatDate("23:11:23-02:00", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SSTZO")));
  }

  @Test
  public void stringToTimestamp() {
    assertEquals(calculateTime("2016-10-26 00:00:00"), formatTimeStamp("2016-10-26", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD")));
    //works: YYYY-MM-dd hh:mm:ss y-MM-dd HH:mm:ss
    assertEquals(calculateTime("2016-10-26 12:11:23"), formatTimeStamp("2016-10-26 12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH24:MI:SS")));
    assertEquals(calculateTime("1970-01-01 12:11:23"), formatTimeStamp("12:11:23", DateFunctionsUtils.getSQLFormatterForFormatString("HH24:MI:SS")));
  }

  @Test
  public void stringToTimestampChangeTZ() {
    DateTimeZone defaultTZ = DateTimeZone.getDefault();
    try {
      DateTimeZone.setDefault(DateTimeZone.forID("America/Los_Angeles"));
      assertEquals(1236478747000L, formatTimeStamp("2009-03-08 02:19:07", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD HH:MI:SS")));
    } finally {
      DateTimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void stringToTimestampChangeTZOffset() {
    DateTimeZone defaultTZ = DateTimeZone.getDefault();
    try {
      assertEquals(calculateTime("2008-09-15 10:53:00"), formatTimeStamp("2008-09-15T15:53:00+0500", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD\"T\"HH24:MI:SSTZO")));
      assertEquals(calculateTime("2011-01-01 04:00:00"), formatTimeStamp("2010-12-31T23:00:00-0500", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD\"T\"HH24:MI:SSTZO")));
      assertEquals(calculateTime("2010-12-31 23:00:00"), formatTimeStamp("2010-12-31T23:00:00", DateFunctionsUtils.getSQLFormatterForFormatString("YYYY-MM-DD\"T\"HH24:MI:SS")));
    } finally {
      DateTimeZone.setDefault(defaultTZ);
    }
  }

  private long calculateTime(String dateString) {
    long expected = -1;

    try {
      DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      dfm.setTimeZone(TimeZone.getTimeZone("UTC"));
      expected = (dfm.parse(dateString).getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }

    return expected;
  }
}
