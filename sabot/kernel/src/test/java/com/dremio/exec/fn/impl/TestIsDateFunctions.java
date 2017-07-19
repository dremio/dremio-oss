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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;

/**
 * Tests for IS_DATE, IS_TIMESTAMP, and IS_TIME
 */
public class TestIsDateFunctions extends BaseTestQuery {

  @Test
  public void testIsDate() throws Exception {
    final String query = "SELECT " +
      "IS_DATE(`date`, 'MM.DD.YY') AS f1, " +
      "IS_DATE(`date`, 'MONTH DD, YYYY') AS f2, " +
      "IS_DATE(`date`, 'DY, MONTH DD, YYYY') AS f3, " +
      "IS_DATE(`date`, 'MM.DD.YY, HH24:MI:SS') AS f4 " +
      "FROM cp.`/is_date.json`";
    testBuilder()
      .ordered()
      .sqlQuery(query)
      .baselineColumns("f1", "f2", "f3", "f4")
      .baselineValues(true, false, false, false)
      .baselineValues(false, true, false, false)
      .baselineValues(false, false, true, false)
      .baselineValues(false, false, false, true)
      .baselineValues(false, false, false, false)
      .go();
  }

  @Test
  public void testInvalidIsDatePattern() throws Exception {
    try {
      test("SELECT IS_DATE(`date`, 'yummy') FROM cp.`/is_date.json`");
      fail("Query expected to fail");
    } catch (UserException uex) {
      assertEquals(ErrorType.FUNCTION, uex.getErrorType());
      assertEquals("Failure parsing the formatting string at column 1 of: yummy", uex.getOriginalMessage());
    }
  }

  @Test
  public void testInvalidIsTimePattern() throws Exception {
    try {
      test("SELECT IS_TIME(`time`, 'yummy') FROM cp.`/is_time.json`");
      fail("Query expected to fail");
    } catch (UserException uex) {
      assertEquals(ErrorType.FUNCTION, uex.getErrorType());
      assertEquals("Failure parsing the formatting string at column 1 of: yummy", uex.getOriginalMessage());
    }
  }

  @Test
  public void testInvalidIsTimestampPattern() throws Exception {
    try {
      test("SELECT IS_TIMESTAMP(`time`, 'yummy') FROM cp.`/is_time.json`");
      fail("Query expected to fail");
    } catch (UserException uex) {
      assertEquals(ErrorType.FUNCTION, uex.getErrorType());
      assertEquals("Failure parsing the formatting string at column 1 of: yummy", uex.getOriginalMessage());
    }
  }

  @Test
  public void testIsTime() throws Exception {
    final String query = "SELECT " +
      "IS_TIME(`time`, 'HH:MI:SS') as f1, " +
      "IS_TIME(`time`, 'HH24:MI:SS.FFF') as f2, " +
      "IS_TIME(`time`, 'DD-MON-YY HH12.MI.SS.FFF AM') as f3 " +
      "FROM cp.`/is_time.json`";
    testBuilder()
            .ordered()
            .sqlQuery(query)
            .baselineColumns("f1", "f2", "f3")
            .baselineValues(true, false, false)
            .baselineValues(false, true, false)
            .baselineValues(false, false, true)
            .baselineValues(false, false, false)
            .go();
  }

  @Test
  public void testIsTimestamp() throws Exception {
    final String query = "SELECT " +
            "IS_TIMESTAMP(`time`, 'HH:MI:SS') as f1, " +
            "IS_TIMESTAMP(`time`, 'HH24:MI:SS.FFF') as f2, " +
            "IS_TIMESTAMP(`time`, 'DD-MON-YY HH12.MI.SS.FFF AM') as f3 " +
            "FROM cp.`/is_time.json`";
    testBuilder()
            .ordered()
            .sqlQuery(query)
            .baselineColumns("f1", "f2", "f3")
            .baselineValues(true, false, false)
            .baselineValues(false, true, false)
            .baselineValues(false, false, true)
            .baselineValues(false, false, false)
            .go();
  }
}
