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
package com.dremio.exec.physical.impl;

import static com.dremio.sabot.Fixtures.date;
import static com.dremio.sabot.Fixtures.time;
import static com.dremio.sabot.Fixtures.ts;

import org.joda.time.LocalDateTime;
import org.junit.Test;

import com.dremio.sabot.BaseTestFunction;

/* This class tests the existing date types. Simply using date types
 * by casting from VarChar, performing basic functions and converting
 * back to VarChar.
 */
public class TestExtractFunctions extends BaseTestFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExtractFunctions.class);

  @Test
  public void testFromTimeStamp() throws Exception {
    testFunctions(new Object[][]{
      {"extractMinute(c0)", ts("1970-01-02T10:20:33"), 20L},
      {"extractMinute(c0)", ts("2008-12-28T11:34:00.129"), 34L},
      {"extractMinute(c0)", ts("2000-2-27T14:24:00"), 24L},
      {"extractHour(c0)", ts("1970-01-02T10:20:33"), 10L},
      {"extractHour(c0)", ts("2008-12-28T11:34:00.129"), 11L},
      {"extractHour(c0)", ts("2000-2-27T14:24:00"), 14L},
      {"extractDay(c0)", ts("1970-01-02T10:20:33"), 2L},
      {"extractDay(c0)", ts("2008-12-28T11:34:00.129"), 28L},
      {"extractDay(c0)", ts("2000-2-27T14:24:00"), 27L},
      {"extractMonth(c0)", ts("1970-01-02T10:20:33"), 1L},
      {"extractMonth(c0)", ts("2008-12-28T11:34:00.129"), 12L},
      {"extractMonth(c0)", ts("2000-2-27T14:24:00"), 2L},
      {"extractYear(c0)", ts("1970-01-02T10:20:33"), 1970L},
      {"extractYear(c0)", ts("2008-12-28T11:34:00.129"), 2008L},
      {"extractYear(c0)", ts("2000-2-27T14:24:00"), 2000L},
    });

  }

  @Test
  public void testFromDate() throws Exception {
    testFunctions(new Object[][]{
      {"extractDay(c0)", date("1970-01-02"), 2L},
      {"extractDay(c0)", date("2008-12-28"), 28L},
      {"extractDay(c0)", date("2000-2-27"), 27L},
      {"extractMonth(c0)", date("1970-01-02"), 1L},
      {"extractMonth(c0)", date("2008-12-28"), 12L},
      {"extractMonth(c0)", date("2000-2-27"), 2L},
      {"extractYear(c0)", date("1970-01-02"), 1970L},
      {"extractYear(c0)", date("2008-12-28"), 2008L},
      {"extractYear(c0)", date("2000-2-27"), 2000L},
    });

  }

  @Test
  public void testFromTime() throws Exception {
    testFunctions(new Object[][]{
      {"extractMinute(c0)", time("10:20:33"), 20L},
      {"extractMinute(c0)", time("11:34:00"), 34L},
      {"extractMinute(c0)", time("14:24:00"), 24L},
      {"extractHour(c0)", time("10:20:33"), 10L},
      {"extractHour(c0)", time("11:34:00"), 11L},
      {"extractHour(c0)", time("14:24:00"), 14L},
    });

  }

  @Test
  public void testTimestampDiff() throws Exception {
    LocalDateTime startDt = ts("2015-09-10T20:49:42.000");
    LocalDateTime endDt = ts("2017-03-30T22:50:59.050");
    testFunctions(new Object[][]{
      {"timestampdiffSecond(c0, c1)", startDt, endDt, 48996077},
      {"timestampdiffMinute(c0, c1)", startDt, endDt, 816601},
      {"timestampdiffHour(c0, c1)", startDt, endDt, 13610},
      {"timestampdiffDay(c0, c1)", startDt, endDt, 567},
      {"timestampdiffWeek(c0, c1)", startDt, endDt, 81},
      {"timestampdiffMonth(c0, c1)", startDt, endDt, 18},
      {"timestampdiffQuarter(c0, c1)", startDt, endDt, 6},
      {"timestampdiffYear(c0, c1)", startDt, endDt, 1}
      });
  }

  @Test
  public void testTimestampDiffReverse() throws Exception {
    LocalDateTime endDt = ts("2015-09-10T20:49:42.000");
    LocalDateTime startDt = ts("2017-03-30T22:50:59.050");
    testFunctions(new Object[][]{
      {"timestampdiffSecond(c0, c1)", startDt, endDt, -48996077},
      {"timestampdiffMinute(c0, c1)", startDt, endDt, -816601},
      {"timestampdiffHour(c0, c1)", startDt, endDt, -13610},
      {"timestampdiffDay(c0, c1)", startDt, endDt, -567},
      {"timestampdiffWeek(c0, c1)", startDt, endDt, -81},
      {"timestampdiffMonth(c0, c1)", startDt, endDt, -18},
      {"timestampdiffQuarter(c0, c1)", startDt, endDt, -6},
      {"timestampdiffYear(c0, c1)", startDt, endDt, -1}
    });
  }

  @Test
  public void testTimestampDiffZero() throws Exception {
    LocalDateTime startDt = ts("2015-09-10T20:49:42.000");
    LocalDateTime endDt = ts("2015-09-10T20:49:42.999");
    testFunctions(new Object[][]{
      {"timestampdiffSecond(c0, c1)", startDt, endDt, 0},
      {"timestampdiffMinute(c0, c1)", startDt, endDt, 0},
      {"timestampdiffHour(c0, c1)", startDt, endDt, 0},
      {"timestampdiffDay(c0, c1)", startDt, endDt, 0},
      {"timestampdiffWeek(c0, c1)", startDt, endDt, 0},
      {"timestampdiffMonth(c0, c1)", startDt, endDt, 0},
      {"timestampdiffQuarter(c0, c1)", startDt, endDt, 0},
      {"timestampdiffYear(c0, c1)", startDt, endDt, 0}
    });
  }


//
//  @Test
//  public void testFromIntervalDay() throws Exception {
//
//    long expectedValues[][] = {
//      {20, 01, 01, 00, 00},
//      {00, 00, 00, 00, 00},
//      {20, 01, 00, 00, 00},
//      {20, 01, 01, 00, 00},
//      {00, 00, 00, 00, 00},
//      {-39, 00, 01, 00, 00}
//    };
//    testFrom("intervalday", "/test_simple_interval.json", "stringinterval", expectedValues);
//  }
//
//  @Test
//  public void testFromIntervalYear() throws Exception {
//    long expectedValues[][] = {
//      {00, 00, 00, 02, 02},
//      {00, 00, 00, 02, 02},
//      {00, 00, 00, 00, 00},
//      {00, 00, 00, 02, 02},
//      {00, 00, 00, 00, 00},
//      {00, 00, 00, 10, 01}
//    };
//    testFrom("intervalyear", "/test_simple_interval.json", "stringinterval", expectedValues);
//  }
//
//  private void testFrom(String fromType, String testDataFile, String columnName,
//      long expectedValues[][]) throws Exception {
//    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
//         SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, serviceSet, CLASSPATH_SCAN_RESULT);
//         DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, serviceSet.getCoordinator())) {
//
//      // run query.
//      bit.run();
//      client.connect();
//      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
//        Files.toString(FileUtils.getResourceAsFile("/functions/extractFrom.json"), Charsets.UTF_8)
//        .replace("#{TEST_TYPE}", fromType)
//        .replace("#{TEST_FILE}", testDataFile)
//        .replace("#{COLUMN_NAME}", columnName));
//
//      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
//
//      QueryDataBatch batch = results.get(0);
//      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
//
//      for(int i=0; i<expectedValues.length; i++) {
//        for(int j=0; j<expectedValues[i].length; j++) {
//          NullableBigIntVector vv =
//              (NullableBigIntVector) batchLoader.getValueAccessorById(NullableBigIntVector.class, j).getValueVector();
//          assertEquals("["+i+"]["+j+"]: Expected: " + expectedValues[i][j] + ", Actual: " + vv.getAccessor().get(i),
//              expectedValues[i][j], vv.getAccessor().get(i));
//        }
//      }
//
//      for(QueryDataBatch b : results){
//        b.release();
//      }
//      batchLoader.clear();
//    }
//  }
}
