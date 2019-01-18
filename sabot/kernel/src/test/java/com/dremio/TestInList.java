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
package com.dremio;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestInList extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestInList.class);

  @Test
  public void testLargeInList1() throws Exception {
    int actualRecordCount = testSql("select employee_id from cp.\"employee.json\" where employee_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 ,14, 15, 16, 17, 18, 19, 20" +
        ", 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40" +
        ", 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60" +
        ")");

    int expectedRecordCount = 59;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }


  @Test
  public void testWithMultipleInLists() throws Exception {
    StringBuilder b = new StringBuilder();
    for (int i = 50; i < 150; i++) {
      b.append(Integer.toString(i));
      b.append(",");
    }
    String dummyValues = b.toString();
    String query = "select n_name from cp.\"tpch/nation.parquet\" where n_nationkey in (" + dummyValues + " 24) and " +
            "n_regionkey in (" + dummyValues + " 1)";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("n_name")
            .baselineValues("UNITED STATES")
            .go();
  }
}
