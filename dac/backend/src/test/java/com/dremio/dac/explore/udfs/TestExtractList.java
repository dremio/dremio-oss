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
package com.dremio.dac.explore.udfs;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;

/**
 * Tests for UDFs in {@link ExtractList}
 */
public class TestExtractList extends BaseTestQuery {

  @Test
  public void arrayLength() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " +
            "array_length(intList) l1," +
            "array_length(bitList) l2," +
            "array_length(mapList) l3," +
            "array_length(listList) l4," +
            "array_length(listList[0]) l5 " +
            "FROM cp.\"testfiles/array_length.json\"")
        .baselineColumns("l1", "l2", "l3", "l4", "l5")
        .baselineValues(3, 1, 2, 2, 2)
        .baselineValues(0, 0, 0, 0, 0)
        .go();
  }

  @Test
  public void arrayLengthInvalidInput() throws Exception {
    errorMsgTestHelper("SELECT array_length(intList[0]) FROM cp.\"testfiles/array_length.json\"",
        "'array_length' is supported only on LIST type input. Given input type : BIGINT");
  }

  @Test
  public void sublistLongType() throws Exception {
    /**
     * Test data
     * row1: [11, 12, 13, 20, 21, 23, 53, 124323]
     * row2: [11, 234, 345]
     * row3: [11]
     * row4: null
     */

    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " +
            "sublist(intType, 0, 3) as r1, " +
            "sublist(intType, 1, 2) as r2, " +
            "sublist(intType, 3, 3) as r3, " +
            "sublist(intType, 3, 6) as r4, " +
            "sublist(intType, 5, 200) as r5, " +
            "sublist(intType, -1, 2) as r6, " +
            "sublist(intType, -2, 20) as r7, " +
            "sublist(intType, -8, 3) as r8, " +
            "sublist(intType, -20, 3) as r9, " +
            "sublist(intType, 1, -3) as r10 " +
            " FROM cp.\"testfiles/sublist.json\""
        ).baselineColumns("r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10"
        ).baselineValues(
            listOf(11L, 12L, 13L), // r1
            listOf(11L, 12L), // r2
            listOf(13L, 20L, 21L), // r3
            listOf(13L, 20L, 21L, 23L, 53L, 124323L), // r4
            listOf(21L, 23L, 53L, 124323L), // r5
            listOf(124323L), // r6
            listOf(53L, 124323L), // r7
            listOf(11L, 12L, 13L), // r8
            null, // r9
            null // r10
        ).baselineValues(
            listOf(11L, 234L, 345L), // r1
            listOf(11L, 234L), // r2
            listOf(345L), // r3
            listOf(345L), // r4
            null, // r5
            listOf(345L), // r6
            listOf(234L, 345L), // r7
            null, // r8
            null, // r9
            null // r10
        ).baselineValues(
            listOf(11L), // r1
            listOf(11L), // r2
            null, // r3
            null, // r4
            null, // r5
            listOf(11L), // r6
            null, // r7
            null, // r8
            null, // r9
            null // r10
        ).baselineValues(
            null, // r1
            null, // r2
            null, // r3
            null, // r4
            null, // r5
            null, // r6
            null, // r7
            null, // r8
            null, // r9
            null // r10
        ).go();
  }

  @Test
  public void sublistListType() throws Exception {
    /**
     * Test data
     * row1: [["21", "22", "23"], ["24", "25", "26", "27"]]
     * row2: [["21"], ["24"]]
     * row3: [["24"]]
     * row4: null
     */

    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " +
            "sublist(listType, 0, 3) as r1, " +
            "sublist(listType, 1, 2) as r2, " +
            "sublist(listType, 2, 3) as r3, " +
            "sublist(listType, 3, 6) as r4, " +
            "sublist(listType, -1, 2) as r5, " +
            "sublist(listType, -10, 20) as r6 " +
            " FROM cp.\"testfiles/sublist.json\""
        ).baselineColumns("r1", "r2", "r3", "r4", "r5", "r6"
        ).baselineValues(
            listOf(listOf("21", "22", "23"), listOf("24", "25", "26", "27")), // r1
            listOf(listOf("21", "22", "23"), listOf("24", "25", "26", "27")), // r2
            listOf(listOf("24", "25", "26", "27")), // r3
            null, // r4
            listOf(listOf("24", "25", "26", "27")), // r5
            null // r6
        ).baselineValues(
            listOf(listOf("21"), listOf("24")), // r1
            listOf(listOf("21"), listOf("24")), // r2
            listOf(listOf("24")), // r3
            null, // r4
            listOf(listOf("24")), // r5
            null // r6
        ).baselineValues(
            listOf(listOf("24")), // r1
            listOf(listOf("24")), // r2
            null, // r3
            null, // r4
            listOf(listOf("24")), // r5
            null // r6
        ).baselineValues(
            null, // r1
            null, // r2
            null, // r3
            null, // r4
            null, // r5
            null // r6
        ).go();
  }

  @Test
  public void sublistNestedListVarCharType() throws Exception {
    /**
     * Test data
     * row1: ["24", "25", "26", "27"]
     * row2: ["24"]
     * row3: null
     * row4: null
     */

    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " +
            "sublist(listType[1], 0, 3) as r1, " +
            "sublist(listType[1], 1, 2) as r2, " +
            "sublist(listType[1], 2, 3) as r3, " +
            "sublist(listType[1], 3, 6) as r4, " +
            "sublist(listType[1], -1, 2) as r5, " +
            "sublist(listType[1], -10, 20) as r6 " +
            " FROM cp.\"testfiles/sublist.json\""
        ).baselineColumns("r1", "r2", "r3", "r4", "r5", "r6"
    ).baselineValues(
        listOf("24", "25", "26"), // r1
        listOf("24", "25"), // r2
        listOf("25", "26", "27"), // r3
        listOf("26", "27"), // r4
        listOf("27"), // r5
        null // r6
    ).baselineValues(
        listOf("24"), // r1
        listOf("24"), // r2
        null, // r3
        null, // r4
        listOf("24"), // r5
        null // r6
    ).baselineValues(
        null, // r1
        null, // r2
        null, // r3
        null, // r4
        null, // r5
        null // r6
    ).baselineValues(
        null, // r1
        null, // r2
        null, // r3
        null, // r4
        null, // r5
        null // r6
    ).go();
  }


  @Ignore("DX-3990")
  @Test
  public void subListListMap1() throws Exception {
    /**
     * Test data:
     *   row1: [ { "a" : 2, "b" : "3" }, { "a": 4, "b": "5" }, { "a": 6, "b": "7"}]
     *   row2: [ { "a" : 8, "b" : "9" }, { "a": 10, "b": "11" }]
     *   row3: [ { "a" : 8, "b" : "9" }]
     *   row4: null
     */
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " +
            "sublist(mapType, 0, 3) as r1, " +
            "sublist(mapType, 1, 2) as r2, " +
            "sublist(mapType, 2, 3) as r3, " +
            "sublist(mapType, 3, 6) as r4, " +
            "sublist(mapType, -1, 2) as r5, " +
            "sublist(mapType, -10, 20) as r6 " +
            " FROM cp.\"testfiles/sublist.json\""
        ).baselineColumns("r1", "r2", "r3", "r4", "r5", "r6"
    ).baselineValues(
        listOf(mapOf("a", 2L, "b", "3"), mapOf("a", 4L, "b", "5"), mapOf("a", 6L, "b", "7")), // r1
        listOf(mapOf("a", 2L, "b", "3"), mapOf("a", 4L, "b", "5")), // r2
        listOf(mapOf("a", 4L, "b", "5"), mapOf("a", 6L, "b", "7")), // r3
        listOf(mapOf("a", 6L, "b", "7")), // r4
        listOf(mapOf("a", 6L, "b", "7")), // r5
        null // r6
    ).baselineValues(
        listOf(mapOf("a", 8L, "b", "9"), mapOf("a", 10L, "b", "11")), // r1
        listOf(mapOf("a", 8L, "b", "9"), mapOf("a", 10L, "b", "11")), // r2
        listOf(mapOf("a", 10L, "b", "11")), // r3
        null, // r4
        listOf(mapOf("a", 10L, "b", "11")), // r5
        null // r6
    ).baselineValues(
        listOf(mapOf("a", 8L, "b", "9")), // r1
        listOf(mapOf("a", 8L, "b", "9")), // r2
        null, // r3
        null, // r4
        listOf(mapOf("a", 8L, "b", "9")), // r5
        null // r6
    ).baselineValues(
        null, // r1
        null, // r2
        null, // r3
        null, // r4
        null, // r5
        null // r6
    ).go();
  }

  @Test
  public void testMultiFunctionsWithComplexReturnType() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT nested_1.\"time\" AS time_2, nested_1.\"time\"[0] AS time_1, business_id, type\n" +
        "  FROM (\n" +
        "    SELECT regexp_split(nested_0.\"time\", '\\Q-\\E', 'FIRST', -1) AS \"time\", business_id, type\n" +
        "    FROM (\n" +
        "      SELECT flatten(\"time\") AS \"time\", business_id, type\n" +
        "      FROM cp.\"testfiles/yelp_checkin.json\"\n" +
        "    ) nested_0\n" +
        "  ) nested_1")
      .baselineColumns("time_2", "time_1","business_id", "type")
      .baselineValues(listOf("Fri", "0:2"), "Fri", "7KPBkxAOEtb3QeIL9PEErg", "checkin")
      .baselineValues(listOf("Sat", "0:1"), "Sat", "7KPBkxAOEtb3QeIL9PEErg", "checkin")
      .baselineValues(listOf("Sun", "0:1"), "Sun", "7KPBkxAOEtb3QeIL9PEErg", "checkin")
      .baselineValues(listOf("Mon", "13:1"),"Mon", "kREVIrSBbtqBhIYkTccQUg", "checkin")
      .baselineValues(listOf("Thu", "13:1"),"Thu", "kREVIrSBbtqBhIYkTccQUg", "checkin")
      .baselineValues(listOf("Sat", "16:1"),"Sat", "kREVIrSBbtqBhIYkTccQUg", "checkin")
      .baselineValues(listOf("Thu", "0:1"), "Thu", "tJRDll5yqpZwehenzE2cSg", "checkin")
      .baselineValues(listOf("Mon", "1:1"), "Mon", "tJRDll5yqpZwehenzE2cSg", "checkin")
      .baselineValues(listOf("Mon","12:1"), "Mon", "tJRDll5yqpZwehenzE2cSg", "checkin")
      .baselineValues(listOf("Sat","16:1"), "Sat", "tJRDll5yqpZwehenzE2cSg", "checkin")
      .baselineValues(listOf("Fri", "0:1"), "Fri", "nhZ1HGWD8lMErdn3FuWuTQ", "checkin")
      .baselineValues(listOf("Sat", "0:1"), "Sat", "nhZ1HGWD8lMErdn3FuWuTQ", "checkin")
      .baselineValues(listOf("Sun", "0:1"), "Sun", "nhZ1HGWD8lMErdn3FuWuTQ", "checkin")
      .go();
  }
}
