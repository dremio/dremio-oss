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

package com.dremio;

import org.junit.Test;

public class TestNotInQueries extends PlanTestBase {

  @Test
  public void testSimpleNotIn() throws Exception {
    String query = "Select count(*) from cp.\"employees.json\" " +
      "where position_id not in " +
      "(67,940,617,10,111)";

    String expectedFilterCondition =
        "[AND(NOT(OR(=($0, 67), =($0, 940), =($0, 617), =($0, 10), =($0, 111))), IS NOT NULL($0))]";

    testPlanSubstrPatterns(query,
      new String[]{expectedFilterCondition}, new String[]{"<>"});
  }

  @Test
  public void testLongNotIn() throws Exception {
    String query = "Select count(*) from cp.\"employees.json\" " +
      "where position_id not in " +
      "(67,940,617,10,111,12,706,705,65,930,317,39,62,15,19,601,602,7,49,936,612," +
      "924,267,169,64,146,196,9998,942,136,939,52,50,45,946,178,38,8,933,17,61,615," +
      "63,37,611,700,603,96,117,4,5,32,613,133,163,609,86,20,941,2,66,935,934,937," +
      "181,610,11,6,620,923,134,316,59,13,608,607,327,605,948,619,34,9997,51,153,1,180,22,614,944,947,606,604)";

    String expectedFilterCondition =
        "[AND(NOT(OR(=($0, 67), =($0, 940), =($0, 617), =($0, 10), =($0, 111), =($0, 12), =($0, 706), =($0, 705), =($0, 65), =($0, 930), =($0, 317), =($0, 39), =($0, 62), =($0, 15), =($0, 19), =($0, 601), =($0, 602), =($0, 7), =($0, 49), =($0, 936), =($0, 612), =($0, 924), =($0, 267), =($0, 169), =($0, 64), =($0, 146), =($0, 196), =($0, 9998), =($0, 942), =($0, 136), =($0, 939), =($0, 52), =($0, 50), =($0, 45), =($0, 946), =($0, 178), =($0, 38), =($0, 8), =($0, 933), =($0, 17), =($0, 61), =($0, 615), =($0, 63), =($0, 37), =($0, 611), =($0, 700), =($0, 603), =($0, 96), =($0, 117), =($0, 4), =($0, 5), =($0, 32), =($0, 613), =($0, 133), =($0, 163), =($0, 609), =($0, 86), =($0, 20), =($0, 941), =($0, 2), =($0, 66), =($0, 935), =($0, 934), =($0, 937), =($0, 181), =($0, 610), =($0, 11), =($0, 6), =($0, 620), =($0, 923), =($0, 134), =($0, 316), =($0, 59), =($0, 13), =($0, 608), =($0, 607), =($0, 327), =($0, 605), =($0, 948), =($0, 619), =($0, 34), =($0, 9997), =($0, 51), =($0, 153), =($0, 1), =($0, 180), =($0, 22), =($0, 614), =($0, 944), =($0, 947), =($0, 606), =($0, 604))), IS NOT NULL($0))]";

    testPlanSubstrPatterns(query,
      new String[]{expectedFilterCondition}, new String[]{"<>"});
  }

  @Test
  public void testSimpleNotInMixed() throws Exception {
    String query = "Select count(*) from cp.\"employees.json\" " +
      "where position_id > 0 AND position_id not in " +
      "(67,940,617,10,111)";

    String expectedFilterCondition = "[AND(NOT(OR(=($0, 67), =($0, 940), =($0, 617), =($0, 10), =($0, 111))), >($0, 0), IS NOT NULL($0))]";

    testPlanSubstrPatterns(query,
      new String[]{expectedFilterCondition}, new String[]{"<>"});
  }

  @Test
  public void testMultipleNotIn() throws Exception {
    String query = "Select count(*) from cp.\"employees.json\" " +
      "where position_id not in " +
      "(67,940,617,10,111) AND full_name not in ('ab','bc','cd','de','ef')";

    String expectedFilterCondition =
        "[AND(NOT(OR(=($0, 'ab':VARCHAR(2)), =($0, 'bc':VARCHAR(2)), =($0, 'cd':VARCHAR(2)), =($0, 'de':VARCHAR(2)), =($0, 'ef':VARCHAR(2)))), NOT(OR(=($1, 67), =($1, 940), =($1, 617), =($1, 10), =($1, 111))), IS NOT NULL($1), IS NOT NULL($0))]";

    testPlanSubstrPatterns(query,
      new String[]{expectedFilterCondition}, new String[]{"<>"});
  }

  @Test
  public void testMultipleNotInMixed() throws Exception {
    String query = "Select count(*) from cp.\"employees.json\" " +
      "where position_id not in " +
      "(67,940,617,10,111) AND full_name not in ('ab','bc','cd','de','ef') AND position_id > -1";

    String expectedFilterCondition =
        "[AND(NOT(OR(=($0, 'ab':VARCHAR(2)), =($0, 'bc':VARCHAR(2)), =($0, 'cd':VARCHAR(2)), =($0, 'de':VARCHAR(2)), =($0, 'ef':VARCHAR(2)))), NOT(OR(=($1, 67), =($1, 940), =($1, 617), =($1, 10), =($1, 111))), >($1, -1), IS NOT NULL($1), IS NOT NULL($0))]";

    testPlanSubstrPatterns(query,
      new String[]{expectedFilterCondition}, new String[]{"<>"});
  }
}
