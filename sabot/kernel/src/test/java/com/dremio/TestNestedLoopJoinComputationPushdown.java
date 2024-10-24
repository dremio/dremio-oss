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

import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestNestedLoopJoinComputationPushdown extends PlanTestBase {

  @Before
  public void before() {
    setSessionOption(PlannerSettings.ENABLE_CROSS_JOIN, true);
  }

  @Test
  @Ignore // DX-35419
  public void alternativeDistance() throws Exception {
    String sql =
        "select count(*) cnt from cp.\"geo/geo.json\" t1\n"
            + "cross join cp.\"geo/geo.json\" t2\n"
            + "where\n"
            + "3958.75 * Atan(Sqrt(1 - power(((Sin(t1.lat / 57.2958) * Sin(t2.lat / 57.2958)) + (Cos(t1.lat / 57.2958) "
            + "* Cos(t2.lat / 57.2958) * Cos((t2.lng / 57.2958) - (t1.lng / 57.2958)))), 2)) / ((Sin(t1.lat / 57.2958)"
            + "* Sin(t2.lat / 57.2958)) + (Cos(t1.lat / 57.2958) * Cos(t2.lat / 57.2958) * Cos((t2.lng / 57.2958) - (t1.lng / 57.2958))))) %s %d\n";
    testPlanMatchingPatterns(
        String.format(sql, "<", 100),
        new String[] {"(?s)NestedLoopJoin.*Project.*COS"},
        "(?s)COS.*NestedLoop",
        "NestedLoop.*COS");

    testBuilder()
        .sqlQuery(String.format(sql, "<", 100))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(13L)
        .go();
  }

  @Test
  @Ignore // DX-35419
  public void correctDistance() throws Exception {
    String sql =
        "select count(t2.lat) cnt from cp.\"geo/geo.json\" t1\n"
            + "cross join cp.\"geo/geo.json\" t2\n"
            + "where\n"
            + "3958.75 * 2 * asin(sqrt(power(sin((t2.lat/57.2958 - t1.lat/57.2958)/2),2) + cos(t1.lat/57.2958)*cos(t2.lat/57.2958)*power(sin(t2.lng/57.2958-t1.lng/57.2958),2))) %s %d\n";
    testPlanMatchingPatterns(
        String.format(sql, "<", 100),
        new String[] {"(?s)NestedLoopJoin.*Project.*COS"},
        "(?s)COS.*NestedLoop",
        "NestedLoop.*COS");

    testBuilder()
        .sqlQuery(String.format(sql, "<", 100))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(11L)
        .go();

    testBuilder()
        .sqlQuery(String.format(sql, "<", 1))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5L)
        .go();

    testBuilder()
        .sqlQuery(String.format(sql, ">", 100))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(14L)
        .go();

    testBuilder()
        .sqlQuery(String.format(sql, ">", 1))
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(20L)
        .go();
  }
}
