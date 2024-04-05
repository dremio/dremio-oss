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
package com.dremio.exec.planner.physical;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for controlling hash join distribution with hints. */
public class TestJoinDistributionHint extends PlanTestBase {
  private static AutoCloseable autoCloseable;

  @BeforeClass
  public static void setup() throws Exception {
    autoCloseable = withOption(ExecConstants.SLICE_TARGET_OPTION, 1);
  }

  @AfterClass
  public static void clean() throws Exception {
    autoCloseable.close();
  }

  /**
   * Baseline test to confirm that the query with no hints behaves in the way we expect. If this
   * fails then subsequent test cases that use hints may not be altering the plan in the way they
   * should.
   *
   * @throws Exception
   */
  @Test
  public void testDefaultBroadcastExchange() throws Exception {
    // When only selecting 1 column from each table, we will default to a broadcast exchange for the
    // join.
    final String query =
        "SELECT o_orderkey FROM cp.\"tpch/orders.parquet\" o \n"
            + "inner JOIN (select l_orderkey from cp.\"tpch/lineitem.parquet\" limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER o_orderkey[\\S ]+INTEGER l_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }

  /**
   * Baseline test to confirm that the query with no hints behaves in the way we expect. If this
   * fails then subsequent test cases that use hints may not be altering the plan in the way they
   * should.
   *
   * @throws Exception
   */
  @Test
  public void testDefaultHashToRandomExchange() throws Exception {
    // When selecting all columns from each table, we will default to a hash exchange for the join.
    final String query =
        "SELECT * FROM cp.\"tpch/orders.parquet\" o \n"
            + "inner JOIN (select * from cp.\"tpch/lineitem.parquet\" limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER o_orderkey[\\S ]+INTEGER l_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }

  /**
   * Use BROADCAST hint to force a broadcast exchange on the right side of a join instead of the
   * default hash exchange.
   *
   * @throws Exception
   */
  @Test
  public void testBroadcastHintOnRight() throws Exception {
    final String query =
        "SELECT * FROM cp.\"tpch/orders.parquet\" o \n"
            + "inner JOIN (select * from cp.\"tpch/lineitem.parquet\" /*+ BROADCAST */ limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER o_orderkey[\\S ]+INTEGER l_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }

  /**
   * Use BROADCAST hint to force a broadcast exchange on the left side of a join instead of the
   * default hash exchange.
   *
   * <p>The resulting plan will have changed the order of the join to accommodate the hint.
   *
   * @throws Exception
   */
  @Test
  public void testBroadcastHintOnLeft() throws Exception {
    final String query =
        "SELECT * FROM cp.\"tpch/orders.parquet\" /*+ BROADCAST */ o \n"
            + "inner JOIN (select * from cp.\"tpch/lineitem.parquet\" limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER l_orderkey[\\S ]+INTEGER o_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER o_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }

  /**
   * Use BROADCAST hint to force broadcast exchange. Since we are applying it to both sides of the
   * join, the order will depend on our costing (in this case, preserving the order in the query).
   *
   * @throws Exception
   */
  @Test
  public void testBroadcastHintOnBoth() throws Exception {
    final String query =
        "SELECT * FROM cp.\"tpch/orders.parquet\" /*+ BROADCAST */ o \n"
            + "inner JOIN (select * from cp.\"tpch/lineitem.parquet\" /*+ BROADCAST */ limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER o_orderkey[\\S ]+INTEGER l_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }

  /**
   * Use BROADCAST hint to force the broadcast exchange to be on the opposite side of the join than
   * what the optimizer would have picked by default.
   *
   * @throws Exception
   */
  @Test
  public void testAlterBroadcastedSide() throws Exception {
    // When only selecting 1 column from each table, we will default to a broadcast exchange for the
    // join.
    // The optimizer normally would broadcast lineitem, but we will force it to broadcast orders.
    final String query =
        "SELECT o_orderkey FROM cp.\"tpch/orders.parquet\" /*+ BROADCAST */ o \n"
            + "inner JOIN (select l_orderkey from cp.\"tpch/lineitem.parquet\" limit 10000) l \n"
            + "ON o.o_orderkey = l.l_orderkey\n";
    final String[] expectedPatterns =
        new String[] {
          "HashJoin[\\S ]+RecordType\\(INTEGER l_orderkey[\\S ]+INTEGER o_orderkey",
          "BroadcastExchange[\\S ]+RecordType\\(INTEGER o_orderkey"
        };
    final String[] excludedPatterns =
        new String[] {
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER o_orderkey",
          "HashToRandomExchange[\\S ]+RecordType\\(INTEGER l_orderkey"
        };
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);
  }
}
