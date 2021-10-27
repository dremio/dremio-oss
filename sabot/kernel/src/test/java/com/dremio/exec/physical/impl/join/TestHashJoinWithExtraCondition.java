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
package com.dremio.exec.physical.impl.join;

import org.junit.Test;

import com.dremio.PlanTestBase;

/**
 * Queries that test hash join with additional inequality expressions.
 */
public class TestHashJoinWithExtraCondition extends PlanTestBase {

  private static final String NATION = "dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/nation\"";
  private static final String REGION = "dfs.\"${WORKING_PATH}/src/test/resources/tpchmulti/region\"";

  @Test
  public void testInnerJoin() throws Exception {
    String sql = String.format("SELECT count(*) as cnt FROM\n"
      + "%s nations \n"
      + "INNER JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + "  AND (2 > (CASE length(n_comment) / length(r_comment) when 0 then 0 when 1 then 1 when 2 then 2 else 3 end))"
      + " group by regions.R_NAME", NATION, REGION);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(2L)
      .baselineValues(5L)
      .baselineValues(4L)
      .baselineValues(5L)
      .go();
    testPlanMatchingPatterns(sql, new String[]{"HashJoin"});
  }

  @Test
  public void testIsNotDistinctLeftJoin() throws Exception {
    String sql = String.format("SELECT convert_from(regions.R_NAME, 'utf8', 'x') as r, count(*) as cnt FROM\n"
      + "%s nations \n"
      + "LEFT JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY is not distinct from regions.R_REGIONKEY \n"
      + "  AND (length(nations.n_comment) > length(regions.r_comment)) \n"
      + " group by regions.R_NAME", NATION, REGION);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("r", "cnt")
      .baselineValues("ASIA", 5L)
      .baselineValues("EUROPE", 4L)
      .baselineValues("AMERICA", 5L)
      .baselineValues(null, 11L)
      .go();
    testPlanMatchingPatterns(sql, new String[]{"HashJoin"});
  }

  @Test
  public void testExpressionHavingKeyInequality() throws Exception {
    String sql = String.format("SELECT convert_from(regions.R_NAME, 'utf8', 'x') as r, count(*) as cnt FROM\n"
      + "%s nations \n"
      + "RIGHT JOIN\n"
      + "%s regions \n"
      + "  on nations.N_REGIONKEY = regions.R_REGIONKEY \n"
      + "  AND ((length(n_comment) < length(r_comment) \n"
      + "  AND nations.N_REGIONKEY > 2) OR (regions.R_REGIONKEY >= nations.N_REGIONKEY AND regions.R_REGIONKEY < 2)) \n"
      + " group by regions.R_NAME \n"
      + "", NATION, REGION);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("r", "cnt")
      .baselineValues("EUROPE", 1L)
      .baselineValues("AMERICA", 5L)
      .baselineValues("MIDDLE EAST", 5L)
      .baselineValues("AFRICA", 5L)
      .baselineValues("ASIA", 1L)
      .go();
    testPlanMatchingPatterns(sql, new String[]{"HashJoin"});
  }

  @Test
  public void testMultiJoinWithComplexExpressionOnLargerDataset() throws Exception {
    String sql = "SELECT count(*) as cnt "
      + "FROM "
      + "cp.\"tpch/orders.parquet\" "
      + "left join cp.\"tpch/lineitem.parquet\"  on o_orderkey = l_orderkey and "
      + "((case when o_totalprice * l_quantity < 10000.00 then 0 "
      + "when o_totalprice * l_quantity < 100000.00 then 1 else 2 end) > 1 "
      + "and regexp_matches(concat(o_comment, l_comment), '^a')) "
      + "full join cp.\"tpch/nation.parquet\" on n_nationkey = mod(O_orderkey, 25) ";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(17787L)
      .go();
    testPlanMatchingPatterns(sql, new String[]{"HashJoin"});
  }
}
