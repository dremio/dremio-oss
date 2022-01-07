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

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import org.junit.Rule;
import org.junit.Test;

import com.dremio.test.TemporarySystemProperties;

public abstract class TestAbstractCaseNormalQueries extends PlanTestBase {

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testFixedCaseClause() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE n_regionkey \n" +
      "                     WHEN 0 THEN 'AFRICA' \n" +
      "                     WHEN 1 THEN 'AMERICA' \n" +
      "                     WHEN 2 THEN 'ASIA' \n" +
      "                     ELSE NULL \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('ASIA', 'AMERICA')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(10L)
      .go();
  }

  @Test
  public void testFixedCaseEnsureOrder() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE \n" +
      "                     WHEN n_nationkey = 0 THEN 'AFRICA' \n" +
      "                     WHEN n_nationkey >= 0 THEN 'AMERICA' \n" +
      "                     WHEN n_nationkey = 2 THEN 'ASIA' \n" +
      "                     ELSE 'OTHER' \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('AFRICA', 'OTHER', 'ASIA')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(1L)
      .go();
  }

  @Test
  public void testFixedNestedCase() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE n_regionkey \n" +
      "                     WHEN 0 THEN CASE WHEN n_nationkey < 10  THEN 'FIRST AFRICAN' ELSE 'AFRICAN' END \n" +
      "                     WHEN 1 THEN 'AMERICA' \n" +
      "                     WHEN 2 THEN 'ASIA' \n" +
      "                     ELSE CASE WHEN n_nationkey > 5 THEN 'LAST OTHER' " +
      "                               WHEN n_nationkey < 10 THEN 'MIDDLE OTHER' " +
      "                               ELSE 'OTHER' END \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  region IN ('FIRST AFRICAN', 'LAST OTHER', 'MIDDLE OTHER')";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(12L)
      .go();
  }

  @Test
  public void testMultiConditionCaseVarchar() throws Exception {
    final String sql = "select n_name as name, " +
      "case n_name when 'BRAZIL' then concat(n_name, '_AMERICA') " +
      "when 'ALGERIA' then concat(n_name, '_AFRICA') " +
      "when 'ALGERIA' then concat(n_name, '_UNKNOWN') " +
      "when 'CHINA' then concat(n_name, '_ASIA') " +
      "when 'EGYPT' then concat(n_name, '_AFRICA') " +
      "else concat(n_name, '_OTHER') end as nation " +
      "from cp.\"tpch/nation.parquet\" order by n_name limit 6";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "nation")
      .baselineValues("ALGERIA", "ALGERIA_AFRICA")
      .baselineValues("ARGENTINA", "ARGENTINA_OTHER")
      .baselineValues("BRAZIL", "BRAZIL_AMERICA")
      .baselineValues("CANADA", "CANADA_OTHER")
      .baselineValues("CHINA", "CHINA_ASIA")
      .baselineValues("EGYPT", "EGYPT_AFRICA")
      .go();
  }

  @Test
  public void testMultiConditionCase() throws Exception {
    final String sql = "select n_name as name, " +
      "case n_regionkey when 0 then concat(n_name, '_AFRICA') " +
      "when 1 then concat(n_name, '_AMERICA') " +
      "when 2 then concat(n_name, '_ASIA') " +
      "else concat(n_name, '_OTHER') end as nation " +
      "from cp.\"tpch/nation.parquet\" order by n_name limit 6";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "nation")
      .baselineValues("ALGERIA", "ALGERIA_AFRICA")
      .baselineValues("ARGENTINA", "ARGENTINA_AMERICA")
      .baselineValues("BRAZIL", "BRAZIL_AMERICA")
      .baselineValues("CANADA", "CANADA_AMERICA")
      .baselineValues("CHINA", "CHINA_ASIA")
      .baselineValues("EGYPT", "EGYPT_OTHER")
      .go();
  }

  @Test
  public void testSingleConditionCase() throws Exception {
    final String sql = "select n_name as name, " +
      "case when n_regionkey = 0 then concat(n_name, '_AFRICA') else concat(n_name, '_OTHER') end as nation " +
      "from cp.\"tpch/nation.parquet\" limit 4";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "nation")
      .baselineValues("ALGERIA", "ALGERIA_AFRICA")
      .baselineValues("ARGENTINA", "ARGENTINA_OTHER")
      .baselineValues("BRAZIL", "BRAZIL_OTHER")
      .baselineValues("CANADA", "CANADA_OTHER")
      .go();
  }

  @Test
  public void testMultiProjectCase() throws Exception {
    final String sql = "select n_name as name, " +
      "case when n_regionkey = 0 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 0" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 1" +
      "   else 9 end " +
      "when n_regionkey = 1 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 10" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 11" +
      "   else 99 end " +
      "when n_regionkey = 2 then " +
      "   case when n_nationkey >= 0 and n_nationkey < 10 then 100" +
      "   when n_nationkey >= 10 and n_nationkey < 20 then 101" +
      "   else 999 end " +
      "else 0 end as _useless1," +
      "case when n_nationkey < 100 then " +
      "   case when n_regionkey >= 0 and n_regionkey < 3 then 1000" +
      "   when n_regionkey >= 3 and n_regionkey < 20 then 1111" +
      "   else 9999 end " +
      "else 0 end as _useless2 " +
      "from cp.\"tpch/nation.parquet\" limit 3";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("name", "_useless1", "_useless2")
      .baselineValues("ALGERIA", 0, 1000)
      .baselineValues("ARGENTINA", 10, 1000)
      .baselineValues("BRAZIL", 10, 1000)
      .go();
  }

  @Test
  public void testMixedCaseExpressionInWhen() throws Exception {
    final String sql = "select count(*) as _cnt from " +
      "(select CASE WHEN regexp_matches(CASE " +
      "    WHEN \n" +
      "            regexp_matches(LOWER(o_comment), '.*(above the).*')\n" +
      "            OR\n" +
      "            regexp_matches(LOWER(o_comment), '.*(fluffily).*')\n" +
      "        THEN 'FIRST100'\n" +
      "    WHEN \n" +
      "            regexp_matches(LOWER(o_comment), '.*(packages).*')\n" +
      "            OR\n" +
      "            regexp_matches(LOWER(o_comment), '.*(foxes).*')\n" +
      "        THEN 'SECOND100'\n" +
      "        ELSE 'LAST' END\n" +
      ", '.*(T1|D1).*') OR regexp_matches(o_comment, '.*( 1000\\:).*')\n" +
      "        THEN 'FIRST'\n" +
      "\n" +
      " WHEN\n" +
      "   regexp_matches(LOWER(o_comment), '.*(haggle furious).*')\n" +
      "    OR\n" +
      "   regexp_matches(LOWER(o_comment), '.*(blithely final).*')\n" +
      " THEN 'SECOND'\n" +
      " ELSE 'LAST'\n" +
      " END as filtered_comment\n" +
      "from cp.\"/tpch/orders.parquet\" ) " +
      " where filtered_comment = 'SECOND' OR filtered_comment = 'FIRST'";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_cnt")
      .baselineValues(4918L)
      .go();
  }

  @Test
  public void testComplexCase() throws Exception {
    final String sql = "select case when c.ingredients[2].name = 'Cheese' then 'Scrumptious' " +
      "                             when c.ingredients[0].name = 'Tomatoes' then 'Sweetened' " +
      "                             else 'Unknown' end as _flavor from cp.\"parquet/complex.parquet\" c";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_flavor")
      .baselineValues("Scrumptious")
      .baselineValues("Sweetened")
      .go();
  }

  @Test
  public void testMixedComplexCase() throws Exception {
    final String sql = "select case when c.ingredients[2].name = 'Cheese' then " +
      "                                  case c.inventor.age when 18 then 'C' when 25 then 'A' else 'U' end " +
      "                             when c.inventor.age > 10 then " +
      "                                 case when c.ingredients[1].name like 'C%' then 'AA' else " +
      "                                    case when c.inventor.age > 25 then 'BB' else 'CC' end end " +
      "                             when c.ingredients[0].name = 'Tomatoes' then 'Sweetened' " +
      "                             else 'Unknown' end as _flavor from cp.\"parquet/complex.parquet\" c";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_flavor")
      .baselineValues("A")
      .baselineValues("CC")
      .go();
  }

  @Test
  public void testComplexReturn() throws Exception {
    final String sql = "select case when c.ingredients[0].name = 'Beef' then c.ingredients " +
      "                             else NULL end as _new_recipe " +
      "  from cp.\"parquet/complex.parquet\" c";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("_new_recipe")
      .baselineValues((Object) null)
      .baselineValues(listOf(mapOf("name", "Beef"),
        mapOf("name", "Lettuce"), mapOf("name", "Cheese")))
      .go();
  }

  @Test
  public void testFilterCase() throws Exception {
    String sql = "WITH t1 AS \n" +
      "( \n" +
      "       SELECT \n" +
      "              CASE \n" +
      "                     WHEN n_regionkey = 0 THEN 'AFRICA' \n" +
      "                     WHEN n_regionkey = 1 THEN 'AMERICAS' \n" +
      "                     WHEN n_regionkey = 2 THEN 'ASIA' \n" +
      "                     ELSE 'OTHER123456789012' \n" +
      "              END AS region \n" +
      "       FROM   cp.\"tpch/nation.parquet\") \n" +
      "SELECT count(*) cnt\n" +
      "FROM   t1 \n" +
      "WHERE  CASE WHEN length(region) > 10 THEN region LIKE '%NOTTHERE%' ELSE region LIKE '%A' END";
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("cnt")
      .baselineValues(10L)
      .go();
  }
}
