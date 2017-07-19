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
package com.dremio.exec.store.parquet.r3;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.hppc.LongIntHashMap;
import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.google.common.base.Stopwatch;

@Ignore
public class TestReader3 extends BaseTestQuery {

  @Rule
  public final TestRule TIMEOUT =  TestTools.getTimeoutRule(50000, TimeUnit.SECONDS);

//  @Ignore
  @Test
  public void createNewFile() throws Exception {
    test("set \"store.parquet.compression\" = 'none'");
    test("set \"store.parquet.page-size\" = 8192");
    testNoResult("set planner.disable_exchanges=true");
    test("create table dfs_root.\"/tmp/bah\" as select L_ORDERKEY, L_LINENUMBER, L_COMMENT from dfs_root.opt.data.sfb");
  }

  @Test
  public void doubleInt64() throws Exception {
    test("select L_ORDERKEY, L_LINENUMBER from dfs_root.opt.data.sf1p_head2 limit 10");
  }

  @Test
  public void singleInt64() throws Exception {
    test("select L_ORDERKEY from dfs_root.opt.data.sf1p_head2 limit 10");
  }

  @Test
  public void filtered1() throws Exception {
    test("select L_ORDERKEY, L_LINENUMBER from dfs_root.opt.data.sf1p_head2 where L_ORDERKEY = 3 limit 10");
  }

  @Test
  public void doMany(){
    for(int x =0; x < 20; x++){
    LongIntHashMap map = new LongIntHashMap();
    Stopwatch watch = Stopwatch.createStarted();
    for(int i =0; i < 24_000_000; i++){
      map.put(i % 7, i);
    }

    System.out.println(watch.elapsed(TimeUnit.MICROSECONDS));
    }
  }

  @Test
  public void nonFilter1() throws Exception {
    test("select count(L_ORDERKEY) from dfs_root.opt.data.sf1p_head2 where L_ORDERKEY/3 = 1");
  }

  @Test
  public void var() throws Exception {
    test("select count(*) from dfs_root.tmp.bah limit 10" );
  }

  @Test
  public void var2() throws Exception {
    test("select count(*) from dfs_root.tmp.bah where L_COMMENT like '%bold%'" );
  }

  @Test
  public void filtered2() throws Exception {
    test("select count(*) from dfs_root.opt.data.sf1p_head2 where L_ORDERKEY = 3 limit 10");
  }

  @Test
  public void hashagg() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    for(int i = 0; i < 10000; i++){
      testNoResult("select L_LINENUMBER, count(*) from dfs_root.tmp.bah6 group by L_LINENUMBER");
    }
  }

  @Test
  public void varcharagg() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    for(int i = 0; i < 10000; i++){
      testNoResult("select count(*) from dfs_root.tmp.bah where L_COMMENT is not null" );
    }
  }

  @Test
  public void allThree() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    for(int i = 0; i < 10000; i++){
      testNoResult("select count(*) from dfs_root.tmp.bah where L_COMMENT is not null and L_LINENUMBER is not null and L_ORDERKEY is not null" );
    }
  }

  @Test
  public void xxx() throws Exception {
    test("set \"store.parquet.compression\" = 'none'");
//    test("set \"store.parquet.page-size\" = 8192");
    testNoResult("set planner.disable_exchanges=true");
    test("create table dfs_root.\"/tmp/bah6\" as select case when mod(L_ORDERKEY, 2) = 0 then L_ORDERKEY ELSE NULL END as L_ORDERKEY, L_LINENUMBER, case when mod(L_ORDERKEY, 2) = 0 then L_COMMENT ELSE NULL END as L_COMMENT from dfs_root.opt.data.bah");
  }

  @Test
  public void everyOtherNullBinary() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    String query = "select count(L_COMMENT) from dfs_root.tmp.bah5 where L_ORDERKEY/3 = 1";
    for(int i =0; i < 10000; i++){
      testNoResult(query);
    }
  }

  @Test
  public void first5000() throws Exception {
    testNoResult("set planner.disable_exchanges=true");

    test("select concat(counter(), L_COMMENT) from dfs_root.tmp.bah5 limit 505");
  }

  @Test
  public void everyOtherNull() throws Exception {
    testNoResult("set planner.disable_exchanges=true");

    for(int i =0; i < 10000; i++){
//      testNoResult("select count(L_ORDERKEY) from dfs_root.tmp.bah4 where L_ORDERKEY/3 = 1");

      test("select L_COMMENT from dfs_root.opt.data.bah5 OFFSET 0 FETCH NEXT 1 ROWS ONLY");
      System.out.println("hello");
    }
  }

  @Test
  public void testX() throws Exception {
    test("select * from dfs_root.\"/src/dremio/contrib/data/tpch-sample-data/target/tpch/orders.parquet\" where o_orderpriority = 'blue'");
//    test("select count(*) from dfs_root.\"/opt/data/sf0.1p/lineitem/\"");
//    test("select * from dfs_root.\"/opt/data/sf1p/region/region.parquet\"");
//    test("select * from cp.\"tpch/region.parquet\"");
//    test("select sum(c_custkey), sum(c_nationkey), sum(c_acctbal), min(c_mktsegment), min(c_comment) from dfs_root.\"/opt/data/tpch10/customer/c0/0_0_0.parquet\"");
//    test("select * from dfs_root.opt.data.sf1p.nation");
  }
  @Test
  public void everyOtherNullx() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    testNoResult("create table dfs_root.\"tmp/chunk/old2\" STORE AS (type => 'text', fieldDelimiter => ',') as select L_COMMENT from dfs_root.opt.data.bah5 OFFSET 23900000 FETCH NEXT 100 ROWS ONLY");
  }

  @Test
  public void simpleScan() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    for(int i =0; i < 10000; i++){
      test("select count(L_ORDERKEY) from dfs_root.opt.data.bah5 where L_ORDERKEY/3 = 1");
      test("select L_ORDERKEY from dfs_root.opt.data.bah5 where L_ORDERKEY/3 = 1 limit 10");
//      testNoResult("create table dfs_root.\"tmp/oldReaderFile\" store as (type => 'json', prettyPrint => false) as select L_ORDERKEY, L_COMMENT from dfs_root.opt.data.bah5");
    }
  }

  @Test
  public void x() throws Exception{
    testNoResult("set planner.disable_exchanges=true");
    testNoResult("create table dfs_root.\"tmp/chunk/gb5\" as select cast(l_linenumber as varchar) as varline, cast(l_linenumber as int) as l_linenumber from dfs_root.opt.data.bah5 limit 10");
  }

  @Test
  public void everyOtherNull2() throws Exception {
    testNoResult("set planner.disable_exchanges=true");

    for(int i =0; i < 10000; i++){
//      test("select l_linenumber, l_linenumber+100, l_linenumber+1000, l_linenumber+10000, l_linenumber+100000, count(l_linenumber) from dfs_root.\"tmp/chunk/gb2/01.parquet\" group by l_linenumber, l_linenumber+100, l_linenumber+1000, l_linenumber+10000, l_linenumber+100000");
//      test("select varline, varline || 'a', varline || 'b', varline || 'c', varline || 'd', varline || 'e', count(l_linenumber), count(l_linenumber*2), count(l_linenumber*3), count(l_linenumber*4) from dfs_root.\"tmp/chunk/gb4\" group by varline, varline || 'a', varline || 'b', varline || 'c', varline || 'd', varline || 'e'");
//      test("select varline, varline || 'a', count(l_linenumber) from dfs_root.\"tmp/chunk/gb4\" group by varline, varline || 'a'");
      test("select varline, count(l_linenumber) from dfs_root.\"tmp/chunk/gb4\" group by varline");
//      test("select varline, varline || 'a', varline || 'b', varline || 'c', count(l_linenumber), count(l_linenumber*2), count(l_linenumber*3), count(l_linenumber*4) from dfs_root.\"tmp/chunk/gb4\" group by varline, varline || 'a', varline || 'b', varline || 'c'");
    }
  }

  @Test
  public void nbp() throws Exception {
    testNoResult("set planner.disable_exchanges=true");

    for(int i =0; i < 10000; i++){
      test("select count(*) from (SELECT\n" +
          "        ID_UID,     \n" +
          "        (LL_PREN||' '||LL_NOM||' ('||ID_UID||')') AS USER_NAME,\n" +
          "         CAST (NU_PERIOD AS VARCHAR (6)) AS PERIOD,\n" +
          "         COALESCE (LL_GA, CD_GA_FCT) AS BUSINESS_GROUP,\n" +
          "         COALESCE (LL_EJ, CD_EJ_FCT) AS LEGAL_ENTITY,\n" +
          "         CASE\n" +
          "            WHEN ID_ROLE = 'SB'\n" +
          "            THEN\n" +
          "               'SENIOR BANKER'\n" +
          "            WHEN ID_ROLE = 'GRM1'\n" +
          "            THEN\n" +
          "               'PILOT BANKER'\n" +
          "            WHEN ID_ROLE = 'GRM2'\n" +
          "            THEN\n" +
          "               'SUPPORT PILOT'\n" +
          "            WHEN ID_ROLE = 'RM1'\n" +
          "            THEN\n" +
          "               'CONTRIBUTORS RM1'\n" +
          "            WHEN (ID_ROLE = 'RRM1' AND IN_CONTRIB_FINAL_LE = 'Y')\n" +
          "            THEN\n" +
          "               'CONTRIBUTORS RRM1'\n" +
          "            WHEN (ID_ROLE = 'RRM2' AND IN_CONTRIB_FINAL_LE = 'Y')\n" +
          "            THEN\n" +
          "               'CONTRIBUTORS RRM2'\n" +
          "            ELSE\n" +
          "               'UNKNOWN'\n" +
          "         END\n" +
          "            AS ROLE,\n" +
          "         CASE WHEN RTRIM (LL_ZON_GEO1, ' ') = '' THEN NULL ELSE LL_ZON_GEO1 END\n" +
          "            AS LE_BUSINESS_REGION,\n" +
          "         CASE WHEN RTRIM (LL_ZON_GEO2, ' ') = '' THEN NULL ELSE LL_ZON_GEO2 END\n" +
          "            AS LE_BUSINESS_SUB_REGION,\n" +
          "         CASE WHEN RTRIM (LL_PAY, ' ') = '' THEN NULL ELSE LL_PAY END\n" +
          "            AS LE_BUSINESS_COUNTRY,\n" +
          "         SUM (CASE WHEN IN_REV_BLOCK = 1 THEN COALESCE (MT_REV_N2, 0) END)\n" +
          "            AS REVENUES_N2,\n" +
          "         SUM (CASE WHEN IN_REV_BLOCK = 1 THEN COALESCE (MT_REV_N1, 0) END)\n" +
          "            AS REVENUES_N1,\n" +
          "         SUM (CASE WHEN IN_REV_BLOCK = 1 THEN COALESCE (MT_REV_YTD_N1, 0) END)\n" +
          "            AS REVENUES_YTD_N1,\n" +
          "         SUM (CASE WHEN IN_REV_BLOCK = 1 THEN COALESCE (MT_REV, 0) END)\n" +
          "            AS REVENUES_YTD_N\n" +
          "    FROM dfs_root.tmp.bnp " +
          "   WHERE NOT (    COALESCE (MT_REV_N2, 0) = 0\n" +
          "                  AND COALESCE (MT_REV_N1, 0) = 0\n" +
          "                  AND COALESCE (MT_REV_YTD_N1, 0) = 0\n" +
          "                  AND COALESCE (MT_REV, 0) = 0)\n" +
          "         AND (    LL_ZON_GEO1 IS NOT NULL\n" +
          "              AND LL_ZON_GEO1 <> 'NULL'\n" +
          "              AND LL_ZON_GEO2 IS NOT NULL\n" +
          "              AND LL_PAY IS NOT NULL)\n" +
          "GROUP BY ID_UID, \n" +
          "    (LL_PREN||' '||LL_NOM||' ('||ID_UID||')'),\n" +
          "         NU_PERIOD,\n" +
          "         COALESCE (LL_GA, CD_GA_FCT),\n" +
          "         COALESCE (LL_EJ, CD_EJ_FCT),\n" +
          "         ID_ROLE,\n" +
          "         IN_CONTRIB_FINAL_LE,\n" +
          "         LL_ZON_GEO1,\n" +
          "         LL_ZON_GEO2,\n" +
          "         LL_PAY)");
    }
  }

  @Test
  public void join() throws Exception {
    testNoResult("set planner.disable_exchanges=true");
    for(int i =0; i < 15; i++){
      test("select\n" +
          "  o.o_orderpriority,\n" +
          "  count(*) as order_count\n" +
          "from\n" +
          "  dfs.opt.data.sf1p.orders o\n" +
          "\n" +
          "where\n" +
          "  o.o_orderdate >= date '1996-10-01'\n" +
          "  and o.o_orderdate < date '1996-10-01' + interval '3' month\n" +
          "  and\n" +
          "  exists (\n" +
          "    select\n" +
          "      *\n" +
          "    from\n" +
          "      dfs.opt.data.sf1p.lineitem l\n" +
          "    where\n" +
          "      l.l_orderkey = o.o_orderkey\n" +
          "      and l.l_commitdate < l.l_receiptdate\n" +
          "  )\n" +
          "group by\n" +
          "  o.o_orderpriority\n" +
          "order by\n" +
          "  o.o_orderpriority\n" +
          "");
    }
  }

  @Test
  public void anotherTest() throws Exception {
    test("alter system set planner.enable_decimal_data_type = true");
    test("select c_date, count(distinct c_integer) from dfs.`/Users/jnadeau/Downloads/0_0_0.parquet` group by c_date");
//    test("select c_date, c_integer from dfs.`/Users/jnadeau/Downloads/0_0_0.parquet` where c_date = date '2015-03-06' group by c_date, c_integer");
//    test("select distinct c_integer from dfs.`/Users/jnadeau/Downloads/0_0_0.parquet` where c_date = date '2015-03-06'");
  }

}
