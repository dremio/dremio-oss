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
package com.dremio.plugins.elastic;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.rpc.user.QueryDataBatch;

/*
Should use this for storage plugin (bootstrap-storage-plugins.json)
with the correct IP address.  Also, should load yelp dataset into elasticsearch.
{
  "storage":{
    elasticsearch : {
      type:"elasticsearch",
      enabled: true,
      "hosts"               : "172.30.10.101:9200",
      "batch_size"          : "1000",
      "scroll_timeout"      : "65000"
    }
  }
}
 */

@Ignore
public class TestExternal extends BaseTestQuery {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(50000, TimeUnit.SECONDS);

  @Test
  public void query() throws Exception {
    System.out.println(getResultString(testRunAndReturn(QueryType.SQL, "show databases"), ""));
    System.out.println(getResultString(testRunAndReturn(QueryType.SQL, "select * from elasticsearch.twitter.tweet"), ""));
  }

  @Test
  public void query01() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for select count(*) from elasticsearch.academic.business");
    test("select count(*) from elasticsearch.academic.business");
  }

  @Test
  public void query02() throws Exception {
    test("explain plan for select avg(review_count) from elasticsearch.academic.business");
    test("select avg(review_count) from elasticsearch.academic.business");
  }

  @Test
  public void query03() throws Exception {
    test("explain plan for select * from elasticsearch.academic.business");
    //test("select * from elasticsearch.business_sizes.onemb");
  }

  @Test
  public void query04() throws Exception {
    test("explain plan for select count(*), sum(stars), avg(review_count) from elasticsearch.academic.business");
    test("select count(*), sum(stars), avg(review_count) from elasticsearch.academic.business");
  }

  @Test
  public void query05() throws Exception {
    test("explain plan for select count(*) from elasticsearch.academic.business where stars > 4.9");
    test("select count(*) from elasticsearch.academic.business where stars > 4.9");
  }

  @Test
  public void query06() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for select count(*), min(review_count) from elasticsearch.academic.business where stars > 4 and " +
            "review_count > 100");
    test("select count(*), min(review_count) from elasticsearch.academic.business where stars > 4 and " +
            "review_count > 100");
  }

  @Test
  public void query07() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for select state, city, count(*) from elasticsearch.academic.business where review_count > 100 " +
            "group by state,city");
    test("select state, city, count(*) from elasticsearch.academic.business where review_count > 100 group by state,city");
  }

  @Test
  public void query08() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for SELECT * FROM elasticsearch.academic.business where review_count > 1000");
    test("SELECT * FROM elasticsearch.academic.business where review_count > 1000");
  }

  @Test
  public void query09() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for SELECT count(*), max(review_count) FROM elasticsearch.academic.business where review_count > " +
            "1000");
    test("SELECT count(*), max(review_count) FROM elasticsearch.academic.business where review_count > 1000");
  }

  @Test
  public void query10() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for SELECT state, count(*), max(review_count) FROM elasticsearch.academic.business where " +
            "review_count > 1000 group by state");
    test("SELECT state, count(*), max(review_count) FROM elasticsearch.academic.business where " +
            "review_count > 1000 group by state");
  }

  // Will have exchange (HashToRandom) below HassAggPrel.
  @Test
  public void query11() throws Exception {
    //test("alter session set \"planner.disable_exchanges\"=true");
    test("explain plan for select yelping_since, count(*), avg(average_stars) from elasticsearch.academic.\"user\" where " +
            "review_count > 100 group by yelping_since");
    test("select yelping_since, count(*), avg(average_stars) from elasticsearch.academic.\"user\" where " +
            "review_count > 100 group by yelping_since");
  }

  // Will have exchange (HashToRandom) below HassAggPrel.
  @Test
  public void query12() throws Exception {
    test("explain plan for select distinct t.votes.funny from elasticsearch.academic.review t limit 10");
    //test("select distinct t.votes.funny from elasticsearch.academic.review t limit 10");
  }

  @Test
  public void query13() throws Exception {
    test("explain plan for select state as TYPEASTHIS, name from elasticsearch.academic.business limit 10000");
    test("select state as TYPEASTHIS, name from elasticsearch.academic.business limit 10000");
  }

  @Test
  public void query14() throws Exception {
    test("explain plan for select state as TYPEASTHIS, name from elasticsearch.academic.business group by state, name");
    test("select state as TYPEASTHIS, name from elasticsearch.academic.business group by state, name");
  }

  @Test
  public void query15() throws Exception {
    test("explain plan for select count(distinct state), count(distinct city) from elasticsearch.academic.business");
    test("select count(distinct state), count(distinct city) from elasticsearch.academic.business");
  }

  @Test
  public void query16() throws Exception {
    test("explain plan for select stddev_pop(stars) from elasticsearch.academic.business");
    test("select stddev_pop(stars) from elasticsearch.academic.business");
  }

  @Test
  public void query17() throws Exception {
    test("explain plan for select stddev(stars) from elasticsearch.academic.business");
    test("select stddev(stars) from elasticsearch.academic.business");
  }

  @Test
  public void query18() throws Exception {
    test("explain plan for select stddev_samp(stars) from elasticsearch.academic.business");
    test("select stddev_samp(stars) from elasticsearch.academic.business");
  }

  @Test
  public void query19() throws Exception {
    test("explain plan for select var_pop(stars) from elasticsearch.academic.business");
    test("select var_pop(stars) from elasticsearch.academic.business");
  }

  @Test
  public void query20() throws Exception {
    test("explain plan for select variance(stars) from elasticsearch.academic.business");
    test("select variance(stars) from elasticsearch.academic.business");
  }

  @Test
  public void query21() throws Exception {
    test("explain plan for select var_samp(stars) from elasticsearch.academic.business");
    test("select var_samp(stars) from elasticsearch.academic.business");
  }

  // Will have exchange (HashToRandom) below HassAggPrel.
  @Test
  public void query22() throws Exception {
    test("explain plan for select yelping_since, count(*), avg(average_stars) from elasticsearch.academic.\"user\" group by " +
            "yelping_since");
    test("select yelping_since, count(*), avg(average_stars) from elasticsearch.academic.\"user\" group by " +
            "yelping_since");
  }

  @Test
  public void query23() throws Exception {
    String query = "select count(stars), sum(review_count), count(*) from elasticsearch.academic.business where stars >=4";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query24() throws Exception {
    String query = "SELECT * FROM (SELECT \"Custom_SQL_Query\".\"business_id\" AS \"business_id\" FROM \n" +
            "  (select * from elasticsearch.\"clickstreams2016*\".click) \"Custom_SQL_Query\"\n" +
            " GROUP BY \"Custom_SQL_Query\".\"business_id\") T LIMIT 0";
    test("explain plan for " + query);
    printSchema(query);
    //test(query);
  }

  @Test
  public void query25() throws Exception {
    test("explain plan for select * from elasticsearch.academic.business b join elasticsearch.academic.review r on b._id = r._id limit 2");
    //test("select * from elasticsearch.academic.business b join elasticsearch.academic.review r on b._id = r._id limit 2");
  }

  @Test
  public void query26() throws Exception {
    test("alter system set \"exec.enable_union_type\"=true");
    //test("alter system set \"planner.enable_mergejoin\"=false");
    test("explain plan for select b.name, avg(r.votes.funny) as avg_funny, avg(r.votes.useful) as avg_useful, avg(r" +
            ".votes.cool) as avg_cool from elasticsearch.academic.business b join elasticsearch.academic.review r on b._id = r.business_id group by b.name;");
    //test("select b.name, avg(r.votes.funny) as avg_funny, avg(r.votes.useful) as avg_useful, avg(r.votes.cool) " +
    //        "as avg_cool from elasticsearch.academic.business b join elasticsearch.academic.review r on b._id = r.business_id group by b.name;");
  }

  @Test
  public void query27() throws Exception {
    test("alter system set \"exec.enable_union_type\"=true");
    test("select * from elasticsearch.academic.business;");
  }

  @Test
  public void query28() throws Exception {
    test("explain plan for select t.categories[0] from elasticsearch.academic.business t limit 100");
    test("select t.categories[0] from elasticsearch.academic.business t limit 100");
  }

  @Test
  public void query29() throws Exception {
    test("select * from elasticsearch.academic.business b join elasticsearch.academic.business r on b.business_id= r" +
            ".business_id limit 2");
    test("select * from elasticsearch.academic.business b join elasticsearch.academic.business r on b._id= r._id limit 2");
  }

  @Test
  public void query30() throws Exception {
    test("explain plan for select avg(stars) avg_stars, state from (select city,review_count,state,stars,business_id, name,flatten(categories) "
            +" from elasticsearch.academic.business order by stars desc limit 5000) where state <> 'WI' group by state");
    test("select avg(stars) avg_stars, state from (select city,review_count,state,stars,business_id, name,flatten(categories) "
            +" from elasticsearch.academic.business order by stars desc limit 5000) where state <> 'WI' group by state");
  }

  @Test
  public void query31() throws Exception {
    String query = "select state, stars, flatten(categories) from (select stars, state, name, categories " +
            " from elasticsearch.academic.business order by stars desc limit 30000) where state <> 'WI'";
    test("explain plan for " + query);
    //test(query);
  }

  @Test
  public void query32() throws Exception {
    String query = "SELECT * FROM (SELECT \"business\".\"state\" AS \"state\", SUM(1) AS \"sum_Number_of_Records_ok\"\n" +
            " FROM \"elasticsearch.academic\".\"business\" \"business\" GROUP BY \"business\".\"state\") T LIMIT 0";
    test("explain plan for " + query);
    printSchema(query);
    test(query);
  }

  @Test
  public void query33() throws Exception {
    test("explain plan for select stars, count(*) from elasticsearch.academic.business group by stars limit 0");
    test("select stars, count(*) from elasticsearch.academic.business group by stars limit 0");
  }

  @Test
  public void query34() throws Exception {
    String query = "SELECT \"business\".\"longitude\", count(1) FROM \"elasticsearch.academic\".\"business\" \"business\"\n" +
            "group by \"business\".\"longitude\" limit 10";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query35() throws Exception {
    String query = "SELECT AVG(\"business\".\"longitude\") AS \"avg_longitude_ok\" FROM \"elasticsearch.academic\".\"business\" \"business\"\n" +
            " where review_count < 10 limit 10";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query36() throws Exception {
    String query = "SELECT * FROM (SELECT AVG(\"business\".\"longitude\") AS \"avg_longitude_ok\" FROM \"elasticsearch.academic\".\"business\" \"business\"\n" +
            " where review_count < 10 HAVING (COUNT(1) > 0)) T LIMIT 0";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query37() throws Exception {
    String query = "SELECT \"review\".\"stars\" AS \"stars__review_\" FROM \"elasticsearch.academic\".\"review\" \"review\"\n" +
            "GROUP BY \"review\".\"stars\" limit 10";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query38() throws Exception {
    String query = "SELECT * FROM (SELECT SUM(\"user\".\"fans\") AS \"sum_fans_ok\", \"user\".\"yelping_since\" AS \"yelping_since\"\n" +
            " FROM \"elasticsearch.academic\".\"user\" \"user\" GROUP BY \"user\".\"yelping_since\") T LIMIT 0";
    test("explain plan for " + query);
    printSchema(query);
    test(query);
  }

  @Test
  public void query39() throws Exception {
    String query = "SELECT * FROM (SELECT AVG(\"user\".\"average_stars\") AS \"avg_average_stars_ok\",\n" +
            "  AVG(CAST(\"user\".\"review_count\" AS FLOAT)) AS \"avg_review_count_ok\",\n" +
            "  \"user\".\"yelping_since\" AS \"yelping_since\"\n" +
            "FROM \"elasticsearch.academic\".\"user\" \"user\"\n" +
            "WHERE (\"user\".\"yelping_since\" IN ('2014-01', '2014-02', '2014-03', '2014-04', '2014-05', '2014-06', '2014-07', '2014-08', '2014-09', '2014-10', '2014-11', '2014-12'))\n" +
            "GROUP BY \"user\".\"yelping_since\") T LIMIT 0\n";

    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query40() throws Exception {
    String query = "SELECT * FROM (SELECT \"user\".\"name\" AS \"name\",\n" +
            "  SUM(1) AS \"sum_Number_of_Records_ok\"\n" +
            "FROM \"elasticsearch.academic\".\"user\" \"user\"\n" +
            "GROUP BY \"user\".\"name\") T LIMIT 0";
    test("explain plan for " + query);
    test(query);
  }

  @Test // still fails due to multi projection issue
  public void query41() throws Exception {
    String query = "SELECT * FROM (SELECT COUNT(1) AS \"cnt_Number_of_Records_ok\",\n" +
            "  \"user\".\"name\" AS \"name\"\n" +
            "FROM \"elasticsearch.academic\".\"user\" \"user\"\n" +
            "GROUP BY \"user\".\"name\") T LIMIT 0";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query42() throws Exception {
    test("explain plan for select count(*), sum(1), count(1) from elasticsearch.academic.business");
    test("select count(*), sum(1), count(1) from elasticsearch.academic.business");
  }

  @Test
  public void query43() throws Exception {
    String query = "select count(stars), sum(review_count), count(1), sum(1)from elasticsearch.academic.business";
    test("explain plan for " + query);
    test(query);
  }

  // Bug related to elastic search booleans (without the fix this fix, it returns false, false
  @Test
  public void query44() throws Exception {
    String query = "select \"open\", not \"open\" from elasticsearch.academic.business group by \"open\"";
    test(query);
  }

  @Test
  public void query45() throws Exception {
    /*
    Field validator = IteratorValidatorBatchIterator.class.getDeclaredField("VALIDATE_VECTORS");
    validator.setAccessible(true);
    Field modifiers = Field.class.getDeclaredField("modifiers");
    modifiers.setAccessible(true);
    modifiers.set(validator, validator.getModifiers() & ~Modifier.FINAL);

    validator.set(null, true);
    */

    //test("alter session set \"exec.enable_union_type\"=true");
    String query = "select * from (select * from elasticsearch.academic.business limit 1000) t left join (select * from elasticsearch.academic.business limit 500) s on t.business_id = s.business_id";
    query = "select * from (select business_id as BID1, categories as CAT1 from elasticsearch.academic.business limit 1000) t left join (select business_id as BID2, categories as CAT2 from elasticsearch.academic.business limit 10) s on t.BID1 = s.BID2";
    test(query);
  }

  @Test
  public void query46() throws Exception {
    String query = "select b.name, avg(r.votes.funny) as avg_funny, avg(r.votes.useful) as avg_useful, avg(r.votes.cool) as avg_cool from elasticsearch.academic.business b join elasticsearch.academic.review r on b.business_id =r.business_id group by b.name";
    test("explain plan for " + query);

    test(query);

    //query = "SELECT * FROM (SELECT SUM(\"user\".\"fans\") AS \"sum_fans_ok\", \"user\".\"yelping_since\" AS \"yelping_since\"\n" +
    //        " FROM \"elasticsearch.academic\".\"user\" \"user\" GROUP BY \"user\".\"yelping_since\") T LIMIT 0";
    //test("explain plan for " + query);
  }

  @Test
  public void query47() throws Exception {
    String query = "select state, city, count(stars) as countStar, avg(stars) as avgStar, sum(review_count) as sumReview, avg( (stars-1)*2) as avgFunc from elastic.academic.business where state <> 'NV' and ((stars - 1)*2) > 4 and \"open\" group by state, city";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query48() throws Exception {
    String query = "select t.attributes.\"Accepts Credit Cards\" from elasticsearch.academic.business t group by t.attributes.\"Accepts Credit Cards\"";
    test("explain plan for " + query);
    test(query);
  }

  @Test
  public void query50() throws Exception {
    test("set planner.leaf_limit_enable = true");
    String query = "WITH dremio_limited_preview AS (\n" +
        "  select * from elasticsearch.academic.review where contains(text:amazing)\n" +
        ") SELECT * FROM dremio_limited_preview LIMIT 500";
    test("explain plan for " + query);
    test("set planner.leaf_limit_enable = false");
  }

  private void printSchema(String query) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(query);
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    QueryDataBatch batch = results.get(0);
    loader.load(batch.getHeader().getDef(), batch.getData());
    System.out.println(loader.getSchema());
    VectorUtil.showVectorAccessibleContent(loader);
  }
}
