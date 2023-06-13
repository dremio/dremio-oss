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

import static com.dremio.plugins.elastic.ElasticsearchType.OBJECT;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.plugins.elastic.ElasticBaseTestQuery.PushdownWithKeyword;
import com.google.common.collect.ImmutableMap;

/**
 * Test for elasticsearch contains with scripts disabled.  Runs all the tests in parent class with scripts disabled.
 */
@PushdownWithKeyword(enabled = true)
public class ITTestElasticsearchPushdownWithKeywordEnabled extends ElasticBaseTestQuery {
  @SuppressWarnings("checkstyle:MemberName")
  protected String TABLENAME;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(150, TimeUnit.SECONDS);

  @Before
  public void loadData() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", OBJECT, new Object[][]{
        {ImmutableMap.of("name", "Denmark-Norway")},
        {ImmutableMap.of("name", "Norway")},
        {ImmutableMap.of("name", "Denmark")}
      })
    };

    elastic.load(schema, table, data);
    TABLENAME = "elasticsearch." + schema + "." + table;
  }

  @Test
  public void testLike() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name like 'Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Norway")
      .go();
  }

  @Test
  public void testLikeHyphen() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name like 'Denmark-Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark-Norway")
      .go();
  }

  @Test
  public void testLikeMulti() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name like 'Denmark' or l.location.name like 'Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .baselineValues("Norway")
      .go();
  }

  @Test
  public void testLikeWithoutKeyword() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("name", TEXT, new Object[][]{
        {"Denmark-Norway"},
        {"Denmark"},
        {"Norway"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select name from %s where name like 'Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .expectsEmptyResultSet()
      .go();
  }

  @Test
  public void testLikePattern() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name like '%%Denmark%%'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .baselineValues("Denmark-Norway")
      .go();
  }

  @Test
  public void testEquals() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name = 'Denmark'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .go();
  }

  @Test
  public void testEqualsHyphen() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name = 'Denmark-Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark-Norway")
      .go();
  }

  @Test
  public void testEqualsMulti() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name = 'Denmark' or l.location.name = 'Norway'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .baselineValues("Norway")
      .go();
  }

  @Test
  public void testEqualsWithoutKeyword() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("name", TEXT, new Object[][]{
        {"Denmark-Norway"},
        {"Denmark"},
        {"Norway"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select name from %s where name = 'Norway' or name = 'Denmark'", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Norway")
      .baselineValues("Denmark")
      .baselineValues("Denmark-Norway")
      .go();
  }

  @Test
  public void testIn() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name in('Denmark')", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .go();
  }

  @Test
  public void testInHyphen() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name in('Denmark-Norway')", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark-Norway")
      .go();
  }

  @Test
  public void testInMulti() throws Exception {

    String sql = String.format("select l.location.name from %s l where l.location.name in('Denmark', 'Norway')", TABLENAME);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("name")
      .baselineValues("Denmark")
      .baselineValues("Norway")
      .go();
  }
}
