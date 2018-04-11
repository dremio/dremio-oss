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
package com.dremio.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
import com.dremio.exec.util.GlobalDictionaryBuilder;

/**
 * Global dictionary planning test.
 */
public class TestGlobalDictionaryPlan extends PlanTestBase {

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  private static Path tableDirPath1;
  private static Path tableDirPath2;
  private static FileSystem fs;
  private static String[] ALL_COLUMNS = new String[] {"DictionaryLookup(decoded fields=[[city, group, position, state]])"};

  @BeforeClass
  public static void setup() throws Exception {
    testNoResult("alter session set `store.parquet.enable_dictionary_encoding_binary_type`=true");
    testNoResult("CREATE TABLE dfs_test.globaldictionary AS SELECT * FROM cp.`globaldictionary.json`");
    testNoResult("CREATE TABLE dfs_test.places AS SELECT * FROM cp.`places.json`");
    fs = FileSystem.getLocal(new Configuration());

    tableDirPath1 = new Path(getDfsTestTmpSchemaLocation() + "/globaldictionary");
    GlobalDictionaryBuilder.createGlobalDictionaries(fs, tableDirPath1, getAllocator());

    tableDirPath2 = new Path(getDfsTestTmpSchemaLocation() + "/places");
    GlobalDictionaryBuilder.createGlobalDictionaries(fs, tableDirPath2, getAllocator());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // set it to default
    testNoResult("alter session set `store.parquet.enable_dictionary_encoding_binary_type`=false");
    BaseTestQuery.testNoResult("alter session set `planner.enable_global_dictionary`=true");
    fs.delete(tableDirPath1, true);
    fs.delete(tableDirPath2, true);
  }

  @Test
  public void testLimit() throws Exception {
    final String query = "select * from dfs_test.globaldictionary limit 1";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query, ALL_COLUMNS, null);
    validateResults(query, "testLimit");
  }

  @Test
  public void testSimpleProject() throws Exception {
    final String query = "select * from dfs_test.globaldictionary";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query, ALL_COLUMNS, null);
    validateResults(query, "testSimpleProject");
  }

  @Test
  public void testSimpleFilter() throws Exception {
    final String query = "select * from dfs_test.globaldictionary where employee_id > 1100";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query, ALL_COLUMNS, null);
    validateResults(query, "testSimpleFilter");
  }

  @Test
  public void testFilterWithDictionaryColumn() throws Exception {
    final String query = "select * from dfs_test.globaldictionary where state='TX'";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city, group, position]])"}, null);
    validateResults(query, "testFilterWithDictionaryColumn");
  }

  @Test
  public void testSimpleGroupBy() throws Exception {
    final String query = "select city, state from dfs_test.globaldictionary group by city, state";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query, new String[] {"DictionaryLookup(decoded fields=[[city, state]])"}, null);
    validateResults(query, "testSimpleGroupBy");
  }

  @Test
  public void testGroupByWithFilter() throws Exception {
    final String query = "select city, state from dfs_test.globaldictionary where state='TX' and `position`='STOR' group by city,state";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
//    testPlanSubstrPatternsInOrder(query,
//      new String[] {"DictionaryLookup(decoded fields=[[city]])", "DictionaryLookup(decoded fields=[[state]])"}, null);
    validateResults(query, "testGroupByWithFilter");
  }

  @Test
  public void testSimpleAgg() throws Exception {
    final String query = "select count(state), last_name from dfs_test.globaldictionary group by last_name";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    validateResults(query, "testSimpleAgg");
  }

  @Test
  public void testAggWithDecoding() throws Exception {
    final String query = "select count(state || city), last_name from dfs_test.globaldictionary group by last_name";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city, state]])"}, null);
    validateResults(query, "testAggWithDecoding");
  }

  @Test
  public void testSelfJoin() throws Exception {
    final String query = "select * from dfs_test.globaldictionary t1 inner join dfs_test.globaldictionary t2 on t1.city = t2.state";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city0, group, group0, position, position0, state]])", "DictionaryLookup(decoded fields=[[state0]])", "DictionaryLookup(decoded fields=[[city]])"}, null);
    validateResults(query, "testSelfJoin");
  }

  @Test
  public void testSimpleInnerJoin() throws Exception {
    final String query = "select * from dfs_test.globaldictionary t1 inner join dfs_test.places t2 on t1.employee_id = t2.employee_id";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city, group, place, position, state]])"}, null);
    validateResults(query, "testSimpleInnerJoin");
  }

  @Test
  public void testInnerJoinWithDecoding() throws Exception {
    final String query = "select * from dfs_test.globaldictionary t1 inner join dfs_test.places t2 on t1.state = t2.place";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city, group, position]])",
        "DictionaryLookup(decoded fields=[[place]])", "DictionaryLookup(decoded fields=[[state]])"}, null);
    validateResults(query, "testInnerJoinWithDecoding");
  }

  @Test
  public void testInnerJoinWithFilter() throws Exception {
    final String query = "select * from dfs_test.globaldictionary t1 inner join dfs_test.places t2 on t1.state = t2.place where `position`='Store'";
    disableGlobalDictionary();
    testPlanOneExcludedPattern(query, "DictionaryLookup");
    enableGlobalDictionary();
    testPlanSubstrPatternsInOrder(query,
      new String[] {"DictionaryLookup(decoded fields=[[city, group]])",
        "DictionaryLookup(decoded fields=[[place]])", "DictionaryLookup(decoded fields=[[state]])"}, null);
    validateResults(query, "testInnerJoinWithFilter");
  }
}
