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

import static com.dremio.TestBuilder.mapOf;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;

public class TestSchemaChange extends BaseTestQuery {

  protected static final String WORKING_PATH = TestTools.getWorkingPath();
  protected static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test
  public void testMultiFilesWithDifferentSchema() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = true");
    try {
      final String query = String.format("select * from dfs_root.\"%s/schemachange/multi/\" order by id", TEST_RES_PATH);
      test(query);
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\"");
    }
  }

  @Test
  public void testNewNestedColumn() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = true");
    try {
      final String query = String.format("select a from dfs_root.\"%s/schemachange/nested/\" order by id", TEST_RES_PATH);
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a")
        .baselineValues(mapOf("b", mapOf("c1", 1L)))
        .baselineValues(mapOf("b", mapOf("c2", 2L)))
        .go();
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\"");
    }
  }

  @Test
  public void keepLearningSchemaAcrossFiles() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = true");
    try {
      final String query = String.format("select * from dfs_root.\"%s/schemachange/differentschemas/\"", TEST_RES_PATH);
      try {
        testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .jsonBaselineFile("results/differentschemas.json")
          .build()
          .run();
      } catch (Exception e) {
        // first attempt may fail as a batch may have been sent to the user, so if it does, this must be the message ..
        assertTrue(e.getMessage()
          .contains("New schema found. Please reattempt the query."));
      }
      // .. but the second attempt must not fail, full schema must have been learnt at this point
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("results/differentschemas.json")
        .build()
        .run();
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\"");
    }
  }

  @Test
  public void testOldAndNewSchemaVisibleOnSchemaChange() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = false");
    try {
      final String query = String.format("select * from dfs_root.\"%s/schemachange/schemacontext/\"", TEST_RES_PATH);
      test(query);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("New schema found. Please reattempt the query."));
      assertTrue(e.getMessage().contains("Original Schema"));
      assertTrue(e.getMessage().contains("New Schema"));
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\"");
    }
  }

  @Test
  public void testParquetStructExtraField() throws Exception {
      // test folder has two parquet files
      //a.parquet has: 1 [{"f1":1,"f2":{"f_f_1":1,"f_f_2":2}}]
      //b.parquet has: 2 [{"f1":2,"f2":{"f_f_1":2,"f_f_3":3}}]

      // run select * to promote dataset

      String query = String.format("select * from dfs_root.\"%s/struct_extra_hdfs_test/\"", TEST_RES_PATH);
      try {
        test(query);
      } catch (Exception e) {
        // first attempt fails
        assertTrue(e.getMessage()
          .contains("New schema found. Please reattempt the query."));
      }

      try {
        test(query);
      } catch (Exception e) {
        // second attempt fails
        assertTrue(e.getMessage()
          .contains("New schema found. Please reattempt the query."));
      }

      // third attempt should pass
      test(query);

      // at this point dataset schema should be
      // col1 int, col2 array<struct<f1:int, f2:struct<f_f_1:int,f_f_2:int,f_f_3:int>>>

      query = String.format("select col1, col2[0].f1 as f1, col2[0].f2.f_f_1 as ff1, col2[0].f2.f_f_2 as ff2 " +
        "from dfs_root.\"%s/struct_extra_hdfs_test/\"", TEST_RES_PATH);
      testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "f1", "ff1", "ff2")
      .baselineValues(1,1,1,2)
      .baselineValues(2,2,2,null)
      .go();

      query = String.format("select col1, col2[0].f1 as f1, col2[0].f2.f_f_1 as ff1, col2[0].f2.f_f_3 as ff3 " +
        "from dfs_root.\"%s/struct_extra_hdfs_test/\"", TEST_RES_PATH);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "f1", "ff1", "ff3")
        .baselineValues(1,1,1,null)
        .baselineValues(2,2,2,3)
        .go();
  }
}
