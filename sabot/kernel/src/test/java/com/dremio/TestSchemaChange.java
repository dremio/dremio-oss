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
    test("ALTER SYSTEM SET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "` = true");
    try {
      final String query = String.format("select * from dfs_root.`%s/schemachange/multi/` order by id", TEST_RES_PATH);
      test(query);
    } finally {
      test("ALTER SYSTEM RESET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "`");
    }
  }

  @Test
  public void testNewNestedColumn() throws Exception {
    test("ALTER SYSTEM SET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "` = true");
    try {
      final String query = String.format("select a from dfs_root.`%s/schemachange/nested/` order by id", TEST_RES_PATH);
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a")
        .baselineValues(mapOf("b", mapOf("c1", 1L)))
        .baselineValues(mapOf("b", mapOf("c2", 2L)))
        .go();
    } finally {
      test("ALTER SYSTEM RESET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "`");
    }
  }

  @Test
  public void keepLearningSchemaAcrossFiles() throws Exception {
    test("ALTER SYSTEM SET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "` = true");
    try {
      final String query = String.format("select * from dfs_root.`%s/schemachange/differentschemas/`", TEST_RES_PATH);
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
          .contains("New schema found and recorded. Please reattempt the query."));
      }
      // .. but the second attempt must not fail, full schema must have been learnt at this point
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("results/differentschemas.json")
        .build()
        .run();
    } finally {
      test("ALTER SYSTEM RESET `" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "`");
    }
  }
}
