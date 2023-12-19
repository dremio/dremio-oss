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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;

public class ITDeltaMetadataRefresh extends BaseTestQuery {

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(80, TimeUnit.SECONDS);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ITDeltaMetadataRefresh.class);

  static FileSystem fs;
  static String testRootPath = "/tmp/deltalake/";
  static Configuration conf;
  static Path p = new Path(testRootPath);
  static String[] testTables = {"testDataset",
      "lastCheckpointDataset",
      "schema_change_partition",
      "JsonDataset",
      "commitInfoAtOnlyJson"
  };

  @Before
  public void before() throws Exception {
    createTestDirectory();
  }

  @After
  public void after() throws Exception {
    cleanupTestDirectory();
  }

  @Test
  public void testMetadataRefreshWithoutNewCommit() throws Exception {
    for (String tableName : testTables) {
      validateNoRefreshWithoutNewCommit(tableName);
    }
  }

  @Test
  public void testMetadataRefreshWithNewCommit() throws Exception {
    for (String tableName : testTables) {
      validateNoRefreshWithNewCommit(tableName);
    }
  }

  private void validateNoRefreshWithoutNewCommit(String tableName) throws Exception {
    String fullTableName = "dfs".concat(testRootPath.replace('/', '.').concat(tableName));

    final String refreshSql = "alter table " + fullTableName + " refresh metadata";
    runSQL(refreshSql);

    // verify metadata refresh NOT happened as there is No new commit
    String summaryMsg = String.format("Table '%s' read signature reviewed but source stated metadata is unchanged, no refresh occurred.", fullTableName);
    testRefreshMetadataQueryStatusSummary(refreshSql, true, summaryMsg);
  }

  private void validateNoRefreshWithNewCommit(String tableName) throws Exception {
    String testTablePath = testRootPath + "/" + tableName;
    DeltaTableIntegrationTestUtils testUtils = new DeltaTableIntegrationTestUtils(conf, testTablePath);
    long oldVersion = testUtils.getVersion();
    String fullTableName = "dfs".concat(testRootPath.replace('/', '.').concat(tableName));
    final String refreshSql = "alter table " + fullTableName + " refresh metadata";
    runSQL(refreshSql);

    // do a new commit
    testUtils.doAddFilesCommit();

    long newVersion = testUtils.getVersion();

    // verify that version number increased by 1 after the new commit
    assertEquals(oldVersion + 1, newVersion);

    // Run refresh metadata to verify metadata refresh happened as there is a new commit
    String summaryMsg = String.format("Metadata for table '%s' refreshed.", fullTableName);
    testRefreshMetadataQueryStatusSummary(refreshSql, true, summaryMsg);
  }

  private void testRefreshMetadataQueryStatusSummary(String query, boolean status, String summaryMsg) throws Exception {
    // Run the metadata refresh query and verify result message is correct.
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(status, summaryMsg)
        .build()
        .run();
  }

  private void createTestDirectory() throws Exception {
    // Copy the test delta table to the test root path
    conf = new Configuration();
    conf.set("fs.default.name", "local");
    fs = FileSystem.get(conf);
    if (fs.exists(p)) {
      fs.delete(p, true);
    }

    fs.mkdirs(p);
    for (String tableName : testTables) {
      copyTestTable(tableName);
    }
  }

  private void copyTestTable(String tableName) throws Exception {
    // Copy the test delta table to the test root path
    String destTablePath = testRootPath + "/" + tableName;
    String sourceTablePath = "deltalake/" + tableName;
    copyFromJar(sourceTablePath, java.nio.file.Paths.get(destTablePath));
  }

  private void cleanupTestDirectory() throws Exception {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
  }
}
