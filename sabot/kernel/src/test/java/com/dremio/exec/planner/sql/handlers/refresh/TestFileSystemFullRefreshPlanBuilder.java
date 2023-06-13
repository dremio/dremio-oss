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
package com.dremio.exec.planner.sql.handlers.refresh;

import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;

import java.nio.file.Paths;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestFileSystemFullRefreshPlanBuilder extends BaseTestQuery {
  private static FileSystem fs;
  static String testRootPath = "/tmp/metadatarefreshfailures/";
  private static String finalIcebergMetadataLocation;

  @Before
  public void initFs() throws Exception {
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
    fs = setupLocalFS();
    Path p = new Path(testRootPath);

    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    fs.mkdirs(p);

    copyFromJar("metadatarefresh/incrementRefreshAddingPartition", Paths.get(testRootPath + "/incrementRefreshAddingPartition"));
    BaseTestQuery.setEnableReAttempts(true);
    BaseTestQuery.disablePlanCache();
  }

  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
    //TODO: also cleanup the KV store so that if 2 tests are working on the same dataset we don't get issues.
  }

  @AfterClass
  public static void cleanUpLocation() throws Exception {
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
  }

  @Test
  public void testIncrementalRefreshFailureAfterIcebergCommit() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      String sql = "refresh dataset dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition";

      // this will do a full refresh first
      runSQL(sql);

      // now run with incorrect casing to check it still passes
      sql = "refresh dataset dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPARTITION";
      runSQL(sql);
    }
  }
}
