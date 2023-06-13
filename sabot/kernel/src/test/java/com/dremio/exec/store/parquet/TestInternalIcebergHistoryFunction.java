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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Test class for iceberg snapshot functions select * from table(table_snapshot('table'))
 */
public class TestInternalIcebergHistoryFunction extends BaseTestQuery {

  private static FileSystem fs;
  static String testRootPath = "/tmp/metadatarefresh/";
  static String finalIcebergMetadataLocation;

  @BeforeClass
  public static void setupIcebergMetadataLocation() {
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
  }



  @Before
  public void initFs() throws Exception {
    fs = setupLocalFS();
    Path p = new Path(testRootPath);

    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    fs.mkdirs(p);

    copyFromJar("metadatarefresh/onlyFull", java.nio.file.Paths.get(testRootPath + "/onlyFull"));


  }


  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
    //TODO: also cleanup the KV store so that if 2 tests are working on the same dataset we don't get issues.
  }

  @Test
  public void testHistoryFunction() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

      runSQL(sql);


      Table icebergTable = loadIcebergTable(finalIcebergMetadataLocation);
      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(ImmutableMap.of("`snapshot_id`", icebergTable.history().get(0).snapshotId()));




      testBuilder()
        .ordered()
        .sqlQuery("select count(*) as cnt from table(table_history('dfs.tmp.metadatarefresh.onlyFull'))")
        .baselineColumns("cnt")
        .baselineValues(Long.valueOf(icebergTable.history().size()))
        .go();


      testBuilder()
        .ordered()
        .sqlQuery("select snapshot_id from table(table_history('dfs.tmp.metadatarefresh.onlyFull'))")
        .baselineColumns("snapshot_id")
        .baselineRecords(recordBuilder.build())
        .go();

    }


  }
  private static Table loadIcebergTable(String tableFolderPath) {
    File tableFolder = new File(tableFolderPath);
    Assert.assertTrue(tableFolder.exists());
    File tablePath = tableFolder.listFiles()[0];
    return BaseTestQuery.getIcebergTable(tablePath);
  }
}
