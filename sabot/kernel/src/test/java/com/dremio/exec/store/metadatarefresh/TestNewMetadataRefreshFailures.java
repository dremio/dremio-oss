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
package com.dremio.exec.store.metadatarefresh;

import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.fsDelete;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.verifyIcebergMetadata;
import static com.dremio.exec.store.metadatarefresh.TestNewMetadataRefresh.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.iceberg.model.IncrementalMetadataRefreshCommitter;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class TestNewMetadataRefreshFailures extends BaseTestQuery {

  private static FileSystem fs;
  static String testRootPath = "/tmp/metadatarefreshfailures/";
  private static String finalIcebergMetadataLocation;
  FileFilter manifestFileFilter = pathname -> pathname.getName().toUpperCase().endsWith("avro".toUpperCase());

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
      final String sql = "refresh dataset dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema , Sets.newHashSet("dir0"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(toMap("`col1`", 135768, "`dir0`", "level1"));
      recordBuilder.add(toMap("`col1`", 135748, "`dir0`", "level1"));

      testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition")
        .baselineColumns("col1", "dir0")
        .baselineRecords(recordBuilder.build())
        .go();

      Thread.sleep(1001L);
      //adding level2/float.parquet to the level2 new partition
      copyFromJar("metadatarefresh/level2", Paths.get(testRootPath + "incrementRefreshAddingPartition/level2"));

      final String errorAfterIcebergCommit = Controls.newBuilder()
        .addException(IncrementalMetadataRefreshCommitter.class, IncrementalMetadataRefreshCommitter.INJECTOR_AFTER_ICEBERG_COMMIT_ERROR,
          UnsupportedOperationException.class)
        .build();
      ControlsInjectionUtil.setControls(client, errorAfterIcebergCommit);
      try {
        //this will fail after iceberg commit
        runSQL(sql);
        Assert.fail("expecting injected exception to be thrown");
      } catch (Exception ex) {}

      //Refresh the same iceberg table again
      icebergTable.refresh();

      NamespaceService namespaceService = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
      NamespaceKey namespaceKey = new NamespaceKey(Arrays.asList("dfs", "tmp", "metadatarefreshfailures", "incrementRefreshAddingPartition"));
      DatasetConfig dataset = namespaceService.getDataset(namespaceKey);
      long snapshotAfterFailedCatalogUpdate = icebergTable.currentSnapshot().snapshotId();
      assertNotEquals(snapshotAfterFailedCatalogUpdate, (long)dataset.getPhysicalDataset().getIcebergMetadata().getSnapshotId());
      expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.DoubleType())));

      //schema of col1 updated from int -> double and column id also changed
      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

      long modTime = new File("/tmp/metadatarefreshfailures/incrementRefreshAddingPartition/level2/float.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),"file:/tmp/metadatarefreshfailures/incrementRefreshAddingPartition/level2/float.parquet?version=" + modTime);

      // we'd still get same results as before
      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition")
              .baselineColumns("col1", "dir0")
              .baselineRecords(recordBuilder.build())
              .go();

      // this refresh should attempt repair
      runSQL(sql);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      // no change in iceberg snapshot
      assertEquals(snapshotAfterFailedCatalogUpdate, icebergTable.currentSnapshot().snapshotId());

      final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
      recordBuilder1.add(toMap("`col1`", 135768d, "`dir0`", "level1"));
      recordBuilder1.add(toMap("`col1`", 135748d, "`dir0`", "level1"));
      Float val1 = 10.12f;
      Float val2 = 12.12f;
      recordBuilder1.add(toMap("`col1`", (double)val1, "`dir0`", "level2"));
      recordBuilder1.add(toMap("`col1`", (double)val2, "`dir0`", "level2"));


      // select would now give us updated results
      testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition")
        .baselineColumns("col1", "dir0")
        .baselineRecords(recordBuilder1.build())
        .go();

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition")
              .baselineColumns("cnt")
              .baselineValues(4L)
              .go();
    }
  }

  @Test
  public void testIncrementalRefreshFailureCleanupAfterIcebergCommit() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "refresh dataset dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema , Sets.newHashSet("dir0"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(toMap("`col1`", 135768, "`dir0`", "level1"));
      recordBuilder.add(toMap("`col1`", 135748, "`dir0`", "level1"));
      com.dremio.io.file.Path location = com.dremio.io.file.Path.of(icebergTable.location());
      String pathString  = com.dremio.io.file.Path.getContainerSpecificRelativePath(location);
      File metadataFolder = new File(pathString, "metadata");
      File[] manifestFiles1 = metadataFolder.listFiles(manifestFileFilter);

      testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefreshfailures.incrementRefreshAddingPartition")
        .baselineColumns("col1", "dir0")
        .baselineRecords(recordBuilder.build())
        .go();

      Thread.sleep(1001L);
      //adding level2/float.parquet to the level2 new partition
      copyFromJar("metadatarefresh/level2", Paths.get(testRootPath + "incrementRefreshAddingPartition/level2"));

      final String errorAfterIcebergCommit = Controls.newBuilder()
        .addException(WriterCommitterOperator.class, WriterCommitterOperator.INJECTOR_AFTER_NO_MORETO_CONSUME_ERROR,
          UnsupportedOperationException.class)
        .build();
      ControlsInjectionUtil.setControls(client, errorAfterIcebergCommit);
      try {
        //this will fail after iceberg commit
        runSQL(sql);
        Assert.fail("expecting injected exception to be thrown");
      } catch (Exception ex) {}

      File[] manifestFiles2 = metadataFolder.listFiles(manifestFileFilter);
      Assert.assertEquals(manifestFiles2.length, manifestFiles1.length);

      //Refresh the same iceberg table again
      icebergTable.refresh();
    }
  }

}
