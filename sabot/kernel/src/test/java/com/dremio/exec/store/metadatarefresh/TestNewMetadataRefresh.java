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
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.setupLocalFS;
import static com.dremio.exec.store.metadatarefresh.RefreshDatasetTestUtils.verifyIcebergMetadata;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.server.SimpleJobRunner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;

public class TestNewMetadataRefresh extends BaseTestQuery {

  @Rule
  public final ExpectedException expectedEx = ExpectedException.none();

  private static FileSystem fs;
  static String testRootPath = "/tmp/metadatarefresh/";
  static String finalIcebergMetadataLocation;

  @BeforeClass
  public static void setUpJobRunner() throws Exception {
    SimpleJobRunner jobRunner = (query, userName, queryType) -> {
      try {
        runSQL(query); // queries we get here are inner 'refresh dataset' queries
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    };

    SABOT_NODE_RULE.register(new AbstractModule() {
      @Override
      protected void configure() {
        bind(SimpleJobRunner.class).toInstance(jobRunner);
      }
    });
    BaseTestQuery.setupDefaultTestCluster();
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
    copyFromJar("metadatarefresh/onlyFullWithPartition", java.nio.file.Paths.get(testRootPath + "/onlyFullWithPartition"));
    copyFromJar("metadatarefresh/incrementalRefresh", java.nio.file.Paths.get(testRootPath + "/incrementalRefresh"));
    copyFromJar("metadatarefresh/incrementalRefreshDeleteFile", java.nio.file.Paths.get(testRootPath + "/incrementalRefreshDeleteFile"));
    copyFromJar("metadatarefresh/incrementalRefreshDeletePartition", java.nio.file.Paths.get(testRootPath + "/incrementalRefreshDeletePartition"));
    copyFromJar("metadatarefresh/incrementRefreshAddingPartition", java.nio.file.Paths.get(testRootPath + "/incrementRefreshAddingPartition"));
    copyFromJar("metadatarefresh/concatSchemaFullRefresh", java.nio.file.Paths.get(testRootPath + "/concatSchemaFullRefresh"));
    copyFromJar("metadatarefresh/old2new", java.nio.file.Paths.get(testRootPath + "/old2new"));
    copyFromJar("metadatarefresh/partialRefreshAddingFile", java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingFile"));
    copyFromJar("metadatarefresh/partialRefreshAddingPartition", java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingPartition"));
    copyFromJar("metadatarefresh/partialRefreshAddingMultiLevelPartition", java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingMultiLevelPartition"));
    copyFromJar("metadatarefresh/failPartialRefreshForFirstQuery", java.nio.file.Paths.get(testRootPath + "/failPartialRefreshForFirstQuery"));
    copyFromJar("metadatarefresh/failPartialOnProvidingIncompletePartition", java.nio.file.Paths.get(testRootPath + "/failPartialOnProvidingIncompletePartition"));
    copyFromJar("metadatarefresh/failOnAddingPartition", java.nio.file.Paths.get(testRootPath + "/failOnAddingPartition"));
    copyFromJar("metadatarefresh/withHiddenFiles", java.nio.file.Paths.get(testRootPath + "/withHiddenFiles"));

    fs.mkdirs(new Path(testRootPath + "/path.with/special_chars/and space"));
    copyFromJar("metadatarefresh/onlyFull", java.nio.file.Paths.get(testRootPath + "/path.with/special_chars/and space"));
    copyFromJar("metadatarefresh/maxLeafCols", java.nio.file.Paths.get(testRootPath + "/maxLeafCols"));
  }


  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
    //TODO: also cleanup the KV store so that if 2 tests are working on the same dataset we don't get issues.
  }

  private void copy(java.nio.file.Path source, java.nio.file.Path dest) {
    try {
      Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private void copyFromJar(String sourceElement, final java.nio.file.Path target) throws URISyntaxException, IOException {
    URI resource = Resources.getResource(sourceElement).toURI();
    java.nio.file.Path srcDir = java.nio.file.Paths.get(resource);
    Files.walk(srcDir)
      .forEach(source -> copy(source, target.resolve(srcDir.relativize(source))));
  }

  @Test
  public void testFullRefreshWithoutPartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";

      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , new HashSet<>(), 2);

      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(ImmutableMap.of("`col1`", 135768));
      recordBuilder.add(ImmutableMap.of("`col1`", 135748));
      recordBuilder.add(ImmutableMap.of("`col1`", 135768));
      recordBuilder.add(ImmutableMap.of("`col1`", 135748));

      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefresh.onlyFull")
              .baselineColumns("col1")
              .baselineRecords(recordBuilder.build())
              .go();

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFull")
              .baselineColumns("cnt")
              .baselineValues(4L)
              .go();
    }
  }

  @Test
  public void testFullRefreshWithHiddenFiles() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.withHiddenFiles refresh metadata";

      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
              Types.NestedField.optional(1, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , new HashSet<>(), 2);
    }
  }

  @Test
  public void testRefreshPathWithSpecialChars() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      runSQL("alter table dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\" refresh metadata");
      Schema expectedSchema = new Schema(Arrays.asList(
              Types.NestedField.optional(1, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , new HashSet<>(), 2);

      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(ImmutableMap.of("`col1`", 135768));
      recordBuilder.add(ImmutableMap.of("`col1`", 135748));
      recordBuilder.add(ImmutableMap.of("`col1`", 135768));
      recordBuilder.add(ImmutableMap.of("`col1`", 135748));

      testBuilder()
              .ordered()
              .sqlQuery("select col1 from dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\"")
              .baselineColumns("col1")
              .baselineRecords(recordBuilder.build())
              .go();

      Thread.sleep(1001L);
      Files.copy(Paths.get(testRootPath + "/path.with/special_chars/and space/0.parquet"), Paths.get(testRootPath + "/path.with/special_chars/and space/3.parquet"));
      runSQL("alter table dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\" refresh metadata");

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\"")
              .baselineColumns("cnt")
              .baselineValues(6L)
              .go();

    }
  }

  @Test
  public void testFullRefreshWithPartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.onlyFullWithPartition refresh metadata";

      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.IntegerType()),
        Types.NestedField.optional(2, "dir0", new Types.StringType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , Sets.newHashSet("dir0"), 2);

      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(toMap("`col1`", 135768, "`dir0`", null));
      recordBuilder.add(toMap("`col1`", 135748, "`dir0`", null));
      recordBuilder.add(toMap("`col1`", 135768, "`dir0`", "level1"));
      recordBuilder.add(toMap("`col1`", 135748, "`dir0`", "level1"));

      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefresh.onlyFullWithPartition")
              .baselineColumns("col1", "dir0")
              .baselineRecords(recordBuilder.build())
              .go();

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFullWithPartition")
              .baselineColumns("cnt")
              .baselineValues(4L)
              .go();
    }
  }

  static Map<String, Object> toMap(String k1, Object v1, String k2, Object v2) {
    Map<String, Object> m = new HashMap<>(2);
    m.put(k1, v1);
    m.put(k2, v2);
    return m;
  }

  @Test
  public void testIncrementalRefreshAddingPartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.incrementRefreshAddingPartition refresh metadata";

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
              .sqlQuery("select * from dfs.tmp.metadatarefresh.incrementRefreshAddingPartition")
              .baselineColumns("col1", "dir0")
              .baselineRecords(recordBuilder.build())
              .go();

      Thread.sleep(1001L);
      //adding level2/float.parquet to the level2 new partition
      copyFromJar("metadatarefresh/level2", java.nio.file.Paths.get(testRootPath + "incrementRefreshAddingPartition/level2"));

      //this will do an incremental refresh now
      runSQL(sql);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.DoubleType())));

      //schema of col1 updated from int -> double and column id also changed
      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

      long modTime = new File("/tmp/metadatarefresh/incrementRefreshAddingPartition/level2/float.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),"file:/tmp/metadatarefresh/incrementRefreshAddingPartition/level2/float.parquet?version=" + modTime);
      final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
      recordBuilder1.add(toMap("`col1`", 135768d, "`dir0`", "level1"));
      recordBuilder1.add(toMap("`col1`", 135748d, "`dir0`", "level1"));
      Float val1 = 10.12f;
      Float val2 = 12.12f;
      recordBuilder1.add(toMap("`col1`", (double)val1, "`dir0`", "level2"));
      recordBuilder1.add(toMap("`col1`", (double)val2, "`dir0`", "level2"));

      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefresh.incrementRefreshAddingPartition")
              .baselineColumns("col1", "dir0")
              .baselineRecords(recordBuilder1.build())
              .go();

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.incrementRefreshAddingPartition")
              .baselineColumns("cnt")
              .baselineValues(4L)
              .go();
    }
  }

  @Test
  public void
  testIncrementalRefreshFileAdditionAndSchemaUpdate() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefresh refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema , Sets.newHashSet("dir0"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);
      //adding float.parquet to the testRoot for incremental
      copyFromJar("metadatarefresh/float.parquet", java.nio.file.Paths.get(testRootPath + "/incrementalRefresh/float.parquet"));

      //this will do an incremental refresh now
      runSQL(sql);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.DoubleType())));

      //schema of col1 updated from int -> double and column id also changed
      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema , Sets.newHashSet("dir0"), 2);

      long modTime = new File("/tmp/metadatarefresh/incrementalRefresh/float.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),  "file:/tmp/metadatarefresh/incrementalRefresh/float.parquet?version=" + modTime);
    }
  }

  @Test
  public void testIncrementalRefreshDeleteFile() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefreshDeleteFile refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.DoubleType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , new HashSet<>(), 2);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);

      //delete float.parquet
      File file = new File(testRootPath + "incrementalRefreshDeleteFile/float.parquet");
      long modTime = file.lastModified();
      file.delete();

      //this will do an incremental refresh now
      runSQL(sql);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 1, expectedSchema , new HashSet<>(), 1);

      //check float.parquet is was deleted
      Assert.assertEquals(RefreshDatasetTestUtils.getDeletedFilePaths(icebergTable).get(0), "file:/tmp/metadatarefresh/incrementalRefreshDeleteFile/float.parquet?version=" + modTime);
    }
  }

  @Test
  public void testFullRefreshConcatSchema() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.concatSchemaFullRefresh refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "date", new Types.StringType()),
        Types.NestedField.optional(2, "id1", new Types.LongType()),
        Types.NestedField.optional(3, "iso_code", new Types.StringType()),
        Types.NestedField.optional(4, "id2", new Types.LongType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , new HashSet<>(), 2);
    }
  }

  @Test
  public void testColumnCountTooLarge() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      test("ALTER SYSTEM SET \"store.plugin.max_metadata_leaf_columns\" = 2");
      final String sql = "alter table dfs.tmp.metadatarefresh.concatSchemaFullRefresh refresh metadata";

      //this will do a full refresh first
      expectedEx.expect(UserRemoteException.class);
      expectedEx.expectMessage("Number of fields in dataset exceeded the maximum number of fields of 2");
      runSQL(sql);

    } finally {
      test("ALTER SYSTEM RESET \"store.plugin.max_metadata_leaf_columns\"");
    }
  }

  @Test
  public void testIncrementalRefreshDeletePartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefreshDeletePartition refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "col1", new Types.DoubleType()),
        Types.NestedField.optional(2, "dir0", new Types.StringType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema , Sets.newHashSet("dir0"), 2);
      long modTime = new File("/tmp/metadatarefresh/incrementalRefreshDeletePartition/level1/float.parquet").lastModified();

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
      File file = new File(testRootPath + "incrementalRefreshDeletePartition/level1");
      FileUtils.deleteDirectory(file);

      //this will do an incremental refresh now
      runSQL(sql);

      //Refresh the same iceberg table again
      icebergTable.refresh();

      //schema of col1 updated from int -> double and column id also changed
      verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 1, expectedSchema , Sets.newHashSet("dir0"), 1);

      Assert.assertEquals(RefreshDatasetTestUtils.getDeletedFilePaths(icebergTable).get(0), "file:/tmp/metadatarefresh/incrementalRefreshDeletePartition/level1/float.parquet?version=" + modTime);
    }
  }

  @Test
  public void testOldToNewMetadataPromotion() throws Exception {
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));

    try (AutoCloseable c1 = disableUnlimitedSplitsSupportFlags()) {
      runSQL("alter table dfs.tmp.metadatarefresh.old2new refresh metadata force update");

      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefresh.old2new")
              .baselineColumns("col1")
              .baselineRecords(recordBuilder.build())
              .go();
    }

    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.old2new refresh metadata force update";
      runSQL(sql);
      testBuilder()
              .ordered()
              .sqlQuery("select * from dfs.tmp.metadatarefresh.old2new")
              .baselineColumns("col1")
              .baselineRecords(recordBuilder.build())
              .go();

      testBuilder()
              .ordered()
              .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.old2new")
              .baselineColumns("cnt")
              .baselineValues(4L)
              .go();
    }
  }


  @Test
  public void testPartialRefreshAfterAddingFile() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.partialRefreshAddingFile refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);
      //Adding file in lala partition
      copyFromJar("metadatarefresh/float.parquet", java.nio.file.Paths.get(testRootPath + "partialRefreshAddingFile/lala/float.parquet"));

      //Adding the same file in level2 partition
      copyFromJar("metadatarefresh/float.parquet", java.nio.file.Paths.get(testRootPath + "partialRefreshAddingFile/level2/float.parquet"));

      //this will do an partial refresh now.
      //Only one file addition should be shown as we are refreshing
      //only one partition
      runSQL(sql + " FOR PARTITIONS (\"dir0\" = \'lala\')");

      //Refresh the same iceberg table again
      icebergTable.refresh();

      expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.DoubleType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 3);

      long modTime = new File("/tmp/metadatarefresh/partialRefreshAddingFile/lala/float.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),"file:/tmp/metadatarefresh/partialRefreshAddingFile/lala/float.parquet?version=" + modTime);
    }
  }

  @Test
  public void testPartialRefreshAfterAddingPartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.partialRefreshAddingPartition refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);
      copyFromJar("metadatarefresh/level2", java.nio.file.Paths.get(testRootPath + "partialRefreshAddingPartition/level2"));

      //this will do an partial refresh now. We are refreshing old partition so nothing should
      //show up in the table
      runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'lala')");

      //Refresh the same iceberg table again
      icebergTable.refresh();


      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);


      //Doing a partial refresh again but now providing a newly added partition
      runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'level2')");

      //Refresh the same iceberg table again
      icebergTable.refresh();

      expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "col1", new Types.DoubleType())));

      //Now the table should change and show newly added files
      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);
      long modTime = new File("/tmp/metadatarefresh/partialRefreshAddingPartition/level2/float.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),"file:/tmp/metadatarefresh/partialRefreshAddingPartition/level2/float.parquet?version=" + modTime);
    }
  }

  @Test
  public void testPartialRefreshShouldNotRefreshRecursively() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.partialRefreshAddingMultiLevelPartition refresh metadata";

      //this will do a full refresh first
      runSQL(sql);

      Schema expectedSchema = new Schema(Arrays.asList(
        Types.NestedField.optional(1, "dir0", new Types.StringType()),
        Types.NestedField.optional(2, "dir1", new Types.StringType()),
        Types.NestedField.optional(3, "col1", new Types.IntegerType())));

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "dir1"), 1);

      Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

      Thread.sleep(1001L);

      //adding a file in level1
      copyFromJar("metadatarefresh/int.parquet", java.nio.file.Paths.get(testRootPath + "partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet"));

      //adding a file in level2
      copyFromJar("metadatarefresh/int.parquet", java.nio.file.Paths.get(testRootPath + "partialRefreshAddingMultiLevelPartition/level1/level2/intLevel2.parquet"));


      //this will do an partial refresh now. We are refreshing only
      //level1. So file added in level2 (inside level1) should not
      //show up.
      runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'level1', \"dir1\" = null)");

      //Refresh the same iceberg table again
      icebergTable.refresh();

      verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "dir1"), 2);

      long modTime = new File("/tmp/metadatarefresh/partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet").lastModified();
      Assert.assertEquals(RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),"file:/tmp/metadatarefresh/partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet?version=" + modTime);
    }
  }

  @Test
  public void testPartialRefreshFailureErrorOnFirstQuery() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.failPartialRefreshForFirstQuery refresh metadata for partitions (\"dir0\" = 'lala');";

      //Should throw an error. First query cannot be a partial refresh.
      expectedEx.expectMessage("Selective refresh is not allowed on unknown datasets. Please run full refresh on dfs.tmp.metadatarefresh.failPartialRefreshForFirstQuery");
      runSQL(sql);
    }
  }

  @Test
  public void testPartialRefreshFailureOnProvidingIncompletePartitions() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.failPartialOnProvidingIncompletePartition refresh metadata";

      //First a full refresh
      runSQL(sql);

      Thread.sleep(1001L);
      // without modifying table, read signature check skips refresh
      copyFromJar("metadatarefresh/failPartialOnProvidingIncompletePartition/level1/level2/int.parquet",
        java.nio.file.Paths.get(testRootPath + "failPartialOnProvidingIncompletePartition/int.parquet"));

      //This should error out as we are providing only on partition
      expectedEx.expectMessage("Refresh dataset command must include all partitions.");
      runSQL(sql + " for partitions (\"dir0\" = 'lala')");
    }
  }

  @Test
  public void testRefreshFailureOnAddingPartition() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String sql = "alter table dfs.tmp.metadatarefresh.failOnAddingPartition refresh metadata";

      //First a full refresh
      runSQL(sql);

      Thread.sleep(1001L);
      copyFromJar("metadatarefresh/level2", java.nio.file.Paths.get(testRootPath + "failOnAddingPartition/level1/level2"));

      //This should error out as we have added a partition
      expectedEx.expectMessage("Addition of a new level dir is not allowed in incremental refresh. Please forget and promote the table again.");
      runSQL(sql);
    }
  }

  @Test
  public void testCreateEmptyIcebergTable() throws Exception {
    String tblName = "emptyIcebergTable";
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags();
         AutoCloseable c2 = enableIcebergTables() /* to create iceberg tables with ctas */) {
      final String createTableQuery = String.format("CREATE TABLE %s.%s(id int, code int)", "dfs_test", tblName);
      test(createTableQuery);
      java.nio.file.Path metadataFolder = Paths.get(getDfsTestTmpSchemaLocation(), tblName, "metadata");
      Assert.assertTrue(metadataFolder.toFile().exists());
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCreateAndSelectOnIcebergTable() throws Exception {
    String tblName = "newIcebergTable";
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags();
         AutoCloseable c2 = enableIcebergTables() /* to create iceberg tables with ctas */) {
      final String createTableQuery = String.format("create table %s.%s(id, code) as values(10, 30)", "dfs_test", tblName);
      test(createTableQuery);

      java.nio.file.Path metadataFolder = Paths.get(getDfsTestTmpSchemaLocation(), tblName, "metadata");
      Assert.assertTrue(metadataFolder.toFile().exists());

      Thread.sleep(1001L);

      testBuilder()
        .sqlQuery(String.format("select * from %s.%s", "dfs_test", tblName))
        .unOrdered()
        .baselineColumns("id", "code")
        .baselineValues(10L, 30L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCreateEmptyNonIcebergTable() throws Exception {
    String tblName = "emptyTable";
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String createTableQuery = String.format("CREATE TABLE %s.%s(id int, code int)", "dfs_test", tblName);

      expectedEx.expect(UserRemoteException.class);
      expectedEx.expectMessage("does not support CREATE TABLE");
      test(createTableQuery);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCreateAndSelectOnNonIcebergTable() throws Exception {
    String tblName = "newTable";
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      final String createTableQuery = String.format("create table %s.%s(id, code) as values(10, 30)", "dfs_test", tblName);
      test(createTableQuery);

      java.nio.file.Path metadataFolder = Paths.get(getDfsTestTmpSchemaLocation(), tblName, "metadata");
      Assert.assertFalse(metadataFolder.toFile().exists()); // not an iceberg table since 'dremio.iceberg.ctas.enabled' is false

      Thread.sleep(1001L);

      testBuilder()
        .sqlQuery(String.format("select * from %s.%s", "dfs_test", tblName))
        .unOrdered()
        .baselineColumns("id", "code")
        .baselineValues(10L, 30L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testDataFileOverride() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      File dir1 = new File(testRootPath + "testDataFileOverride");

      if (!dir1.exists()) {
        dir1.mkdir();
      }
      File intParquet = new File(Resources.getResource("metadatarefresh/int.parquet").getFile());
      File floatParquet = new File(Resources.getResource("metadatarefresh/float.parquet").getFile());
      File commonParquet = new File(dir1.getAbsolutePath() + "/common_file_name.parquet");

      FileUtils.copyFile(intParquet, commonParquet, false);
      Stopwatch stopwatch = Stopwatch.createStarted();

      runSQL("alter table dfs.tmp.metadatarefresh.testDataFileOverride refresh metadata");


      final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
      recordBuilder.add(ImmutableMap.of("`col1`", 135768));
      recordBuilder.add(ImmutableMap.of("`col1`", 135748));

      testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefresh.testDataFileOverride")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder.build())
        .go();

      while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < 1000L) {
        Thread.currentThread().sleep(1000L);
      }

      copy(floatParquet.toPath(), commonParquet.toPath());

      runSQL("alter table dfs.tmp.metadatarefresh.testDataFileOverride refresh metadata");

      final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
      Float val1 = 10.12f;
      Float val2 = 12.12f;
      recordBuilder1.add(ImmutableMap.of("`col1`", (double) val1));
      recordBuilder1.add(ImmutableMap.of("`col1`", (double) val2));
      testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefresh.testDataFileOverride")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder1.build())
        .go();
      stopwatch.stop();
    }
  }

  @Test
  public void testRefreshMaxColsExceeded() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.maxLeafCols refresh metadata";

    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags();
         AutoCloseable c2 = setMaxLeafColumns(1000)) {
      runSQL(sql);
    } finally {
      runSQL("alter table dfs.tmp.metadatarefresh.maxLeafCols forget metadata");
    }

    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      runSQL(sql);
      fail("Expecting exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Number of fields in dataset exceeded the maximum number of fields of 800"));
    }
  }

  @Test
  public void testRefreshAndSelectOnNestedStructParquet() throws Exception {
    try (AutoCloseable c1 = enableUnlimitedSplitsSupportFlags()) {
      File dir = new File(testRootPath + "testRefreshAndSelectOnNestedStructParquet");

      if (!dir.exists()) {
        dir.mkdir();
      }
      File nestedParquet = new File(Resources.getResource("metadatarefresh/nested_struct.impala.parquet").getFile());
      FileUtils.copyFileToDirectory(nestedParquet, dir, false);
      runSQL("alter table dfs.tmp.metadatarefresh.testRefreshAndSelectOnNestedStructParquet refresh metadata");
      runSQL("select * from dfs.tmp.metadatarefresh.testRefreshAndSelectOnNestedStructParquet");
    }
  }
}
