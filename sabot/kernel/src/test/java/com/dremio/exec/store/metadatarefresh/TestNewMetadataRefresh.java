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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.UserExceptionAssert;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.Test;

public class TestNewMetadataRefresh extends BaseTestQuery {

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
    copyFromJar("metadatarefresh/onlyFull", java.nio.file.Paths.get(testRootPath + "/onlyFull2"));
    copyFromJar(
        "metadatarefresh/onlyFullWithPartition",
        java.nio.file.Paths.get(testRootPath + "/onlyFullWithPartition"));
    copyFromJar(
        "metadatarefresh/onlyFullWithPartitionInference",
        java.nio.file.Paths.get(testRootPath + "/onlyFullWithPartitionInference"));
    copyFromJar(
        "metadatarefresh/onlyFullWithPartitionInference",
        java.nio.file.Paths.get(testRootPath + "/refreshWithPartitionInference"));
    copyFromJar(
        "metadatarefresh/onlyFullWithPartitionInferenceLevel2",
        java.nio.file.Paths.get(testRootPath + "/onlyFullWithPartitionInferenceLevel2"));
    copyFromJar(
        "metadatarefresh/incrementalRefresh",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefresh"));
    copyFromJar(
        "metadatarefresh/incrementalRefreshDeleteFile",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefreshDeleteFile"));
    copyFromJar(
        "metadatarefresh/incrementalRefreshDeletePartition",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefreshDeletePartition"));
    copyFromJar(
        "metadatarefresh/incrementalRefreshDeletePartitionWithInference",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefreshDeletePartitionWithInference"));
    copyFromJar(
        "metadatarefresh/incrementRefreshAddingPartition",
        java.nio.file.Paths.get(testRootPath + "/incrementRefreshAddingPartition"));
    copyFromJar(
        "metadatarefresh/incrementRefreshAddingPartitionWithInference",
        java.nio.file.Paths.get(testRootPath + "/incrementRefreshAddingPartitionWithInference"));
    copyFromJar(
        "metadatarefresh/concatSchemaFullRefresh",
        java.nio.file.Paths.get(testRootPath + "/concatSchemaFullRefresh"));
    copyFromJar("metadatarefresh/old2new", java.nio.file.Paths.get(testRootPath + "/old2new"));
    copyFromJar(
        "metadatarefresh/partialRefreshAddingFile",
        java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingFile"));
    copyFromJar(
        "metadatarefresh/partialRefreshAddingPartition",
        java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingPartition"));
    copyFromJar(
        "metadatarefresh/partialRefreshAddingMultiLevelPartition",
        java.nio.file.Paths.get(testRootPath + "/partialRefreshAddingMultiLevelPartition"));
    copyFromJar(
        "metadatarefresh/failPartialRefreshForFirstQuery",
        java.nio.file.Paths.get(testRootPath + "/failPartialRefreshForFirstQuery"));
    copyFromJar(
        "metadatarefresh/failPartialOnProvidingIncompletePartition",
        java.nio.file.Paths.get(testRootPath + "/failPartialOnProvidingIncompletePartition"));
    copyFromJar(
        "metadatarefresh/failOnAddingPartition",
        java.nio.file.Paths.get(testRootPath + "/failOnAddingPartition"));
    copyFromJar(
        "metadatarefresh/withHiddenFiles",
        java.nio.file.Paths.get(testRootPath + "/withHiddenFiles"));

    fs.mkdirs(new Path(testRootPath + "/path.with/special_chars/and space"));
    copyFromJar(
        "metadatarefresh/onlyFull",
        java.nio.file.Paths.get(testRootPath + "/path.with/special_chars/and space"));
    copyFromJar(
        "metadatarefresh/maxLeafCols", java.nio.file.Paths.get(testRootPath + "/maxLeafCols"));
    copyFromJar(
        "metadatarefresh/multiLevel", java.nio.file.Paths.get(testRootPath + "/multiLevel"));
  }

  @After
  public void cleanup() throws Exception {
    Path p = new Path(testRootPath);
    fs.delete(p, true);
    fsDelete(fs, new Path(finalIcebergMetadataLocation));
    // TODO: also cleanup the KV store so that if 2 tests are working on the same dataset we don't
    // get issues.
  }

  @Test
  public void testConcurrentFullRefresh() throws Exception {
    int noOfConcurrentRequests = 5;
    ExecutorService executors = Executors.newFixedThreadPool(noOfConcurrentRequests);
    List<Future<String>> futures = new ArrayList<>();
    for (int i = 0; i < noOfConcurrentRequests; i++) {
      futures.add(executors.submit(this::runRefreshQuery));
    }
    for (int i = 0; i < noOfConcurrentRequests; i++) {
      while (!futures.get(i).isDone()) {
        Thread.sleep(1000);
      }
      Assert.assertTrue(futures.get(i).get().isEmpty());
    }
  }

  private String runRefreshQuery() {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull refresh metadata";
    try {
      runSQL(sql);
    } catch (Exception ex) {
      if (ex instanceof ConcurrentModificationException
          || ex instanceof StatusRuntimeException
          || ex instanceof UserException) {
        return ex.getMessage();
      }
    }
    return "";
  }

  @Test
  public void testFullRefreshWithoutPartition() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFull2 refresh metadata";

    runSQL(sql);

    Schema expectedSchema =
        new Schema(Arrays.asList(Types.NestedField.optional(1, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, new HashSet<>(), 2);

    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));

    testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefresh.onlyFull2")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder.build())
        .go();

    testBuilder()
        .ordered()
        .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFull2")
        .baselineColumns("cnt")
        .baselineValues(4L)
        .go();
  }

  @Test
  public void testFullRefreshWithHiddenFiles() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.withHiddenFiles refresh metadata";

    runSQL(sql);

    Schema expectedSchema =
        new Schema(Arrays.asList(Types.NestedField.optional(1, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, new HashSet<>(), 2);
  }

  @Test
  public void testRefreshPathWithSpecialChars() throws Exception {
    runSQL(
        "alter table dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\" refresh metadata");
    Schema expectedSchema =
        new Schema(Arrays.asList(Types.NestedField.optional(1, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, new HashSet<>(), 2);

    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));

    testBuilder()
        .ordered()
        .sqlQuery(
            "select col1 from dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\"")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder.build())
        .go();

    Thread.sleep(1001L);
    Files.copy(
        Paths.get(testRootPath + "/path.with/special_chars/and space/0.parquet"),
        Paths.get(testRootPath + "/path.with/special_chars/and space/3.parquet"));
    runSQL(
        "alter table dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\" refresh metadata");

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\"")
        .baselineColumns("cnt")
        .baselineValues(6L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(col1) as cnt from dfs.tmp.metadatarefresh.\"path.with\".special_chars.\"and space\"")
        .baselineColumns("cnt")
        .baselineValues(6L)
        .go();
  }

  @Test
  public void testFullRefreshWithPartition() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.onlyFullWithPartition refresh metadata";

    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.IntegerType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

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

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFullWithPartition where dir0 = null")
        .baselineColumns("cnt")
        .baselineValues(0L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFullWithPartition where not(dir0 = null)")
        .baselineColumns("cnt")
        .baselineValues(0L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFullWithPartition where dir0 is null")
        .baselineColumns("cnt")
        .baselineValues(2L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.onlyFullWithPartition where dir0 is not null")
        .baselineColumns("cnt")
        .baselineValues(2L)
        .go();
  }

  @Test
  public void testFullRefreshWithPartitionInference() throws Exception {
    final String sql =
        "alter table dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInference refresh metadata";

    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.IntegerType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType()),
                Types.NestedField.optional(3, "level1", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 3, 0, expectedSchema, Sets.newHashSet("dir0", "level1"), 3);

    testBuilder()
        .ordered()
        .sqlQuery(
            "select * from dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInference")
        .baselineColumns("col1", "dir0", "level1")
        .baselineValues(135768, "level1=1", "1")
        .baselineValues(135748, "level1=1", "1")
        .baselineValues(135768, "level1=2", "2")
        .baselineValues(135748, "level1=2", "2")
        .baselineValues(135768, "level1=__HIVE_DEFAULT_PARTITION__", null)
        .baselineValues(135748, "level1=__HIVE_DEFAULT_PARTITION__", null)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInference")
        .baselineColumns("cnt")
        .baselineValues(6L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInference where level1 = '1'")
        .baselineColumns("cnt")
        .baselineValues(2L)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInference where dir0 = 'level1=1'")
        .baselineColumns("cnt")
        .baselineValues(2L)
        .go();
  }

  static Map<String, Object> toMap(String k1, Object v1, String k2, Object v2) {
    Map<String, Object> m = new HashMap<>(2);
    m.put(k1, v1);
    m.put(k2, v2);
    return m;
  }

  @Test
  public void testIncrementalRefreshAddingPartition() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.incrementRefreshAddingPartition refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);

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
    // adding level2/float.parquet to the level2 new partition
    copyFromJar(
        "metadatarefresh/level2",
        java.nio.file.Paths.get(testRootPath + "incrementRefreshAddingPartition/level2"));

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.DoubleType())));

    // schema of col1 updated from int -> double and column id also changed
    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

    long modTime =
        new File("/tmp/metadatarefresh/incrementRefreshAddingPartition/level2/float.parquet")
            .lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementRefreshAddingPartition/level2/float.parquet?version="
            + modTime);
    final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
    recordBuilder1.add(toMap("`col1`", 135768d, "`dir0`", "level1"));
    recordBuilder1.add(toMap("`col1`", 135748d, "`dir0`", "level1"));
    Float val1 = 10.12f;
    Float val2 = 12.12f;
    recordBuilder1.add(toMap("`col1`", (double) val1, "`dir0`", "level2"));
    recordBuilder1.add(toMap("`col1`", (double) val2, "`dir0`", "level2"));

    testBuilder()
        .ordered()
        .sqlQuery("select * from dfs.tmp.metadatarefresh.incrementRefreshAddingPartition")
        .baselineColumns("col1", "dir0")
        .baselineRecords(recordBuilder1.build())
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs.tmp.metadatarefresh.incrementRefreshAddingPartition")
        .baselineColumns("cnt")
        .baselineValues(4L)
        .go();
  }

  @Test
  public void testIncrementalRefreshAddingPartitionWithInference() throws Exception {
    final String sql =
        "alter table dfs_partition_inference.tmp.metadatarefresh.incrementRefreshAddingPartitionWithInference refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.IntegerType()),
                Types.NestedField.optional(3, "level1", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "level1"), 1);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", 135768),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=1"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "1"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    recordBuilder.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", 135748),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=1"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "1"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    testBuilder()
        .ordered()
        .sqlQuery(
            "select * from dfs_partition_inference.tmp.metadatarefresh.incrementRefreshAddingPartitionWithInference")
        .baselineColumns("col1", "dir0", "level1")
        .baselineRecords(recordBuilder.build())
        .go();

    // adding level1=2/float.parquet to the level2 new partition
    copyFromJar(
        "metadatarefresh/level1=2",
        java.nio.file.Paths.get(
            testRootPath + "incrementRefreshAddingPartitionWithInference/level1=2"));

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.DoubleType()),
                Types.NestedField.optional(3, "level1", new Types.StringType())));

    // schema of col1 updated from int -> double
    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "level1"), 2);

    long modTime =
        new File(
                "/tmp/metadatarefresh/incrementRefreshAddingPartitionWithInference/level1=2/float.parquet")
            .lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementRefreshAddingPartitionWithInference/level1=2/float.parquet?version="
            + modTime);
    final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
    recordBuilder1.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", 135768d),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=1"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "1"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    recordBuilder1.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", 135748d),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=1"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "1"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    Float val1 = 10.12f;
    Float val2 = 12.12f;
    recordBuilder1.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", (double) val1),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=2"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "2"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    recordBuilder1.add(
        Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("`col1`", (double) val2),
                new AbstractMap.SimpleImmutableEntry<>("`dir0`", "level1=2"),
                new AbstractMap.SimpleImmutableEntry<>("`level1`", "2"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    testBuilder()
        .ordered()
        .sqlQuery(
            "select * from dfs_partition_inference.tmp.metadatarefresh.incrementRefreshAddingPartitionWithInference")
        .baselineColumns("col1", "dir0", "level1")
        .baselineRecords(recordBuilder1.build())
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.incrementRefreshAddingPartitionWithInference")
        .baselineColumns("cnt")
        .baselineValues(4L)
        .go();
  }

  @Test
  public void testIncrementalRefreshFileAdditionAndSchemaUpdate() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.incrementalRefresh refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

    Thread.sleep(1001L);
    // adding float.parquet to the testRoot for incremental
    copyFromJar(
        "metadatarefresh/float.parquet",
        java.nio.file.Paths.get(testRootPath + "/incrementalRefresh/float.parquet"));

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.DoubleType())));

    // schema of col1 updated from int -> double and column id also changed
    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

    long modTime = new File("/tmp/metadatarefresh/incrementalRefresh/float.parquet").lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementalRefresh/float.parquet?version=" + modTime);
  }

  @Test
  public void testIncrementalRefreshDeleteFile() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.incrementalRefreshDeleteFile refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(Arrays.asList(Types.NestedField.optional(1, "col1", new Types.DoubleType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, new HashSet<>(), 2);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

    Thread.sleep(1001L);

    // delete float.parquet
    File file = new File(testRootPath + "incrementalRefreshDeleteFile/float.parquet");
    long modTime = file.lastModified();
    file.delete();

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    verifyIcebergMetadata(finalIcebergMetadataLocation, 0, 1, expectedSchema, new HashSet<>(), 1);

    // check float.parquet is was deleted
    Assert.assertEquals(
        RefreshDatasetTestUtils.getDeletedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementalRefreshDeleteFile/float.parquet?version=" + modTime);
  }

  @Test
  public void testFullRefreshConcatSchema() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.concatSchemaFullRefresh refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "date", new Types.StringType()),
                Types.NestedField.optional(2, "id1", new Types.LongType()),
                Types.NestedField.optional(3, "iso_code", new Types.StringType()),
                Types.NestedField.optional(4, "id2", new Types.LongType())));

    verifyIcebergMetadata(finalIcebergMetadataLocation, 2, 0, expectedSchema, new HashSet<>(), 2);
  }

  @Test
  public void testSecondLevelPromotion() throws Exception {
    test("select * from dfs.tmp.metadatarefresh.\"incrementRefreshAddingPartition.level1\"");

    Schema expectedSchema =
        new Schema(
            Collections.singletonList(
                Types.NestedField.optional(1, "col1", new Types.IntegerType())));
    // verify v2 execution
    verifyIcebergMetadata(finalIcebergMetadataLocation, 1, 0, expectedSchema, new HashSet<>(), 1);
  }

  @Test
  public void testColumnCountTooLarge() throws Exception {
    try {
      test("ALTER SYSTEM SET \"store.plugin.max_metadata_leaf_columns\" = 2");
      final String sql =
          "ALTER TABLE dfs.tmp.metadatarefresh.concatSchemaFullRefresh REFRESH METADATA";

      // this will do a full refresh first
      assertThatThrownBy(() -> runSQL(sql))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "Number of fields in dataset exceeded the maximum number of fields of 2");

    } finally {
      test("ALTER SYSTEM RESET \"store.plugin.max_metadata_leaf_columns\"");
    }
  }

  @Test
  public void testIncrementalRefreshDeletePartition() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.incrementalRefreshDeletePartition refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.DoubleType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("dir0"), 2);
    long modTime =
        new File("/tmp/metadatarefresh/incrementalRefreshDeletePartition/level1/float.parquet")
            .lastModified();

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
    File file = new File(testRootPath + "incrementalRefreshDeletePartition/level1");
    FileUtils.deleteDirectory(file);

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    // schema of col1 updated from int -> double and column id also changed
    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 0, 1, expectedSchema, Sets.newHashSet("dir0"), 1);

    Assert.assertEquals(
        RefreshDatasetTestUtils.getDeletedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementalRefreshDeletePartition/level1/float.parquet?version="
            + modTime);
  }

  @Test
  public void testIncrementalRefreshDeletePartitionWithInference() throws Exception {
    final String sql =
        "alter table dfs_partition_inference.tmp.metadatarefresh.incrementalRefreshDeletePartitionWithInference refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.DoubleType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType()),
                Types.NestedField.optional(3, "level1", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("dir0", "level1"), 2);

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.incrementalRefreshDeletePartitionWithInference")
        .baselineColumns("cnt")
        .baselineValues(4L)
        .go();

    long modTime =
        new File(
                "/tmp/metadatarefresh/incrementalRefreshDeletePartitionWithInference/level1=2/float.parquet")
            .lastModified();

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);
    File file = new File(testRootPath + "incrementalRefreshDeletePartitionWithInference/level1=2");
    FileUtils.deleteDirectory(file);

    // this will do an incremental refresh now
    runSQL(sql);

    // Refresh the same iceberg table again
    icebergTable.refresh();

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 0, 1, expectedSchema, Sets.newHashSet("dir0", "level1"), 1);

    testBuilder()
        .ordered()
        .sqlQuery(
            "select count(*) as cnt from dfs_partition_inference.tmp.metadatarefresh.incrementalRefreshDeletePartitionWithInference")
        .baselineColumns("cnt")
        .baselineValues(2L)
        .go();

    Assert.assertEquals(
        RefreshDatasetTestUtils.getDeletedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/incrementalRefreshDeletePartitionWithInference/level1=2/float.parquet?version="
            + modTime);
  }

  @Test
  public void testOldToNewMetadataPromotion() throws Exception {
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));
    recordBuilder.add(ImmutableMap.of("`col1`", 135768));
    recordBuilder.add(ImmutableMap.of("`col1`", 135748));

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

  @Test
  public void testPartialRefreshAfterAddingFile() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.partialRefreshAddingFile refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 2, 0, expectedSchema, Sets.newHashSet("dir0"), 2);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

    Thread.sleep(1001L);
    // Adding file in lala partition
    copyFromJar(
        "metadatarefresh/float.parquet",
        java.nio.file.Paths.get(testRootPath + "partialRefreshAddingFile/lala/float.parquet"));

    // Adding the same file in level2 partition
    copyFromJar(
        "metadatarefresh/float.parquet",
        java.nio.file.Paths.get(testRootPath + "partialRefreshAddingFile/level2/float.parquet"));

    // this will do an partial refresh now.
    // Only one file addition should be shown as we are refreshing
    // only one partition
    runSQL(sql + " FOR PARTITIONS (\"dir0\" = \'lala\')");

    // Refresh the same iceberg table again
    icebergTable.refresh();

    expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.DoubleType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 3);

    long modTime =
        new File("/tmp/metadatarefresh/partialRefreshAddingFile/lala/float.parquet").lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/partialRefreshAddingFile/lala/float.parquet?version=" + modTime);
  }

  @Test
  public void testPartialRefreshAfterAddingPartition() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.partialRefreshAddingPartition refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

    Thread.sleep(1001L);
    copyFromJar(
        "metadatarefresh/level2",
        java.nio.file.Paths.get(testRootPath + "partialRefreshAddingPartition/level2"));

    // this will do an partial refresh now. We are refreshing old partition so nothing should
    // show up in the table
    runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'lala')");

    // Refresh the same iceberg table again
    icebergTable.refresh();

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 1);

    // Doing a partial refresh again but now providing a newly added partition
    runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'level2')");

    // Refresh the same iceberg table again
    icebergTable.refresh();

    expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "col1", new Types.DoubleType())));

    // Now the table should change and show newly added files
    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0"), 2);
    long modTime =
        new File("/tmp/metadatarefresh/partialRefreshAddingPartition/level2/float.parquet")
            .lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/partialRefreshAddingPartition/level2/float.parquet?version="
            + modTime);
  }

  @Test
  public void testPartialRefreshShouldNotRefreshRecursively() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.partialRefreshAddingMultiLevelPartition refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "dir0", new Types.StringType()),
                Types.NestedField.optional(2, "dir1", new Types.StringType()),
                Types.NestedField.optional(3, "col1", new Types.IntegerType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "dir1"), 1);

    Table icebergTable = RefreshDatasetTestUtils.getIcebergTable(finalIcebergMetadataLocation);

    Thread.sleep(1001L);

    // adding a file in level1
    copyFromJar(
        "metadatarefresh/int.parquet",
        java.nio.file.Paths.get(
            testRootPath + "partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet"));

    // adding a file in level2
    copyFromJar(
        "metadatarefresh/int.parquet",
        java.nio.file.Paths.get(
            testRootPath
                + "partialRefreshAddingMultiLevelPartition/level1/level2/intLevel2.parquet"));

    // this will do an partial refresh now. We are refreshing only
    // level1. So file added in level2 (inside level1) should not
    // show up.
    runSQL(sql + " FOR PARTITIONS (\"dir0\" = 'level1', \"dir1\" = null)");

    // Refresh the same iceberg table again
    icebergTable.refresh();

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 1, 0, expectedSchema, Sets.newHashSet("dir0", "dir1"), 2);

    long modTime =
        new File(
                "/tmp/metadatarefresh/partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet")
            .lastModified();
    Assert.assertEquals(
        RefreshDatasetTestUtils.getAddedFilePaths(icebergTable).get(0),
        "file:/tmp/metadatarefresh/partialRefreshAddingMultiLevelPartition/level1/intLevel1.parquet?version="
            + modTime);
  }

  @Test
  public void testPartialRefreshFailureErrorOnFirstQuery() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.failPartialRefreshForFirstQuery refresh metadata for partitions (\"dir0\" = 'lala');";

    // Should throw an error. First query cannot be a partial refresh.
    assertThatThrownBy(() -> runSQL(sql))
        .hasMessageContaining(
            "Selective refresh is not allowed on unknown datasets. Please run full refresh on dfs.tmp.metadatarefresh.failPartialRefreshForFirstQuery");
  }

  @Test
  public void testPartialRefreshFailureOnProvidingIncompletePartitions() throws Exception {
    final String sql =
        "alter table dfs.tmp.metadatarefresh.failPartialOnProvidingIncompletePartition refresh metadata";

    // First a full refresh
    runSQL(sql);

    Thread.sleep(1001L);
    // without modifying table, read signature check skips refresh
    copyFromJar(
        "metadatarefresh/failPartialOnProvidingIncompletePartition/level1/level2/int.parquet",
        java.nio.file.Paths.get(
            testRootPath + "failPartialOnProvidingIncompletePartition/int.parquet"));

    // This should error out as we are providing only on partition
    assertThatThrownBy(() -> runSQL(sql + " for partitions (\"dir0\" = 'lala')"))
        .hasMessageContaining("Refresh dataset command must include all partitions.");
  }

  @Test
  public void testRefreshFailureOnAddingPartition() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.failOnAddingPartition refresh metadata";

    // First a full refresh
    runSQL(sql);

    Thread.sleep(1001L);
    copyFromJar(
        "metadatarefresh/level2",
        java.nio.file.Paths.get(testRootPath + "failOnAddingPartition/level1/level2"));

    // This should error out as we have added a partition
    assertThatThrownBy(() -> runSQL(sql))
        .hasMessageContaining(
            "Addition of a new level dir is not allowed in incremental refresh. Please forget and promote the table again.");
  }

  @Test
  public void testCreateEmptyIcebergTable() throws Exception {
    String tblName = "emptyIcebergTable";
    try (AutoCloseable c2 = enableIcebergTables() /* to create iceberg tables with ctas */) {
      final String createTableQuery =
          String.format("CREATE TABLE %s.%s(id int, code int)", "dfs_test_hadoop", tblName);
      test(createTableQuery);
      java.nio.file.Path metadataFolder =
          Paths.get(getDfsTestTmpSchemaLocation(), tblName, "metadata");
      Assert.assertTrue(metadataFolder.toFile().exists());
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCreateAndSelectOnIcebergTable() throws Exception {
    String tblName = "newIcebergTable";
    try (AutoCloseable c2 = enableIcebergTables() /* to create iceberg tables with ctas */) {
      final String createTableQuery =
          String.format(
              "create table %s.%s(id, code) as values(cast(10 as int), cast(30 as int))",
              "dfs_test_hadoop", tblName);
      test(createTableQuery);

      java.nio.file.Path metadataFolder =
          Paths.get(getDfsTestTmpSchemaLocation(), tblName, "metadata");
      Assert.assertTrue(metadataFolder.toFile().exists());

      Thread.sleep(1001L);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", "dfs_test_hadoop", tblName))
          .unOrdered()
          .baselineColumns("id", "code")
          .baselineValues(10, 30)
          .build()
          .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testDataFileOverride() throws Exception {
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

  @Test
  public void testRefreshMaxColsExceeded() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.maxLeafCols refresh metadata";

    try (AutoCloseable c2 = setMaxLeafColumns(1000)) {
      runSQL(sql);
    } finally {
      runSQL("alter table dfs.tmp.metadatarefresh.maxLeafCols forget metadata");
    }

    try (AutoCloseable c2 = setMaxLeafColumns(800)) {
      runSQL(sql);
      fail("Expecting exception");
    } catch (Exception e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Number of fields in dataset exceeded the maximum number of fields of 800"));
    }
  }

  @Test
  public void testRefreshAndSelectOnNestedStructParquet() throws Exception {
    File dir = new File(testRootPath + "testRefreshAndSelectOnNestedStructParquet");

    if (!dir.exists()) {
      dir.mkdir();
    }
    File nestedParquet =
        new File(Resources.getResource("metadatarefresh/nested_struct.impala.parquet").getFile());
    FileUtils.copyFileToDirectory(nestedParquet, dir, false);
    runSQL(
        "alter table dfs.tmp.metadatarefresh.testRefreshAndSelectOnNestedStructParquet refresh metadata");
    runSQL("select * from dfs.tmp.metadatarefresh.testRefreshAndSelectOnNestedStructParquet");
  }

  @Test
  public void testRefreshAddFileInsideEmptyFolder() throws Exception {
    final String sql = "alter table dfs.tmp.metadatarefresh.multiLevel refresh metadata";

    // this will do a full refresh first
    runSQL(sql);

    // table only has 1 record
    final ImmutableList.Builder<Map<String, Object>> recordBuilder1 = ImmutableList.builder();
    long val1 = 2;
    recordBuilder1.add(ImmutableMap.of("`cnt`", val1));
    testBuilder()
        .ordered()
        .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.multiLevel")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder1.build())
        .go();

    copyFromJar(
        "metadatarefresh/float.parquet",
        java.nio.file.Paths.get(testRootPath + "multiLevel/level1/float.parquet"));

    runSQL(sql);

    // table should now have 2 records since read signature check passed
    final ImmutableList.Builder<Map<String, Object>> recordBuilder2 = ImmutableList.builder();
    long val2 = 4;
    recordBuilder2.add(ImmutableMap.of("`cnt`", val2));
    testBuilder()
        .ordered()
        .sqlQuery("select count(*) as cnt from dfs.tmp.metadatarefresh.multiLevel")
        .baselineColumns("col1")
        .baselineRecords(recordBuilder2.build())
        .go();
  }

  /**
   * Verify that UnlimitedSplitsFileDatasetHandle will be used for Parquet Datasets (Regression test
   * for DX-60716) when: parquet dataset (file/folder) unlimitedSplits on (auto-promote is off &&
   * dataset was previously promoted *or* auto-promote is on )
   */
  @Test
  public void testForUnlimitedSplitsFileDatasetHandle() throws Exception {

    // Test to ensure that UnlimitedSplitsFileDatasetHandle gets returned from getDatasetHandle()
    // for parquet datasets
    {
      File dir = new File(testRootPath + "datasetHandleParquetTest");
      if (!dir.exists()) {
        dir.mkdir();
      }

      File dataFile = new File(Resources.getResource("metadatarefresh/int.parquet").getFile());
      FileUtils.copyFileToDirectory(dataFile, dir, false);

      final CatalogServiceImpl pluginRegistry =
          (CatalogServiceImpl) getSabotContext().getCatalogService();
      final FileSystemPlugin<?> msp = pluginRegistry.getSource("dfs");

      // set auto-promote true to trigger getDatasetHandle() to return
      // UnlimitedSplitsFileDatasetHandle if generated
      DatasetRetrievalOptions options =
          DatasetRetrievalOptions.DEFAULT.toBuilder().setAutoPromote(true).build();
      NamespaceKey namespaceKey =
          new NamespaceKey(
              Arrays.asList("dfs", "tmp", "metadatarefresh", "datasetHandleParquetTest"));
      final EntityPath entityPath = MetadataObjectsUtils.toEntityPath(namespaceKey);
      Optional<DatasetHandle> handle =
          msp.getDatasetHandle(entityPath, options.asGetDatasetOptions(null));

      if (!handle.isPresent() || !(handle.get() instanceof UnlimitedSplitsFileDatasetHandle)) {
        Assert.fail("Expected UnlimitedSplitsFileDatasetHandle to be created for parquet dataset");
      }
    }

    // Test to ensure that UnlimitedSplitsFileDatasetHandle do not get returned from
    // getDatasetHandle() for non-parquet datasets
    {
      File dir = new File(testRootPath + "datasetHandleCSVTest");
      if (!dir.exists()) {
        dir.mkdir();
      }

      File dataFile = new File(Resources.getResource("metadatarefresh/bogus.csv").getFile());
      FileUtils.copyFileToDirectory(dataFile, dir, false);

      final CatalogServiceImpl pluginRegistry =
          (CatalogServiceImpl) getSabotContext().getCatalogService();
      final FileSystemPlugin<?> msp = pluginRegistry.getSource("dfs");

      // set auto-promote true to trigger getDatasetHandle() to return
      // UnlimitedSplitsFileDatasetHandle if generated
      DatasetRetrievalOptions options =
          DatasetRetrievalOptions.DEFAULT.toBuilder().setAutoPromote(true).build();
      NamespaceKey namespaceKey =
          new NamespaceKey(Arrays.asList("dfs", "tmp", "metadatarefresh", "datasetHandleCSVTest"));
      final EntityPath entityPath = MetadataObjectsUtils.toEntityPath(namespaceKey);
      Optional<DatasetHandle> handle =
          msp.getDatasetHandle(entityPath, options.asGetDatasetOptions(null));

      if (handle.isPresent() && (handle.get() instanceof UnlimitedSplitsFileDatasetHandle)) {
        // matching an UnlimitedSplitsFileDatasetHandle for a CSV should be a failure
        Assert.fail("Should not create UnlimitedSplitsFileDatasetHandle for non-parquet dataset");
      }
    }
  }

  @Test
  public void testPartialRefreshWithPartitionInferenceErrors() throws Exception {
    final String sql1 =
        "alter table dfs_partition_inference.tmp.metadatarefresh.refreshWithPartitionInference refresh metadata";

    runSQL(sql1);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.IntegerType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType()),
                Types.NestedField.optional(3, "level1", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation, 3, 0, expectedSchema, Sets.newHashSet("dir0", "level1"), 3);

    testBuilder()
        .sqlQuery(
            "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.refreshWithPartitionInference REFRESH METADATA FOR PARTITIONS (\"dir0\"='level1=1') FORCE UPDATE")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            "Metadata for table 'dfs_partition_inference.tmp.metadatarefresh.refreshWithPartitionInference' refreshed.")
        .build()
        .run();

    // Expected failure:
    //    The column 'bogus' is not partition (the error message will provide hint)
    final String sql3 =
        "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.refreshWithPartitionInference REFRESH METADATA FOR PARTITIONS (\"bogus\"='1') FORCE UPDATE";
    UserExceptionAssert.assertThatThrownBy(() -> test(sql3))
        .hasErrorType(ErrorType.VALIDATION)
        .hasMessageContaining(
            "Column 'bogus' not found in the list of partition columns: [level1]");
  }

  @Test
  public void testPartialRefreshWithMultiplePartitionInferenceErrors() throws Exception {
    final String sql1 =
        "alter table dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 refresh metadata";

    runSQL(sql1);

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                Types.NestedField.optional(1, "col1", new Types.IntegerType()),
                Types.NestedField.optional(2, "dir0", new Types.StringType()),
                Types.NestedField.optional(3, "level1", new Types.StringType()),
                Types.NestedField.optional(4, "dir1", new Types.StringType()),
                Types.NestedField.optional(5, "level2", new Types.StringType())));

    verifyIcebergMetadata(
        finalIcebergMetadataLocation,
        6,
        0,
        expectedSchema,
        Sets.newHashSet("dir0", "level1", "dir1", "level2"),
        6);

    // baseline for dirN
    testBuilder()
        .sqlQuery(
            "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"dir0\"='level1=1', \"dir1\"='level2=11') FORCE UPDATE")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            "Metadata for table 'dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2' refreshed.")
        .build()
        .run();

    // baseline for Inferred
    testBuilder()
        .sqlQuery(
            "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"level1\"='1', \"level2\"='11') FORCE UPDATE")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            "Metadata for table 'dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2' refreshed.")
        .build()
        .run();

    // Verify with inferred partitions out of order
    testBuilder()
        .sqlQuery(
            "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"level2\"='11', \"level1\"='1') FORCE UPDATE")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            "Metadata for table 'dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2' refreshed.")
        .build()
        .run();

    // Verify with dirN partitions out of order
    testBuilder()
        .sqlQuery(
            "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"dir1\"='level2=11', \"dir0\"='level1=1') FORCE UPDATE")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            true,
            "Metadata for table 'dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2' refreshed.")
        .build()
        .run();

    // Expected failure because partition names are not consistent
    final String sql2 =
        "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"dir0\"='level1=1', \"level2\"='11') FORCE UPDATE";
    UserExceptionAssert.assertThatThrownBy(() -> test(sql2))
        .hasErrorType(ErrorType.VALIDATION)
        .hasMessageContaining(
            "Partition columns should be all 'dirN' columns or all inferred partition columns");

    Thread.sleep(1001L);

    // Expected failure because partition names are not consistent
    final String sql3 =
        "ALTER TABLE dfs_partition_inference.tmp.metadatarefresh.onlyFullWithPartitionInferenceLevel2 REFRESH METADATA FOR PARTITIONS (\"level2\"='11', \"dir0\"='level1=1') FORCE UPDATE";
    UserExceptionAssert.assertThatThrownBy(() -> test(sql3))
        .hasErrorType(ErrorType.VALIDATION)
        .hasMessageContaining(
            "Partition columns should be all 'dirN' columns or all inferred partition columns");
  }
}
