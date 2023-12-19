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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class TestDeltaMetadataFetchJobManager extends BaseTestQuery  {

  String path;
  FileSelection selection;
  File f;
  FileSystem fs;
  SabotContext sabotContext;

  @Before
  public void setup() throws Exception {
    path = "src/test/resources/deltalake/covid_cases";
    f = new File(path);
    fs = HadoopFileSystem.getLocal(new Configuration());
    selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));
    sabotContext = getSabotContext();
  }

  @Test
  public void testSingleBatchReadLatest() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, null);
    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(20L, 21L, 22L, 23L, 24L, 25L);

    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(manager.getBatchesRead(), 1);
    assertEquals(expectedVersions, actualVersions);
  }

  @Test
  public void testMultipleBatchesReadLatest() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, null);
    manager.setBatchSize(3);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(20L, 21L, 22L, 23L, 24L, 25L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    //Last batch is empty batch as on reading 26 we will figure out that the next file doesn't exist
    assertEquals(manager.getBatchesRead(), 3);
    assertEquals(expectedVersions, actualVersions);

    manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, null);
    manager.setBatchSize(7);

    snapshotList = manager.getListOfSnapshots();
    actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(manager.getBatchesRead(), 1);
    assertEquals(expectedVersions, actualVersions);
  }

  @Test
  public void testSingleBatchWithoutCheckpoint() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testSingleBatchWithoutCheckpoint(TimeTravelOption.newSnapshotIdRequest("5"));
      testSingleBatchWithoutCheckpoint(TimeTravelOption.newTimestampRequest(1610656317464L));
    }
  }

  private void testSingleBatchWithoutCheckpoint(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(5L, 4L, 3L, 2L, 1L, 0L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testSingleBatchWithCheckpoint() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testSingleBatchWithCheckpoint(TimeTravelOption.newSnapshotIdRequest("17"));
      testSingleBatchWithCheckpoint(TimeTravelOption.newTimestampRequest(1610656595704L));
    }
  }

  private void testSingleBatchWithCheckpoint(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testMultipleBatches() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMultipleBatches(TimeTravelOption.newSnapshotIdRequest("8"));
      testMultipleBatches(TimeTravelOption.newTimestampRequest(1610656345288L));
    }
  }

  private void testMultipleBatches(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);
    manager.setBatchSize(5);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 2);
  }

  @Test
  public void testMultipleBatchesWithCheckpoint() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMultipleBatchesWithCheckpoint(TimeTravelOption.newSnapshotIdRequest("18"));
      testMultipleBatchesWithCheckpoint(TimeTravelOption.newTimestampRequest(1610656604782L));
    }
  }

  private void testMultipleBatchesWithCheckpoint(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);
    manager.setBatchSize(4);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 3);
  }

  @Test
  public void testVeryLargeBatchSize() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testVeryLargeBatchSize(TimeTravelOption.newSnapshotIdRequest("22"));
      testVeryLargeBatchSize(TimeTravelOption.newTimestampRequest(1610656648256L));
    }
  }

  private void testVeryLargeBatchSize(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);
    //Will end up reading 2 checkpoint files
    manager.setBatchSize(20);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(22L, 21L, 20L, 19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testEndingWithCheckpointDataset() throws Exception {
    testEndingWithCheckpointDataset(null); // readLatest
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testEndingWithCheckpointDataset(TimeTravelOption.newSnapshotIdRequest("10"));
      testEndingWithCheckpointDataset(TimeTravelOption.newTimestampRequest(1608800728932L));
    }
  }

  public void testEndingWithCheckpointDataset(TimeTravelOption.TimeTravelRequest travelRequest) throws Exception {
    String path = "src/test/resources/deltalake/ending_with_checkpoint_dataset";
    File f = new File(path);
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));

    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);
    //Setting batch size more than files read
    manager.setBatchSize(3);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = manager.isReadLatest() ? Arrays.asList(10L) : Arrays.asList(10L, 9L, 8L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testMetadataFetchJobConcurrency() throws Exception {
    Integer defaultPoolSize = DeltaMetadataFetchPool.POOL_SIZE;
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      //Setting thread pool size to a low value to ensure that if there are more than
      //2 tasks they are queued and not dropped.
      DeltaMetadataFetchPool.POOL_SIZE = 2;

      testMetadataFetchJobConcurrency(TimeTravelOption.newSnapshotIdRequest("22"));
      testMetadataFetchJobConcurrency(TimeTravelOption.newTimestampRequest(1610656648256L));
    } finally {
      DeltaMetadataFetchPool.POOL_SIZE = defaultPoolSize;
    }
  }

  @Test
  public void testMetadataFetchJobConcurrencyDefault() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMetadataFetchJobConcurrency(TimeTravelOption.newSnapshotIdRequest("22"));
      assertTrue("The Active worker count should be zero after work.",
      DeltaMetadataFetchPool.getPool().getActiveCount() == 0);
      assertTrue("The largest number of threads that have ever simultaneously been in the pool should be > one.",
      DeltaMetadataFetchPool.getPool().getLargestPoolSize() > 1);
    }
  }

  private void testMetadataFetchJobConcurrency(TimeTravelOption.TimeTravelRequest travelRequest) {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, travelRequest);
    manager.setBatchSize(10);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(22L, 21L, 20L, 19L, 18L, 17L, 16L, 15L, 14L, 13L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }
}
