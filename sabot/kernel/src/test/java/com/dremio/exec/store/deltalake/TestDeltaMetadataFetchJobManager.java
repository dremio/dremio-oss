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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
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
    selection = FileSelection.create(fs, Path.of(f.getAbsolutePath()));
    sabotContext = getSabotContext();
  }

  @Test
  public void testSingleBatchReadLatest() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, true);
    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(20L, 21L, 22L, 23L, 24L, 25L);

    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(manager.getBatchesRead(), 1);
    assertEquals(expectedVersions, actualVersions);
  }

  @Test
  public void testMultipleBatchesReadLatest() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, true);
    manager.setBatchSize(3);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(20L, 21L, 22L, 23L, 24L, 25L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    //Last batch is empty batch as on reading 26 we will figure out that the next file doesn't exists
    assertEquals(manager.getBatchesRead(), 3);
    assertEquals(expectedVersions, actualVersions);

    manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, true);
    manager.setBatchSize(7);

    snapshotList = manager.getListOfSnapshots();
    actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(manager.getBatchesRead(), 1);
    assertEquals(expectedVersions, actualVersions);
  }

  @Test
  public void testSingleBatchWithVersionGivenWithoutCheckpoint() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 5L);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(5L, 4L, 3L, 2L, 1L, 0L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testSingleBatchWithVersionGivenWithCheckpoint() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 17L);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();
    List<Long> expectedVersions = Arrays.asList(17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testMultipleBatchesWithVersionGiven() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 8L);
    manager.setBatchSize(5);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 2);
  }

  @Test
  public void testMultipleBatchesWithVersionGivenWithCheckpoint() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 18L);
    manager.setBatchSize(4);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 3);
  }

  @Test
  public void testVeryLargeBatchSize() {
    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 22L);
    //Will end up reading 2 checkpoint files
    manager.setBatchSize(20);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(22L, 21L, 20L, 19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testEndingWithCheckpointDatasetReadLatest() throws IOException {
    String path = "src/test/resources/deltalake/ending_with_checkpoint_dataset";
    f = new File(path);
    selection = FileSelection.create(fs, Path.of(f.getAbsolutePath()));

    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, true);
    //Setting batch size more than files
    manager.setBatchSize(20);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(10L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }

  @Test
  public void testEndingWithCheckpointDatasetGivenVersion() throws IOException {
    String path = "src/test/resources/deltalake/ending_with_checkpoint_dataset";
    File f = new File(path);
    selection = FileSelection.create(fs, Path.of(f.getAbsolutePath()));

    DeltaMetadataFetchJobManager manager = new DeltaMetadataFetchJobManager(sabotContext, fs, selection, 10);
    //Setting batch size more than files read
    manager.setBatchSize(3);

    List<DeltaLogSnapshot> snapshotList = manager.getListOfSnapshots();

    List<Long> expectedVersions = Arrays.asList(10L, 9L, 8L);
    List<Long> actualVersions = snapshotList.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(expectedVersions, actualVersions);
    assertEquals(manager.getBatchesRead(), 1);
  }


}
