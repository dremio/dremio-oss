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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.MetadataProtoUtils;

public class TestDeltaLakeTable extends BaseTestQuery {

  String path;
  FileSelection selection;
  File f;
  FileSystem fs;
  SabotContext sabotContext;

  @Before
  public void setup() throws IOException {
    path = "src/test/resources/deltalake/covid_cases";
    f = new File(path);
    fs = HadoopFileSystem.getLocal(new Configuration());
    selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));
    sabotContext = getSabotContext();
  }

  @Test
  public void testWithLargeDatasetLatest() throws IOException {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, null);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000020.checkpoint.parquet", "00000000000000000021.json", "00000000000000000022.json", "00000000000000000023.json", "00000000000000000024.json", "00000000000000000025.json");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 25);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"continent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gdp_per_capita\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cardiovasc_death_rate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diabetes_prevalence\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 26);
    assertEquals(snap.getNetBytesAdded(), 104714);

    assertEquals(5, table.buildDatasetXattr().getNumCommitJsonDataFileCount());
  }

  @Test
  public void testWithLargeVersion() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testWithLargeVersion(TimeTravelOption.newSnapshotIdRequest("17"));
      testWithLargeVersion(TimeTravelOption.newTimestampRequest(1610656595704L));
    }
  }

  private void testWithLargeVersion(TimeTravelOption.TimeTravelRequest travelRequest) throws Exception {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000010.checkpoint.parquet", "00000000000000000011.json", "00000000000000000012.json", "00000000000000000013.json", "00000000000000000014.json", "00000000000000000015.json", "00000000000000000016.json", "00000000000000000017.json");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 17);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"continent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gdp_per_capita\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cardiovasc_death_rate\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"diabetes_prevalence\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 18);
    assertEquals(snap.getNetBytesAdded(), 77265);
  }

  @Test
  public void testWithSmallVersion() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testWithSmallVersion(TimeTravelOption.newSnapshotIdRequest("7"));
      testWithSmallVersion(TimeTravelOption.newTimestampRequest(1610656335387L));
    }
  }

  private void testWithSmallVersion(TimeTravelOption.TimeTravelRequest travelRequest) throws Exception {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000000.json",  "00000000000000000001.json", "00000000000000000002.json", "00000000000000000003.json", "00000000000000000004.json", "00000000000000000005.json", "00000000000000000006.json", "00000000000000000007.json");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 7);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"continent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gdp_per_capita\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 8);
    assertEquals(snap.getNetBytesAdded(), 48840);
  }

  @Test
  public void testVersionZero() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testVersionZero(TimeTravelOption.newSnapshotIdRequest("0"));
      testVersionZero(TimeTravelOption.newTimestampRequest(1610656189269L));
    }
  }

  private void testVersionZero(TimeTravelOption.TimeTravelRequest travelRequest) throws IOException {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000000.json");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 0);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"continent\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gdp_per_capita\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 1);
    assertEquals(snap.getNetBytesAdded(), 9308);
  }

  @Test
  public void testEndingWithCheckpointDataset() throws Exception {
    File f = new File("src/test/resources/deltalake/ending_with_checkpoint_dataset");
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));

    testEndingWithCheckpointDataset(selection, null); // read latest
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testEndingWithCheckpointDataset(selection, TimeTravelOption.newSnapshotIdRequest("10"));
      testEndingWithCheckpointDataset(selection, TimeTravelOption.newTimestampRequest(1608800728932L));
    }
  }

  private void testEndingWithCheckpointDataset(FileSelection selection, TimeTravelOption.TimeTravelRequest travelRequest) throws IOException {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000010.checkpoint.parquet");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 10);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 11);
    assertEquals(snap.getNetBytesAdded(), 14663);
  }

  @Test
  public void testEndingWithMultiPartCheckpointDatasetReadLatest() throws IOException {
    File f = new File("src/test/resources/deltalake/multiPartCheckpoint");
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, null);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("00000000000000000010.checkpoint.0000000001.0000000002.parquet","00000000000000000010.checkpoint.0000000002.0000000002.parquet","00000000000000000011.json");

    assertEquals(expected, actual);
    assertEquals(snap.getVersionId(), 11);
    assertEquals(snap.getSchema(), "{\"type\":\"struct\",\"fields\":[{\"name\":\"intcol\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}, {\"name\":\"longcol\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}, {\"name\":\"stringcol\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}");
    assertEquals(snap.getNetFilesAdded(), 5);
    assertEquals(snap.getNetBytesAdded(), 4737);
  }

  private String getPath(DatasetSplit datasetSplit) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      datasetSplit.getExtraInfo().writeTo(baos);
      EasyProtobuf.EasyDatasetSplitXAttr splitXAttr = EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(baos.toByteArray());
      java.nio.file.Path path = Paths.get(splitXAttr.getPath());
      return path.getFileName().toString();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testMetadataStaleCheck() throws IOException {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, null);

    BytesOutput signature = table.readSignature();
    DeltaLakeProtobuf.DeltaLakeReadSignature deltaLakeReadSignature = LegacyProtobufSerializer.parseFrom(DeltaLakeProtobuf.DeltaLakeReadSignature.PARSER, MetadataProtoUtils.toProtobuf(signature));
    //Before snapshot list is fetched  metadata should be stable
    assertTrue(table.checkMetadataStale(deltaLakeReadSignature));

    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();
    assertEquals(snap.getVersionId(), 25);

    signature = table.readSignature();
    deltaLakeReadSignature = LegacyProtobufSerializer.parseFrom(DeltaLakeProtobuf.DeltaLakeReadSignature.PARSER, MetadataProtoUtils.toProtobuf(signature));
    //after fetching metadata should not be stale
    assertFalse(table.checkMetadataStale(deltaLakeReadSignature));
  }

  @Test
  public void testMultipartCheckpointLastVersion() throws Exception {
    File f = new File("src/test/resources/deltalake/multipartCheckpointSkips");
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));

    testMultipartCheckpointLastVersion(selection, null);
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMultipartCheckpointLastVersion(selection, TimeTravelOption.newSnapshotIdRequest("13"));
      testMultipartCheckpointLastVersion(selection, TimeTravelOption.newTimestampRequest(1684117263315L));
    }
  }

  private void testMultipartCheckpointLastVersion(FileSelection selection, TimeTravelOption.TimeTravelRequest travelRequest) throws IOException {
    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList(
      "00000000000000000012.checkpoint.0000000001.0000000004.parquet",
      "00000000000000000012.checkpoint.0000000002.0000000004.parquet",
      "00000000000000000012.checkpoint.0000000003.0000000004.parquet",
      "00000000000000000012.checkpoint.0000000004.0000000004.parquet",
      "00000000000000000013.json");

    assertEquals(expected, actual);
    assertEquals(13L, snap.getVersionId());
    assertEquals(4L, snap.getNetFilesAdded());
    assertEquals(2836L, snap.getNetBytesAdded());
  }

  @Test
  public void testMultipartCheckpointSkipToPreviousCheckpoint() throws Exception {
    File f = new File("src/test/resources/deltalake/multipartCheckpointSkips");
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMultipartCheckpointSkipToPreviousCheckpoint(selection, TimeTravelOption.newSnapshotIdRequest("10"));
      testMultipartCheckpointSkipToPreviousCheckpoint(selection, TimeTravelOption.newTimestampRequest(1684117237899L));
    }
  }

  private void testMultipartCheckpointSkipToPreviousCheckpoint(FileSelection selection, TimeTravelOption.TimeTravelRequest travelRequest) throws IOException {
    // checkpoint for version 9 is missing a part, expect to skip and use previous checkpoint for version 6

    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList(
      "00000000000000000006.checkpoint.0000000001.0000000002.parquet",
      "00000000000000000006.checkpoint.0000000002.0000000002.parquet",
      "00000000000000000007.json",
      "00000000000000000008.json",
      "00000000000000000009.json",
      "00000000000000000010.json");

    assertEquals(expected, actual);
    assertEquals(10L, snap.getVersionId());
    assertEquals(1L, snap.getNetFilesAdded());
    assertEquals(798L, snap.getNetBytesAdded());
  }

  @Test
  public void testMultipartCheckpointSkipToVersion0() throws Exception {
    File f = new File("src/test/resources/deltalake/multipartCheckpointSkips");
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      testMultipartCheckpointSkipToVersion0(selection, TimeTravelOption.newSnapshotIdRequest("4"));
      testMultipartCheckpointSkipToVersion0(selection, TimeTravelOption.newTimestampRequest(1684117164770L));
    }
  }

  private void testMultipartCheckpointSkipToVersion0(FileSelection selection, TimeTravelOption.TimeTravelRequest travelRequest) throws IOException {
    // checkpoint for version 3 is missing a part, expect to skip and use only json log files starting version 0

    DeltaLakeTable table = new DeltaLakeTable(sabotContext, fs, selection, travelRequest);
    DeltaLogSnapshot snap = table.getConsolidatedSnapshot();

    List<String> actual = table.getAllSplits().stream().map(this::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList(
      "00000000000000000000.json",
      "00000000000000000001.json",
      "00000000000000000002.json",
      "00000000000000000003.json",
      "00000000000000000004.json");

    assertEquals(expected, actual);
    assertEquals(4L, snap.getVersionId());
    assertEquals(8L, snap.getNetFilesAdded());
    assertEquals(5487L, snap.getNetBytesAdded());
  }
}
