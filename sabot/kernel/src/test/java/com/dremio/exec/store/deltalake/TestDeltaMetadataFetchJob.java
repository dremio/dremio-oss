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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Tests for {@link DeltaMetadataFetchJob}
 */
public class TestDeltaMetadataFetchJob extends BaseTestQuery {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  static FileSystem fs;
  static SabotContext context;
  static Path metaDir;

  @BeforeClass
  public static void setup() throws IOException {
    metaDir = Path.of("src/test/resources/deltalake/_delta_log");
    fs = HadoopFileSystem.getLocal(new Configuration());
    context = getSabotContext();
  }

  @Test
  public void testCommitJsonRead() {
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.of(0));
    DeltaLogSnapshot snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 0);
    assertEquals("WRITE", snapshot.getOperationType());
    final String expectedSchemaString  = "{\"type\":\"struct\",\"fields\":[{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());


    job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.ofCheckpoint(0L, 1));
    //Will try checkpoint read first then JSON read
    snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 0);
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());
  }

  @Test
  public void testMultiDigitCommitJsonRead() {
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.of(11L));
    DeltaLogSnapshot snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 11);
    assertEquals("WRITE", snapshot.getOperationType());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());
    assertEquals(1333, snapshot.getNetBytesAdded());
  }

  @Test
  public void testCheckpointParquetRead() {
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.ofCheckpoint(10L, 1));
    DeltaLogSnapshot snapshot = job.get();

    assertEquals(10L, snapshot.getVersionId());
    assertEquals("UNKNOWN", snapshot.getOperationType());
    final String expectedSchemaString  = "{\"type\":\"struct\",\"fields\":[{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(11L, snapshot.getNetFilesAdded());
    assertEquals(276L, snapshot.getNetOutputRows());
  }

  @Test
  public void testCommitFileNotFound() {
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.of(1000L));
    assertThatThrownBy(() -> job.get()).cause().isInstanceOf(DeltaMetadataFetchJob.InvalidFileException.class);
  }


  @Test
  public void testCheckpointFileNotFound() {
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(context, metaDir, fs, DeltaVersion.ofCheckpoint(1000L, 1));
    assertThatThrownBy(() -> job.get()).cause().isInstanceOf(DeltaMetadataFetchJob.InvalidFileException.class);
  }
}
