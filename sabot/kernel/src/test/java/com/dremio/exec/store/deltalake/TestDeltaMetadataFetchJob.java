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
import java.io.IOException;
import java.util.concurrent.CompletionException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Tests for {@link DeltaMetadataFetchJob}
 */
public class TestDeltaMetadataFetchJob {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testCommitJsonRead() throws IOException {
    String path = "src/test/resources/deltalake/_delta_log";
    Path metaDir = Path.of(path);

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), false, 0L);
    DeltaLogSnapshot snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 0);
    assertEquals("WRITE", snapshot.getOperationType());
    final String expectedSchemaString  = "{\"type\":\"struct\",\"fields\":[{\"name\":\"iso_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"location\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());


    job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), true, 0L);
    //Will try checkpoint read first then JSON read
    snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 0);
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());
  }

  @Test
  public void testMultiDigitCommitJsonRead() throws IOException {
    String path = "src/test/resources/deltalake/_delta_log";
    Path metaDir = Path.of(path);

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), false, 11L);
    DeltaLogSnapshot snapshot = job.get();

    assertEquals(snapshot.getVersionId(), 11);
    assertEquals("WRITE", snapshot.getOperationType());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(11L, snapshot.getNetOutputRows());
    assertEquals(1333, snapshot.getNetBytesAdded());
  }

  @Test
  public void testCheckpointParquetRead() {
    //TODO
    //Add test for parquet checkpoint parquet read. Reader not ready yet
  }

  @Test
  public void testFileNotFound() throws IOException {
    String path = "src/test/resources/deltalake/_delta_log";
    Path metaDir = Path.of(path);

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), true, 1000L);

    try {
      DeltaLogSnapshot snapshot = job.get();
    }
    catch (CompletionException e) {
      assertTrue(e.getCause() instanceof DeltaMetadataFetchJob.InvalidFileException);
    }
  }

  @Test
  public void testCommitWrittenAfterReadStart() throws IOException {
    File root = tempDir.newFolder();
    Path metaDir = Path.of(root.getAbsolutePath());
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    File commitJson = new File(root, "00000000000000000001.checkpoint.parquet");
    commitJson.createNewFile();
    commitJson.setLastModified(System.currentTimeMillis() + 10 * 1000);


    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), true, 1L);

    try {
      DeltaLogSnapshot snapshot = job.get();
    }
    catch (CompletionException e) {
      assertTrue(e.getCause() instanceof DeltaMetadataFetchJob.InvalidFileException);
    }
  }

  @Test
  public void testBadConfig() throws IOException {
    String path = "src/test/resources/deltalake/_delta_log";
    Path metaDir = Path.of(path);

    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    DeltaMetadataFetchJob job = new DeltaMetadataFetchJob(null, metaDir, fs, System.currentTimeMillis(), true, 0L);
    //Should fail as we are trying to read checkpoint but the file is a commit json

    try {
      DeltaLogSnapshot snapshot = job.get();
    }
    catch (CompletionException e) {
      assertTrue(e.getCause() instanceof DeltaMetadataFetchJob.InvalidFileException);
    }
  }
}
