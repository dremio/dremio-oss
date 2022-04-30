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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests for {@link DeltaLogCommitJsonReader}
 */
public class TestDeltaLogCommitJsonReader {

    @Test
    public void testCommitInfoWithoutOpStats() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_0.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

        assertEquals("WRITE", snapshot.getOperationType());
        final String expectedSchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(expectedSchemaString, snapshot.getSchema());
        assertEquals(0L, snapshot.getNetBytesAdded());
        assertEquals(DremioCost.LARGE_FILE_COUNT, snapshot.getNetFilesAdded());
        assertEquals(DremioCost.LARGE_ROW_COUNT, snapshot.getNetOutputRows());
        assertEquals(ImmutableList.of("ws_sold_date_sk"), snapshot.getPartitionColumns());
        assertEquals(1609775409819L, snapshot.getTimestamp());
        assertTrue(snapshot.isMissingRequiredValues());
    }

    @Test
    public void testCommitInfoWithoutOpStatsOnlySchemaChange() throws IOException {
      File f = FileUtils.getResourceAsFile("/deltalake/test1_0_noadd.json");
      Configuration conf = new Configuration();
      final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
      DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
      DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

      assertEquals("WRITE", snapshot.getOperationType());
      final String expectedSchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
      assertEquals(expectedSchemaString, snapshot.getSchema());
      assertEquals(0L, snapshot.getNetBytesAdded());
      assertEquals(0L, snapshot.getNetFilesAdded());
      assertEquals(0L, snapshot.getNetOutputRows());
      assertFalse(snapshot.isMissingRequiredValues());
      assertTrue(snapshot.getPartitionColumns().isEmpty());
      assertEquals(1609775409819L, snapshot.getTimestamp());
    }

    @Test
    public void testCommitInfoStatsHasNumFiles() throws IOException {
      File f = FileUtils.getResourceAsFile("/deltalake/jsonLog_onlyNumFiles.json");
      Configuration conf = new Configuration();
      final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
      DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
      DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

      assertEquals(0L, snapshot.getNetBytesAdded());
      assertEquals(1824L, snapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, snapshot.getNetOutputRows());
      assertTrue(snapshot.isMissingRequiredValues());
    }

    @Test
    public void testCommitInfoStatsHasNumRecords() throws IOException {
      File f = FileUtils.getResourceAsFile("/deltalake/jsonLog_onlyNumRecords.json");
      Configuration conf = new Configuration();
      final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
      DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
      DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

      assertEquals(0L, snapshot.getNetBytesAdded());
      assertEquals(DremioCost.LARGE_FILE_COUNT, snapshot.getNetFilesAdded());
      assertEquals(1824L, snapshot.getNetOutputRows());
      assertTrue(snapshot.isMissingRequiredValues());
    }

    @Test
    public void testCommitInfoWithOpStats() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_1.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

        assertEquals("WRITE", snapshot.getOperationType());
        final String expectedSchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(expectedSchemaString, snapshot.getSchema());
        assertEquals(1208L, snapshot.getNetBytesAdded());
        assertEquals(1L, snapshot.getNetFilesAdded());
        assertEquals(0L, snapshot.getNetOutputRows());
        assertEquals(ImmutableList.of("ws_sold_date_sk"), snapshot.getPartitionColumns());
        assertEquals(1609775409819L, snapshot.getTimestamp());
    }

    @Test
    public void testMerge() throws Exception {
      Configuration conf = new Configuration();
      final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
      DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();

      File fileWithStats = FileUtils.getResourceAsFile("/deltalake/test1_1.json");
      File fileWithNumFilesOnly = FileUtils.getResourceAsFile("/deltalake/jsonLog_onlyNumFiles.json");
      File fileWithNumRecordsOnly = FileUtils.getResourceAsFile("/deltalake/jsonLog_onlyNumRecords.json");
      File fileNoStats = FileUtils.getResourceAsFile("/deltalake/test1_0.json");

      DeltaLogSnapshot snapshotWithStats = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(fileWithStats.toURI())))), -1);
      DeltaLogSnapshot snapshotWithNumFilesOnly = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(fileWithNumFilesOnly.toURI())))), -1);
      DeltaLogSnapshot snapshotWithNumRecordsOnly = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(fileWithNumRecordsOnly.toURI())))), -1);
      DeltaLogSnapshot snapshotNoStats = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(fileNoStats.toURI())))), -1);

      DeltaLogSnapshot mergedSnapshot = snapshotWithStats.clone();
      assertFalse(mergedSnapshot.isMissingRequiredValues());
      mergedSnapshot.merge(snapshotWithNumFilesOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(snapshotWithStats.getNetFilesAdded() + snapshotWithNumFilesOnly.getNetFilesAdded(), mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());
      mergedSnapshot.merge(snapshotWithNumRecordsOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());

      mergedSnapshot = snapshotWithStats.clone();
      mergedSnapshot.merge(snapshotWithNumRecordsOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(snapshotWithStats.getNetOutputRows() + snapshotWithNumRecordsOnly.getNetOutputRows(), mergedSnapshot.getNetOutputRows());
      mergedSnapshot.merge(snapshotWithNumFilesOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());

      mergedSnapshot = snapshotNoStats.clone();
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      mergedSnapshot.merge(snapshotWithNumFilesOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());

      mergedSnapshot = snapshotNoStats.clone();
      mergedSnapshot.merge(snapshotWithNumRecordsOnly);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());

      mergedSnapshot = snapshotNoStats.clone();
      mergedSnapshot.merge(snapshotWithStats);
      assertTrue(mergedSnapshot.isMissingRequiredValues());
      assertEquals(DremioCost.LARGE_FILE_COUNT, mergedSnapshot.getNetFilesAdded());
      assertEquals(DremioCost.LARGE_ROW_COUNT, mergedSnapshot.getNetOutputRows());
    }

    @Test
    public void testCommitInfoWithPartitionCols() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_2.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);
        assertEquals(Lists.newArrayList("intField", "stringField", "longField", "doubleField"), snapshot.getPartitionColumns());
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(schemaString, snapshot.getSchema());
    }

    @Test
    public void testCommitInfoAtLast() throws IOException {
      File f = FileUtils.getResourceAsFile("/deltalake/commitInfoAtLast.json");
      Configuration conf = new Configuration();
      final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
      DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
      DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);
      assertEquals(snapshot.getNetFilesAdded(),7);
      assertEquals(snapshot.getNetBytesAdded(),112657);
      assertEquals(snapshot.getNetOutputRows(), 2660);
    }

    @Test
    public void testFileWithoutMetadata() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_3.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

        assertEquals("WRITE", snapshot.getOperationType());
        assertNull(snapshot.getSchema());
        assertEquals(112657L, snapshot.getNetBytesAdded());
        assertEquals(7L, snapshot.getNetFilesAdded());
        assertEquals(2660L, snapshot.getNetOutputRows());
        assertTrue(snapshot.getPartitionColumns().isEmpty());
        assertEquals(1605801178171L, snapshot.getTimestamp());
    }

    @Test
    public void testFileWithoutProtocol() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_4.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);

        assertEquals("ADD COLUMNS", snapshot.getOperationType());
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"Year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayofMonth\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayOfWeek\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSDepTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSArrTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"UniqueCarrier\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"FlightNum\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TailNum\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ActualElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"AirTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Origin\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Dest\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Distance\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiIn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiOut\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Cancelled\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CancellationCode\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Diverted\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CarrierDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"WeatherDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"NASDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"SecurityDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"LateAircraftDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"newcol\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(schemaString, snapshot.getSchema());
        assertEquals(0L, snapshot.getNetBytesAdded());
        assertEquals(0L, snapshot.getNetFilesAdded());
        assertEquals(0L, snapshot.getNetOutputRows());
        assertEquals(Lists.newArrayList("DayOfWeek"), snapshot.getPartitionColumns());
        assertEquals(1605849831878L, snapshot.getTimestamp());
    }

    @Test(expected = IllegalStateException.class)
    public void testErrorOnIncompatibleProtocolVersion() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_5.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(null, null, fs, new ArrayList<>(Arrays.asList(fs.getFileAttributes(Path.of(f.toURI())))), -1);
    }
}
