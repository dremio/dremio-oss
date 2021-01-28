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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
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
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));

        assertEquals("WRITE", snapshot.getOperationType());
        final String expectedSchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(expectedSchemaString, snapshot.getSchema());
        assertEquals(0L, snapshot.getNetBytesAdded());
        assertEquals(0L, snapshot.getNetFilesAdded());
        assertEquals(0L, snapshot.getNetOutputRows());
        assertEquals(-1L, snapshot.getVersionId()); // not written for first file
        assertTrue(snapshot.getPartitionColumns().isEmpty());
        assertEquals(1609775409819L, snapshot.getTimestamp());
    }

    @Test
    public void testCommitInfoWithOpStats() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_1.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));

        assertEquals("WRITE", snapshot.getOperationType());
        final String expectedSchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(expectedSchemaString, snapshot.getSchema());
        assertEquals(1208L, snapshot.getNetBytesAdded());
        assertEquals(1L, snapshot.getNetFilesAdded());
        assertEquals(0L, snapshot.getNetOutputRows());
        assertEquals(-1L, snapshot.getVersionId()); // not written for first file
        assertTrue(snapshot.getPartitionColumns().isEmpty());
        assertEquals(1609775409819L, snapshot.getTimestamp());
    }

    @Test
    public void testCommitInfoWithPartitionCols() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_2.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));
        assertEquals(Lists.newArrayList("intField", "stringField", "longField", "doubleField"), snapshot.getPartitionColumns());
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(schemaString, snapshot.getSchema());
    }

    @Test
    public void testFileWithoutMetadata() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_3.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));

        assertEquals("WRITE", snapshot.getOperationType());
        assertNull(snapshot.getSchema());
        assertEquals(112657L, snapshot.getNetBytesAdded());
        assertEquals(7L, snapshot.getNetFilesAdded());
        assertEquals(2660L, snapshot.getNetOutputRows());
        assertEquals(0L, snapshot.getVersionId()); // not written for first file
        assertTrue(snapshot.getPartitionColumns().isEmpty());
        assertEquals(1605801178171L, snapshot.getTimestamp());
    }

    @Test
    public void testFileWithoutProtocol() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_4.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));

        assertEquals("ADD COLUMNS", snapshot.getOperationType());
        final String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"Year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayofMonth\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayOfWeek\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSDepTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSArrTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"UniqueCarrier\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"FlightNum\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TailNum\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ActualElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"AirTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Origin\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Dest\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Distance\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiIn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiOut\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Cancelled\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CancellationCode\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Diverted\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CarrierDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"WeatherDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"NASDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"SecurityDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"LateAircraftDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"newcol\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
        assertEquals(schemaString, snapshot.getSchema());
        assertEquals(0L, snapshot.getNetBytesAdded());
        assertEquals(0L, snapshot.getNetFilesAdded());
        assertEquals(0L, snapshot.getNetOutputRows());
        assertEquals(21L, snapshot.getVersionId());
        assertEquals(Lists.newArrayList("DayOfWeek"), snapshot.getPartitionColumns());
        assertEquals(1605849831878L, snapshot.getTimestamp());
    }

    @Test(expected = IllegalStateException.class)
    public void testErrorOnIncompatibleProtocolVersion() throws IOException {
        File f = FileUtils.getResourceAsFile("/deltalake/test1_5.json");
        Configuration conf = new Configuration();
        final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
        DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
        DeltaLogSnapshot snapshot = jsonReader.parseMetadata(fs, Path.of(f.toURI()));
    }
}
