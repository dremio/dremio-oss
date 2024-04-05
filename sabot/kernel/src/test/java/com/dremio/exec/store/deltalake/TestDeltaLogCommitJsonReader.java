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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/** Tests for {@link DeltaLogCommitJsonReader} */
public class TestDeltaLogCommitJsonReader extends BaseTestQuery {

  public static DeltaLogSnapshot parseCommitJson(String fileName) throws IOException {
    Configuration conf = new Configuration();
    final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.getLocal(conf));
    final SabotContext context = getSabotContext();
    DeltaLogCommitJsonReader jsonReader = new DeltaLogCommitJsonReader();
    return jsonReader.parseMetadata(
        null,
        context,
        fs,
        new ArrayList<>(
            Arrays.asList(
                fs.getFileAttributes(Path.of(FileUtils.getResourceAsFile(fileName).toURI())))),
        -1);
  }

  @Test
  public void testCommitInfoWithoutOpStats() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_0.json");

    assertEquals(DeltaConstants.OPERATION_WRITE, snapshot.getOperationType());
    final String expectedSchemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(0L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(ImmutableList.of("ws_sold_date_sk"), snapshot.getPartitionColumns());
    assertEquals(1609775409819L, snapshot.getTimestamp());
  }

  @Test
  public void testCommitInfoWithoutOpStatsOnlySchemaChange() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_0_noadd.json");

    assertEquals(DeltaConstants.OPERATION_WRITE, snapshot.getOperationType());
    final String expectedSchemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(0L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertTrue(snapshot.getPartitionColumns().isEmpty());
    assertEquals(1609775409819L, snapshot.getTimestamp());
  }

  @Test
  public void testCommitInfoStatsHasNumFiles() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/jsonLog_onlyNumFiles.json");

    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(1824L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetOutputRows());
  }

  @Test
  public void testCommitInfoStatsHasNumRecords() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/jsonLog_onlyNumRecords.json");

    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(0L, snapshot.getNetFilesAdded());
    assertEquals(1824L, snapshot.getNetOutputRows());
  }

  @Test
  public void testCommitInfoWithOpStats() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_1.json");

    assertEquals(DeltaConstants.OPERATION_WRITE, snapshot.getOperationType());
    final String expectedSchemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedSchemaString, snapshot.getSchema());
    assertEquals(1208L, snapshot.getNetBytesAdded());
    assertEquals(1L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(ImmutableList.of("ws_sold_date_sk"), snapshot.getPartitionColumns());
    assertEquals(1609775409819L, snapshot.getTimestamp());
  }

  @Test
  public void testCommitInfoWithPartitionCols() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_2.json");

    assertEquals(
        Lists.newArrayList("intField", "stringField", "longField", "doubleField"),
        snapshot.getPartitionColumns());
    final String schemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"byteField\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"shortField\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"intField\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"longField\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floatField\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"doubleField\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"stringField\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"binaryField\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}},{\"name\":\"booleanField\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestampField\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dateField\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(schemaString, snapshot.getSchema());
  }

  @Test
  public void testCommitInfoAtLast() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/commitInfoAtLast.json");

    assertEquals(snapshot.getNetFilesAdded(), 7);
    assertEquals(snapshot.getNetBytesAdded(), 112657);
    assertEquals(snapshot.getNetOutputRows(), 2660);
  }

  @Test
  public void testFileWithoutMetadata() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_3.json");

    assertEquals(DeltaConstants.OPERATION_WRITE, snapshot.getOperationType());
    assertNull(snapshot.getSchema());
    assertEquals(112657L, snapshot.getNetBytesAdded());
    assertEquals(7L, snapshot.getNetFilesAdded());
    assertEquals(2660L, snapshot.getNetOutputRows());
    assertTrue(snapshot.getPartitionColumns().isEmpty());
    assertEquals(1605801178171L, snapshot.getTimestamp());
  }

  @Test
  public void testFileWithoutProtocol() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_4.json");

    assertEquals(DeltaConstants.OPERATION_ADD_COLUMNS, snapshot.getOperationType());
    final String schemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"Year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayofMonth\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DayOfWeek\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSDepTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSArrTime\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"UniqueCarrier\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"FlightNum\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TailNum\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ActualElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CRSElapsedTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"AirTime\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ArrDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"DepDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Origin\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Dest\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Distance\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiIn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"TaxiOut\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Cancelled\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CancellationCode\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"Diverted\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"CarrierDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"WeatherDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"NASDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"SecurityDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"LateAircraftDelay\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"newcol\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(schemaString, snapshot.getSchema());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(0L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(Lists.newArrayList("DayOfWeek"), snapshot.getPartitionColumns());
    assertEquals(1605849831878L, snapshot.getTimestamp());
  }

  @Test
  public void testErrorOnIncompatibleProtocolVersion() throws Exception {
    try (AutoCloseable ac =
        withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, false)) {
      IOException exception =
          assertThrows(IOException.class, () -> parseCommitJson("/deltalake/test1_5.json"));
      assertEquals("Protocol version 2 is incompatible for Dremio plugin", exception.getMessage());
    }
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test1_5.json");
    }
  }

  @Test
  public void testCommitInfoOperationOptimize() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test_operations/0003_optimize.json");

    assertEquals(DeltaConstants.OPERATION_OPTIMIZE, snapshot.getOperationType());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(-3L, snapshot.getNetFilesAdded());
    assertEquals(-2028L, snapshot.getNetBytesAdded());
    assertEquals(5L, snapshot.getDataFileEntryCount());
  }

  @Test
  public void testCommitInfoOperationDeleteWhere() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test_operations/0005_delete.json");

    assertEquals(DeltaConstants.OPERATION_DELETE, snapshot.getOperationType());
    assertEquals(-2L, snapshot.getNetOutputRows());
    assertEquals(-1L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(3L, snapshot.getDataFileEntryCount());
  }

  @Test
  public void testCommitInfoOperationDeleteAll() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test_operations/0008_delete.json");

    assertEquals(DeltaConstants.OPERATION_DELETE, snapshot.getOperationType());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(-1L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(1L, snapshot.getDataFileEntryCount());
  }

  @Test
  public void testCommitInfoOperationMerge() throws IOException {
    DeltaLogSnapshot snapshot = parseCommitJson("/deltalake/test_operations/0007_merge.json");

    assertEquals(DeltaConstants.OPERATION_MERGE, snapshot.getOperationType());
    assertEquals(0L, snapshot.getNetOutputRows());
    assertEquals(-1L, snapshot.getNetFilesAdded());
    assertEquals(0L, snapshot.getNetBytesAdded());
    assertEquals(3L, snapshot.getDataFileEntryCount());
  }

  interface TestMergeFunction {
    void run(
        String fileName,
        long netOutputRows,
        long netFilesAdded,
        long netBytesAdded,
        long totalFileEntries)
        throws IOException;
  }

  @Test
  public void testMergeCommitInfoWithSetOfOperations() throws IOException {
    final String rootDir = "/deltalake/test_operations/";
    DeltaLogSnapshot combinedSnapshot = new DeltaLogSnapshot();
    TestMergeFunction test =
        (fileName, netOutputRows, netFilesAdded, netBytesAdded, totalFileEntries) -> {
          combinedSnapshot.merge(parseCommitJson(rootDir + fileName));
          assertEquals(netOutputRows, combinedSnapshot.getNetOutputRows());
          assertEquals(netFilesAdded, combinedSnapshot.getNetFilesAdded());
          assertEquals(netBytesAdded, combinedSnapshot.getNetBytesAdded());
          assertEquals(totalFileEntries, combinedSnapshot.getDataFileEntryCount());
        };
    test.run("0000_create.json", 0L, 0L, 0L, 0L);
    test.run("0001_write.json", 2L, 2L, 1368L, 2L);
    test.run("0002_write.json", 4L, 4L, 2736L, 4L);
    test.run("0003_optimize.json", 4L, 1L, 708L, 9L);
    test.run("0004_write.json", 6L, 3L, 2076L, 11L);
    test.run("0005_delete.json", 4L, 2L, 2076L, 14L);
    test.run("0006_ctas.json", 2L, 2L, 1368L, 2L);
    test.run("0007_merge.json", 2L, 1L, 1368L, 5L);
    test.run("0008_delete.json", 0L, 0L, 0L, 0L);
  }

  @Test
  public void testCommitInfoWithColumnMapping() throws Exception {
    try (AutoCloseable ignore =
        withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      DeltaLogSnapshot snapshot =
          parseCommitJson("/deltalake/columnMapping/_delta_log/00000000000000000000.json");

      assertEquals(DeltaConstants.OPERATION_CREATE_OR_REPLACE_TABLE, snapshot.getOperationType());
      assertEquals(DeltaColumnMappingMode.NAME, snapshot.getColumnMappingMode());
      assertEquals(0L, snapshot.getNetBytesAdded());
      assertEquals(0L, snapshot.getNetFilesAdded());
      assertEquals(0L, snapshot.getNetOutputRows());
      assertEquals(1697855825248L, snapshot.getTimestamp());
    }
  }

  @Test
  public void testCommitInfoWithConvertedIceberg() throws Exception {
    try (AutoCloseable ignore =
        withSystemOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING, true)) {
      DeltaLogSnapshot snapshot =
          parseCommitJson(
              "/deltalake/columnMappingConvertedIceberg/_delta_log/00000000000000000000.json");

      assertEquals("CONVERT", snapshot.getOperationType());
      assertEquals(DeltaColumnMappingMode.ID, snapshot.getColumnMappingMode());
      assertEquals(0L, snapshot.getNetBytesAdded());
      assertEquals(0L, snapshot.getNetFilesAdded());
      assertEquals(4L, snapshot.getNetOutputRows());
      assertEquals(1699368782067L, snapshot.getTimestamp());
    }
  }
}
