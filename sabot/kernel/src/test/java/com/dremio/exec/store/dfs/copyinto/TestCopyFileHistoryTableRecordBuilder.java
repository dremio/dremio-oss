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
package com.dremio.exec.store.dfs.copyinto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoErrorInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.VectorUtil;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;

public class TestCopyFileHistoryTableRecordBuilder {

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = allocatorRule.newAllocator("test-copy-file-history-record-builder", 0, Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testBuildRecordV1Schema() {
    int schemaVersion = 1;
    long timeMillis = System.currentTimeMillis();
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    int recordCount = 1000;
    List<CopyIntoErrorInfo> copyErrorInfos = getTestData(recordCount);
    try (VectorContainer container = CopyFileHistoryTableRecordBuilder.buildVector(allocator, schemaVersion, copyErrorInfos)) {
      assertThat(container.getSchema().getFieldCount()).isEqualTo(schema.columns().size());
      assertThat(container.getRecordCount()).isEqualTo(recordCount);

        try (TimeStampMilliVector eventTimestampVector = (TimeStampMilliVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(1));
             VarCharVector jobIdVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(2));
             VarCharVector filePathsVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(3));
             VarCharVector fileStatesVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(4));
             BigIntVector recordsLoadedVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(5));
             BigIntVector recordsRejectedVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(6))) {

            assertThat(eventTimestampVector.getValueCount()).isEqualTo(recordCount);
            assertThat(jobIdVector.getValueCount()).isEqualTo(recordCount);
            assertThat(filePathsVector.getValueCount()).isEqualTo(recordCount);
            assertThat(filePathsVector.getValueCount()).isEqualTo(recordCount);
            assertThat(recordsLoadedVector.getValueCount()).isEqualTo(recordCount);
            assertThat(recordsRejectedVector.getValueCount()).isEqualTo(recordCount);

            for (int i = 0; i < recordCount; i++) {
                CopyIntoErrorInfo errorInfo = copyErrorInfos.get(i);
                assertThat(eventTimestampVector.get(i)).isGreaterThanOrEqualTo(timeMillis);
                assertThat(jobIdVector.get(i)).isEqualTo(errorInfo.getQueryId().getBytes());
                assertThat(filePathsVector.get(i)).isEqualTo(errorInfo.getFilePath().getBytes());
                assertThat(fileStatesVector.get(i)).isEqualTo(errorInfo.getFileState().name().getBytes());
                assertThat(recordsLoadedVector.get(i)).isEqualTo(errorInfo.getRecordsLoadedCount());
                assertThat(recordsRejectedVector.get(i)).isEqualTo(errorInfo.getRecordsRejectedCount());
            }
        }
    }
  }

  @Test
  public void testBuildRecordV1SchemaEmptyInput() {
    int schemaVersion = 1;
    List<CopyIntoErrorInfo> copyErrorInfos = new ArrayList<>();
    try (VectorContainer container = CopyFileHistoryTableRecordBuilder.buildVector(allocator, schemaVersion, copyErrorInfos)) {
      assertThat(container.getRecordCount()).isEqualTo(0);
    }
  }

  @Test
  public void testBuildRecordForUnsupportedSchemaVersion() {
    assertThrows(UnsupportedOperationException.class, () -> CopyFileHistoryTableRecordBuilder.buildVector(allocator,
      2, ImmutableList.of()));
  }

  private List<CopyIntoErrorInfo> getTestData(int recordCount) {
    List<CopyIntoErrorInfo> result = new ArrayList<>();
    String jobId = "be54a88a-3062-11ee-be56-0242ac120002";
    String queryUser = "testUser";
    String tableName = "testTable";
    String storageLocation = "@S3";
    String filePathTemplate = "/path/to/file_%s";
    Random random = new Random();
    for (int i = 0; i < recordCount; i++) {
      result.add(new CopyIntoErrorInfo.Builder(jobId, queryUser, tableName, storageLocation,
        String.format(filePathTemplate, i), new ExtendedFormatOptions(), "JSON", CopyIntoErrorInfo.CopyIntoFileState.PARTIALLY_LOADED)
        .setRecordsLoadedCount(random.nextLong()).setRecordsRejectedCount(random.nextLong() & Long.MAX_VALUE).build());
    }
    return result;
  }
}
