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
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;

public class TestCopyJobHistoryTableRecordBuilder {

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = allocatorRule.newAllocator("test-copy-job-history-record-builder", 0, Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testBuildRecord() {
    int schemaVersion = 1;
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    CopyIntoErrorInfo info = buildInfo();
    long numSuccess = 5L;
    long numErrors = 2L;

    long timeMillis = System.currentTimeMillis();

    try (VectorContainer container = CopyJobHistoryTableRecordBuilder.buildVector(allocator, schemaVersion, info, numSuccess, numErrors)) {

      assertThat(container.getSchema().getFields().size()).isEqualTo(schema.columns().size());

        try (TimeStampMilliVector executedAtVector = (TimeStampMilliVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(1));
             VarCharVector jobIdVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(2));
             VarCharVector tableNameVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(3));
             BigIntVector recordsLoadedCountVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(4));
             BigIntVector recordsRejectedCountVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(5));
             VarCharVector copyOptionsVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(6));
             VarCharVector userNameVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(7));
             BigIntVector snapshotIdVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(8));
             VarCharVector storageLocationVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(9));
             VarCharVector fileFormatVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(10))
        ) {
            assertThat(executedAtVector.getValueCount()).isEqualTo(1);
            assertThat(executedAtVector.get(0)).isGreaterThanOrEqualTo(timeMillis);
            executedAtVector.clear();

            assertThat(jobIdVector.getValueCount()).isEqualTo(1);
            assertThat(jobIdVector.get(0)).isEqualTo(info.getQueryId().getBytes());
            jobIdVector.clear();

            assertThat(tableNameVector.getValueCount()).isEqualTo(1);
            assertThat(tableNameVector.get(0)).isEqualTo(info.getTableName().getBytes());
            tableNameVector.clear();

            assertThat(recordsLoadedCountVector.getValueCount()).isEqualTo(1);
            assertThat(recordsLoadedCountVector.get(0)).isEqualTo(numSuccess);
            recordsLoadedCountVector.clear();

            assertThat(recordsRejectedCountVector.getValueCount()).isEqualTo(1);
            assertThat(recordsRejectedCountVector.get(0)).isEqualTo(numErrors);
            recordsRejectedCountVector.clear();

            assertThat(copyOptionsVector.getValueCount()).isEqualTo(1);
            assertThat(copyOptionsVector.get(0)).isEqualTo(CopyIntoErrorInfo.Util.getJson(info.getFormatOptions()).getBytes());
            copyOptionsVector.clear();

            assertThat(userNameVector.getValueCount()).isEqualTo(1);
            assertThat(userNameVector.get(0)).isEqualTo(info.getQueryUser().getBytes());
            userNameVector.clear();

            assertThat(snapshotIdVector.getValueCount()).isEqualTo(1);
            assertThat(snapshotIdVector.get(0)).isEqualTo(info.getSnapshotId());
            snapshotIdVector.clear();

            assertThat(storageLocationVector.getValueCount()).isEqualTo(1);
            assertThat(storageLocationVector.get(0)).isEqualTo(info.getStorageLocation().getBytes());
            storageLocationVector.clear();

            assertThat(fileFormatVector.getValueCount()).isEqualTo(1);
            assertThat(fileFormatVector.get(0)).isEqualTo(info.getFileFormat().getBytes());
            fileFormatVector.clear();
        }
    }

  }

  @Test
  public void testBuildRecordForUnsupportedSchemaVersion() {
    assertThrows(UnsupportedOperationException.class, () -> CopyJobHistoryTableRecordBuilder.buildVector(allocator,
      2, buildInfo(), 5L, 2L));
  }

  private CopyIntoErrorInfo buildInfo() {
    return new CopyIntoErrorInfo.Builder(
      "be54a88a-3062-11ee-be56-0242ac120002",
      "testUser",
      "testTable",
      "@S3",
      "/path/to/file1",
      new ExtendedFormatOptions(),
      FileType.CSV.name(),
      CopyIntoErrorInfo.CopyIntoFileState.PARTIALLY_LOADED
    ).setSnapshotId(4686816577886312015L).build();
  }
}
