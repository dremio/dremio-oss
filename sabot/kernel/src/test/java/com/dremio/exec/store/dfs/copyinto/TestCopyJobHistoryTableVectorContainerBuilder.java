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
import static org.junit.Assert.fail;

import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.VectorUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.Schema;
import org.junit.Test;

public class TestCopyJobHistoryTableVectorContainerBuilder
    extends TestCopyHistoryTableVectorBuilderBase {

  @Test
  public void testContainerWrite() {
    long numSuccess = 5L;
    long numErrors = 2L;
    long timeMillis = System.currentTimeMillis();
    CopyIntoFileLoadInfo info = getFileLoadInfo(numSuccess, numErrors);
    // V1 schema
    assertContainerContent(1L, info, numSuccess, numErrors, timeMillis);
    // V2 schema
    assertContainerContent(2L, info, numSuccess, numErrors, timeMillis);
  }

  private void assertContainerContent(
      long schemaVersion,
      CopyIntoFileLoadInfo info,
      long numSuccess,
      long numErrors,
      long timeMillis) {
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    try (VectorContainer container = buildVector(schema, info, numSuccess, numErrors, timeMillis)) {
      assertThat(container.getSchema().getFieldCount()).isEqualTo(schema.columns().size());
      for (int i = 1; i <= container.getSchema().getFieldCount(); i++) {
        try (ValueVector vector =
            VectorUtil.getVectorFromSchemaPath(container, schema.findColumnName(i))) {
          assertThat(vector.getValueCount()).isEqualTo(1);
          switch (i) {
            case 1:
              // executed_at
              assertThat(((TimeStampMilliVector) vector).get(0)).isEqualTo(timeMillis);
              break;
            case 2:
              // job_id
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getQueryId().getBytes());
              break;
            case 3:
              // table_name
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getTableName().getBytes());
              break;
            case 4:
              // records_loaded_count
              assertThat(((BigIntVector) vector).get(0)).isEqualTo(info.getRecordsLoadedCount());
              break;
            case 5:
              // records_rejected_count
              assertThat(((BigIntVector) vector).get(0)).isEqualTo(info.getRecordsRejectedCount());
              break;
            case 6:
              // copy_options
              assertThat(((VarCharVector) vector).get(0))
                  .isEqualTo(CopyIntoFileLoadInfo.Util.getJson(info.getFormatOptions()).getBytes());
              break;
            case 7:
              // user_name
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getQueryUser().getBytes());
              break;
            case 8:
              // base_snapshot_id
              assertThat(((BigIntVector) vector).get(0)).isEqualTo(info.getSnapshotId());
              break;
            case 9:
              // storage_location
              assertThat(((VarCharVector) vector).get(0))
                  .isEqualTo(info.getStorageLocation().getBytes());
              break;
            case 10:
              // file_format
              assertThat(((VarCharVector) vector).get(0))
                  .isEqualTo(info.getFileFormat().getBytes());
              break;
            case 11:
              // branch
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getBranch().getBytes());
              break;
            case 12:
              // pipe_name
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getPipeName().getBytes());
              break;
            case 13:
              // pipe_id
              assertThat(((VarCharVector) vector).get(0)).isEqualTo(info.getPipeId().getBytes());
              break;
            case 14:
              // time_elapsed
              assertThat(((BigIntVector) vector).get(0)).isEqualTo(info.getProcessingStartTime());
              break;
            default:
              fail("Unrecognized vector column");
          }
        }
      }
    }
  }

  @Test
  public void testInitializeContainer() {
    assertContainerSchema(1);
    assertContainerSchema(2);
  }

  private void assertContainerSchema(int schemaVersion) {
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    try (VectorContainer container =
        CopyJobHistoryTableVectorContainerBuilder.initializeContainer(getAllocator(), schema)) {
      assertThat(container.getSchema().getFieldCount()).isEqualTo(schema.columns().size());
      assertThat(container.getRecordCount()).isEqualTo(0);
    }
  }
}
