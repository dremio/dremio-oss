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
import java.util.List;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Test;

public class TestCopyFileHistoryTableVectorContainerBuilder
    extends TestCopyHistoryTableVectorBuilderBase {

  @Test
  public void testContainerWrite() {
    long timeMillis = System.currentTimeMillis();
    List<CopyIntoFileLoadInfo> copyIntoFileLoadInfos = getFileLoadInfos(1000);
    // V1 schema
    assertContainerContent(1L, copyIntoFileLoadInfos, timeMillis);
    // v2 schema
    assertContainerContent(2L, copyIntoFileLoadInfos, timeMillis);
  }

  @Test
  public void testInitializeContainer() {
    assertContainerSchema(1);
    assertContainerSchema(2);
  }

  private void assertContainerSchema(int schemaVersion) {
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    try (VectorContainer container =
        CopyFileHistoryTableVectorContainerBuilder.initializeContainer(getAllocator(), schema)) {
      assertThat(container.getSchema().getFieldCount()).isEqualTo(schema.columns().size());
      assertThat(container.getRecordCount()).isEqualTo(0);
    }
  }

  private void assertContainerContent(
      long schemaVersion, List<CopyIntoFileLoadInfo> fileLoadInfos, long timestamp) {
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    try (VectorContainer container = buildVector(schema, fileLoadInfos, timestamp)) {
      assertThat(container.getRecordCount()).isEqualTo(fileLoadInfos.size());
      int recordCount = fileLoadInfos.size();
      for (NestedField col : schema.columns()) {
        try (ValueVector vector = VectorUtil.getVectorFromSchemaPath(container, col.name())) {
          assertThat(vector.getValueCount()).isEqualTo(recordCount);
          switch (col.fieldId()) {
            case 1:
              IntStream.range(0, recordCount)
                  .forEach(
                      i -> assertThat(((TimeStampMilliVector) vector).get(i)).isEqualTo(timestamp));
              break;
            case 2:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getQueryId().getBytes()));
              break;
            case 3:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getFilePath().getBytes()));
              break;
            case 4:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getFileState().name().getBytes()));
              break;
            case 5:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((BigIntVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getRecordsLoadedCount()));
              break;
            case 6:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((BigIntVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getRecordsRejectedCount()));
              break;
            case 7:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getPipeId().getBytes()));
              break;
            case 8:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((BigIntVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getFileSize()));
              break;
            case 9:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getFirstErrorMessage().getBytes()));
              break;
            case 10:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((TimeStampMilliVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getFileNotificationTimestamp()));
              break;
            case 11:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getIngestionSourceType().getBytes()));
              break;
            case 12:
              IntStream.range(0, recordCount)
                  .forEach(
                      i ->
                          assertThat(((VarCharVector) vector).get(i))
                              .isEqualTo(fileLoadInfos.get(i).getRequestId().getBytes()));
              break;
            default:
              fail(String.format("Unrecognized field Id %s", col.fieldId()));
          }
        }
      }
    }
  }
}
