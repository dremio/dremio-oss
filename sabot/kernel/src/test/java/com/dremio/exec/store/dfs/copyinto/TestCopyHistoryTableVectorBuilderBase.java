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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.iceberg.IcebergPartitionTransformTableFunction;
import com.dremio.exec.util.VectorUtil;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class TestCopyHistoryTableVectorBuilderBase {
  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator =
        allocatorRule.newAllocator("test-copy-file-history-record-builder", 0, Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  protected List<CopyIntoFileLoadInfo> getFileLoadInfos(int recordCount) {
    List<CopyIntoFileLoadInfo> result = new ArrayList<>();
    String jobId = "be54a88a-3062-11ee-be56-0242ac120002";
    String queryUser = "testUser";
    String tableName = "testTable";
    String storageLocation = "@S3";
    String filePathTemplate = "/path/to/file_%s";
    Random random = new Random();
    for (int i = 0; i < recordCount; i++) {
      result.add(
          new CopyIntoFileLoadInfo.Builder(
                  jobId,
                  queryUser,
                  tableName,
                  storageLocation,
                  String.format(filePathTemplate, i),
                  new ExtendedFormatOptions(),
                  "JSON",
                  CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
              .setRecordsLoadedCount(random.nextLong())
              .setRecordsRejectedCount(random.nextLong() & Long.MAX_VALUE)
              .setPipeId("d3b8159e-d216-41c7-b4a0-a22ee33eb6df")
              .setFileSize(9999L)
              .setFirstErrorMessage("This is some error")
              .setFileNotificationTimestamp(System.currentTimeMillis())
              .setIngestionSourceType("AWS")
              .setRequestId("208a689d-339f-46dd-9185-1f3a59e924d3")
              .build());
    }
    return result;
  }

  protected CopyIntoFileLoadInfo getFileLoadInfo(long numSuccess, long numErrors) {
    return new CopyIntoFileLoadInfo.Builder(
            "be54a88a-3062-11ee-be56-0242ac120002",
            "testUser",
            "testTable",
            "@S3",
            "/path/to/file1",
            new ExtendedFormatOptions(),
            FileType.CSV.name(),
            CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
        .setRecordsLoadedCount(numSuccess)
        .setRecordsRejectedCount(numErrors)
        .setSnapshotId(4686816577886312015L)
        .setBranch("someBranch")
        .setPipeName("devPipe")
        .setPipeId("d2981263-fb78-4f83-a4e1-27ba02074bae")
        .build();
  }

  protected void assertPartitionVectors(PartitionSpec partitionSpec) {
    List<ValueVector> valueVectors =
        CopyFileHistoryPartitionTransformVectorBuilder.initializeValueVectors(
            getAllocator(), partitionSpec);
    assertThat(valueVectors.size()).isEqualTo(partitionSpec.fields().size());
    IntStream.range(0, valueVectors.size())
        .forEach(i -> assertThat(valueVectors.get(i).getValueCount()).isEqualTo(0));
  }

  protected void assertTransformationVectors(
      VectorContainer container,
      List<ValueVector> valueVectors,
      Schema schema,
      PartitionSpec partitionSpec) {
    for (int i = 0; i < partitionSpec.fields().size(); i++) {
      PartitionField partitionField = partitionSpec.fields().get(i);
      ValueVector v = valueVectors.get(i);
      assertThat(v.getName()).isEqualTo(partitionField.name());
      NestedField sourceField = schema.findField(partitionField.sourceId());
      ValueVector sourceVector = VectorUtil.getVectorFromSchemaPath(container, sourceField.name());
      CompleteType sourceVectorType =
          container
              .getValueVectorId(SchemaPath.getSimplePath(sourceVector.getName()))
              .getFinalType();
      assertThat(sourceVector).isNotNull();
      Transform transform = partitionField.transform();
      assertThat(transform).isNotNull();
      for (int j = 0; j < sourceVector.getValueCount(); j++) {
        assertThat(v.getObject(j))
            .isEqualTo(
                transform
                    .bind(sourceField.type())
                    .apply(
                        IcebergPartitionTransformTableFunction.getValue(
                            j, sourceVector, sourceVectorType)));
      }
    }
  }

  protected VectorContainer buildVector(
      Schema schema, List<CopyIntoFileLoadInfo> copyIntoFileLoadInfos, long timeMillis) {
    VectorContainer container =
        CopyFileHistoryTableVectorContainerBuilder.initializeContainer(getAllocator(), schema);
    assertThat(container.getSchema().getFieldCount()).isEqualTo(schema.columns().size());
    copyIntoFileLoadInfos.forEach(
        i ->
            CopyFileHistoryTableVectorContainerBuilder.writeRecordToContainer(
                container, i, timeMillis));
    return container;
  }

  protected VectorContainer buildVector(
      Schema schema, CopyIntoFileLoadInfo info, long numSuccess, long numErrors, long timeMillis) {
    VectorContainer container =
        CopyJobHistoryTableVectorContainerBuilder.initializeContainer(getAllocator(), schema);
    CopyJobHistoryTableVectorContainerBuilder.writeRecordToContainer(
        container, info, numSuccess, numErrors, 0L, timeMillis);
    return container;
  }
}
