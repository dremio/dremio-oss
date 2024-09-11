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

import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.Test;

public class TestCopyJobHistoryPartitionTransformVectorBuilder
    extends TestCopyHistoryTableVectorBuilderBase {
  @Test
  public void testInitializeVectors() {
    assertPartitionVectors(CopyJobHistoryTableSchemaProvider.getPartitionSpec(1L));
    assertPartitionVectors(CopyJobHistoryTableSchemaProvider.getPartitionSpec(2L));
  }

  @Test
  public void testTransformVectors() {
    assertTransformation(1L);
    assertTransformation(2L);
  }

  private void assertTransformation(long schemaVersion) {
    long currentTimeMillis = System.currentTimeMillis();
    long numSuccess = 5L;
    long numErrors = 2L;
    CopyIntoFileLoadInfo info = getFileLoadInfo(numSuccess, numErrors);
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    PartitionSpec partitionSpec = CopyJobHistoryTableSchemaProvider.getPartitionSpec(schemaVersion);
    try (VectorContainer container =
        buildVector(schema, info, numSuccess, numErrors, currentTimeMillis)) {
      List<ValueVector> valueVectors =
          CopyJobHistoryPartitionTransformVectorBuilder.initializeValueVectors(
              getAllocator(), partitionSpec);
      CopyJobHistoryPartitionTransformVectorBuilder.transformValueVectors(
          container, valueVectors, schema, partitionSpec);
      assertTransformationVectors(container, valueVectors, schema, partitionSpec);
      valueVectors.forEach(ValueVector::close);
    }
  }
}
