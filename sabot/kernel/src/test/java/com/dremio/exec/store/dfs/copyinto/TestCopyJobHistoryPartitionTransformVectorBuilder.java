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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.record.VectorContainer;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.Test;

public class TestCopyJobHistoryPartitionTransformVectorBuilder
    extends TestCopyHistoryTableVectorBuilderBase {
  @Test
  public void testInitializeVectors() {
    LongStream.range(0, ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal())
        .forEach(
            i ->
                assertPartitionVectors(
                    new CopyJobHistoryTableSchemaProvider(i + 1).getPartitionSpec()));
  }

  @Test
  public void testTransformVectors() {
    LongStream.range(0, ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal())
        .forEach(i -> assertTransformation(i + 1));
  }

  private void assertTransformation(long schemaVersion) {
    long currentTimeMillis = System.currentTimeMillis();
    long numSuccess = 5L;
    long numErrors = 2L;
    CopyIntoFileLoadInfo info = getFileLoadInfo(numSuccess, numErrors);
    CopyJobHistoryTableSchemaProvider schemaProvider =
        new CopyJobHistoryTableSchemaProvider(schemaVersion);
    Schema schema = schemaProvider.getSchema();
    PartitionSpec partitionSpec = schemaProvider.getPartitionSpec();
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
