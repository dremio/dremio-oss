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

import java.util.LinkedList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;

/** Builds partition transform vectors specifically for copy job history. */
public class CopyJobHistoryPartitionTransformVectorBuilder
    extends CopyHistoryPartitionTransformVectorBuilderBase {

  private CopyJobHistoryPartitionTransformVectorBuilder() {
    // no-op
  }

  /**
   * Initializes the partition value vectors based on the partition spec.
   *
   * @param allocator The buffer allocator to use for vector allocation.
   * @param partitionSpec The partition spec defining the partition vector types.
   * @return The list of initialized value vectors.
   * @throws UnsupportedOperationException If an unrecognized partition column is encountered.
   */
  public static List<ValueVector> initializeValueVectors(
      BufferAllocator allocator, PartitionSpec partitionSpec) {
    List<ValueVector> vectors = new LinkedList<>();
    if (partitionSpec.isPartitioned()) {
      for (PartitionField partitionField : partitionSpec.fields()) {
        switch (partitionField.fieldId()) {
          case 1000:
            vectors.add(new IntVector(partitionField.name(), allocator));
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unrecognized copy_job_history partition column with id %s and name %s",
                    partitionField.fieldId(), partitionField.name()));
        }
      }
    }
    return vectors;
  }
}
