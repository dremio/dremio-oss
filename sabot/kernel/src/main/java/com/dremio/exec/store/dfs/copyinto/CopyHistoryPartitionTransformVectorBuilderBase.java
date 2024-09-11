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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.iceberg.IcebergPartitionTransformTableFunction;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.util.VectorUtil;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types.NestedField;

/** Base class for building partition transform vectors for copy history system iceberg tables. */
public abstract class CopyHistoryPartitionTransformVectorBuilderBase {

  /**
   * Performs partition transformations on the given value vectors based on the partition spec.
   *
   * @param container The pre-filled vector container
   * @param vectors The list of value vectors to transform. Each vector in the list represent a
   *     column from the iceberg table that is involved in partitioning.
   * @param schema The schema of the iceberg table
   * @param partitionSpec The partition spec defining how to transform the values.
   */
  public static void transformValueVectors(
      VectorContainer container,
      List<ValueVector> vectors,
      Schema schema,
      PartitionSpec partitionSpec) {
    for (int i = 0; i < partitionSpec.fields().size(); i++) {
      PartitionField partitionField = partitionSpec.fields().get(i);
      Transform transform = partitionField.transform();
      NestedField sourceField = schema.findField(partitionField.sourceId());
      ValueVector valueVector = VectorUtil.getVectorFromSchemaPath(container, sourceField.name());
      ValueVector partitionValueVector = vectors.get(i);
      CompleteType completeType =
          container.getValueVectorId(SchemaPath.getSimplePath(sourceField.name())).getFinalType();
      for (int j = 0; j < valueVector.getValueCount(); j++) {
        Object transformedValue =
            transform
                .bind(sourceField.type())
                .apply(
                    IcebergPartitionTransformTableFunction.getValue(j, valueVector, completeType));
        IcebergUtils.writeToVector(partitionValueVector, j, transformedValue);
      }
      partitionValueVector.setValueCount(valueVector.getValueCount());
    }
  }
}
