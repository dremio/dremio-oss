/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.WritePartition;

class PartitionWriteManager {

  private final BitVector.Accessor changeVector;
  private final IntVector.Accessor bucketNumber;

  private final List<ValueVector.Accessor> partitions = new ArrayList<>();

  private final VectorContainer maskedContainer;

  private WritePartition partition;

  public PartitionWriteManager(WriterOptions options, VectorAccessible incoming){
    // if we're a partitioning writer, we'll need to remove the partition number column.
    final Set<Integer> maskedIds = new HashSet<>();

    final TypedFieldId changeDetectionField = incoming.getValueVectorId(SchemaPath.getSimplePath(WriterPrel.PARTITION_COMPARATOR_FIELD));
    if (changeDetectionField != null) {
      maskedIds.add(changeDetectionField.getFieldIds()[0]);
      changeVector = incoming.getValueAccessorById(NullableBitVector.class, changeDetectionField.getFieldIds()).getValueVector().getValuesVector().getAccessor();
    } else {
      throw new IllegalArgumentException("Incoming schema didn't include change detection column even though writer was configured for partitioning.");
    }

    if(options.hasDistributions()){
      final TypedFieldId bucketNumberField = incoming.getValueVectorId(SchemaPath.getSimplePath(WriterPrel.BUCKET_NUMBER_FIELD));
      if (bucketNumberField != null) {
        maskedIds.add(bucketNumberField.getFieldIds()[0]);
        bucketNumber = incoming.getValueAccessorById(NullableIntVector.class, bucketNumberField.getFieldIds()).getValueVector().getValuesVector().getAccessor();
      } else {
        throw new IllegalArgumentException("Incoming schema didn't include partitions even though writer was configured for partitioning.");
      }
    } else {
      bucketNumber = null;
    }


    for(String column : options.getPartitionColumns()){
      final TypedFieldId partitionValueField = incoming.getValueVectorId(SchemaPath.getSimplePath(column));
      if (partitionValueField != null) {
        partitions.add(incoming.getValueAccessorById(ValueVector.class, partitionValueField.getFieldIds()).getValueVector().getAccessor());
      } else {
        throw new IllegalArgumentException("Incoming schema didn't include partitions even though writer was configured for partitioning.");
      }
    }

    maskedContainer = new VectorContainer();
    int id = 0;
    for(VectorWrapper<?> wrapper : incoming){
      if(maskedIds.contains(id)){
        continue;
      }
      maskedContainer.add(wrapper.getValueVector());
      id++;
    }
    maskedContainer.buildSchema();
  }

  public VectorContainer getMaskedContainer() {
    return maskedContainer;
  }

  public WritePartition getExistingOrNewPartition(int offset){
    if(changeVector.get(offset) == 1){
      partition = getPartition(offset);
    }
    return partition;
  }

  private WritePartition getPartition(int offset){
    String[] paths = new String[partitions.size()];
    for(int i = 0; i < paths.length; i++){
      paths[i] = fromObj(partitions.get(i).getObject(offset));
    }

    final Integer bucketNumberValue = bucketNumber == null ? null : bucketNumber.get(offset);

    return new WritePartition(paths, bucketNumberValue);
  }


  public static String fromObj(Object obj){
    if(obj == null){
      return "null";
    } else {
      String value = obj.toString().replaceAll("\\W+", "_");
      if(value.length() > 24){
        return value.substring(0, 24);
      } else {
        return value;
      }
    }
  }
}
