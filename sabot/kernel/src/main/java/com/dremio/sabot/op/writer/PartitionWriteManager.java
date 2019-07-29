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
package com.dremio.sabot.op.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
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
  /**
   * Directory names used when the partition value is null or empty. We need a special value to have a valid
   * directory name (empty strings are not allowed) and a way to differentiate between null and empty string later on
   * using case statements on dirX columns.
   */
  public static final String NULL_PARTITION = "DREMIO_DEFAULT_NULL_PARTITION__";
  public static final String EMPTY_VALUE_PARTITION = "DREMIO_DEFAULT_EMPTY_VALUE_PARTITION__";

  private final BitVector changeVector;
  private final IntVector bucketNumber;

  private final List<ValueVector> partitions = new ArrayList<>();

  private final VectorContainer maskedContainer;

  private WritePartition partition;

  public PartitionWriteManager(WriterOptions options, VectorAccessible incoming){
    // if we're a partitioning writer, we'll need to remove the partition number column.
    final Set<Integer> maskedIds = new HashSet<>();

    final TypedFieldId changeDetectionField = incoming.getValueVectorId(SchemaPath.getSimplePath(WriterPrel.PARTITION_COMPARATOR_FIELD));
    if (changeDetectionField != null) {
      maskedIds.add(changeDetectionField.getFieldIds()[0]);
      changeVector = incoming.getValueAccessorById(BitVector.class, changeDetectionField.getFieldIds()).getValueVector();
    } else {
      throw new IllegalArgumentException("Incoming schema didn't include change detection column even though writer was configured for partitioning.");
    }

    if(options.hasDistributions()){
      final TypedFieldId bucketNumberField = incoming.getValueVectorId(SchemaPath.getSimplePath(WriterPrel.BUCKET_NUMBER_FIELD));
      if (bucketNumberField != null) {
        maskedIds.add(bucketNumberField.getFieldIds()[0]);
        bucketNumber = incoming.getValueAccessorById(IntVector.class, bucketNumberField.getFieldIds()).getValueVector();
      } else {
        throw new IllegalArgumentException("Incoming schema didn't include partitions even though writer was configured for partitioning.");
      }
    } else {
      bucketNumber = null;
    }


    for(String column : options.getPartitionColumns()){
      final TypedFieldId partitionValueField = incoming.getValueVectorId(SchemaPath.getSimplePath(column));
      if (partitionValueField != null) {
        partitions.add(incoming.getValueAccessorById(ValueVector.class, partitionValueField.getFieldIds()).getValueVector());
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
      return NULL_PARTITION;
    } else {
      // Decimal can have maximum precision 38, and if value starts with "-0." then max length can be 38 + 3
      // All remaining number types coming from Arrow object have smaller length than Decimal
      int TEXT_LIMIT = 41;
      String value = obj.toString();
      if (!(obj instanceof Number)) {
        value = value.replaceAll("\\W+", "_");
        TEXT_LIMIT = 24;
      }

      if (value.isEmpty()) {
        return EMPTY_VALUE_PARTITION;
      }

      if(value.length() > TEXT_LIMIT){
        return value.substring(0, TEXT_LIMIT);
      } else {
        return value;
      }
    }
  }
}
