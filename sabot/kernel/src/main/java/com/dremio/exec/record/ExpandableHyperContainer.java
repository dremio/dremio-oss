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
package com.dremio.exec.record;

import java.util.ArrayList;
import java.util.BitSet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class ExpandableHyperContainer extends VectorContainer {

  private int size;

  /* Used to indicate which fields are key, those key fields will not be added to the hyper container for VECTORIZED_GENERIC mode.
   * The corresponding field is key if the bit is set
   */
  private BitSet isKeyBits;

  public ExpandableHyperContainer(BufferAllocator allocator, Schema schema) {
    super(allocator);
    // Add all key fields for VECTORIZED_BIGINT mode
    this.isKeyBits = null;
    int i=0;
    for(Field f : schema.getFields()){
      this.addEmptyHyper(f);
    }
    this.buildSchema(SelectionVectorMode.FOUR_BYTE);
  }

  public ExpandableHyperContainer(BufferAllocator allocator, Schema schema, BitSet isKeyBits) {
    super(allocator);
    this.isKeyBits = isKeyBits;
    int i=0;
    for(Field f : schema.getFields()){
      /* If the bit is not set, the corresponding field will be added to hyper container,
       * otherwise the field will be ignored.
       */
      if (!this.isKeyBits.get(i)) {
        this.addEmptyHyper(f);
      }
      i ++;
    }
    this.buildSchema(SelectionVectorMode.FOUR_BYTE);
  }

  public void noReleaseClear(){
    wrappers.clear();
  }

  @Override
  public void close() {
    size = 0;
    super.close();
  }

  public void addBatch(VectorAccessible batch) {
    if (isKeyBits == null) {
      // Add all field vectors.
      if (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.FOUR_BYTE) {
        int i = 0;
        for (VectorWrapper<?> w : batch) {
          HyperVectorWrapper<?> hyperVectorWrapper = (HyperVectorWrapper<?>) wrappers.get(i++);
          hyperVectorWrapper.addVectors(w.getValueVectors());
        }
      } else {
        int i = 0;
        for (VectorWrapper<?> w : batch) {
          HyperVectorWrapper<?> hyperVectorWrapper = (HyperVectorWrapper<?>) wrappers.get(i++);
          hyperVectorWrapper.addVector(w.getValueVector());
        }
      }
      size++;
    } else {
      /* Only add the field vector whose corresponding bit is not set,
       * otherwise the field vector will not be added to hyper container and it will be closed to release memory.
       */
      if (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.FOUR_BYTE) {
        int i = 0;
        int j = 0;
        for (VectorWrapper<?> w : batch) {
          if (!isKeyBits.get(j)) {
            HyperVectorWrapper<?> hyperVectorWrapper = (HyperVectorWrapper<?>) wrappers.get(i++);
            hyperVectorWrapper.addVectors(w.getValueVectors());
          } else {
            w.close();
          }
          j++;
        }
      } else {
        int i = 0;
        int j = 0;
        for (VectorWrapper<?> w : batch) {
          if (!isKeyBits.get(j)) {
            HyperVectorWrapper<?> hyperVectorWrapper = (HyperVectorWrapper<?>) wrappers.get(i++);
            hyperVectorWrapper.addVector(w.getValueVector());
          } else {
            w.close();
          }
          j++;
        }
      }
      size++;
    }
  }

  /**
   * Break down the hyper container into individual containers (one for each batch).
   * This is a destructive operation : the hyper container is cleared.
   *
   * @return list of component containers.
   */
  public ArrayList<VectorContainer> convertToComponentContainers() {
    ArrayList<VectorContainer> containerList = new ArrayList<>();
    if (wrappers.size() > 0) {
      int numBatches = wrappers.get(0).getValueVectors().length;
      for (int batchId = 0; batchId < numBatches; ++batchId) {
        VectorContainer vectorContainer = new VectorContainer();
        for (VectorWrapper<?> wrapper : wrappers) {
          ValueVector vector = wrapper.getValueVectors()[batchId];
          vectorContainer.add(vector);
        }
        vectorContainer.buildSchema();
        containerList.add(vectorContainer);
      }
      wrappers.clear();
    } else {
      // empty hyper-container. Create empty batches.
      for (int i = 0; i < size; ++i) {
        VectorContainer empty = VectorContainer.create(getAllocator(), getSchema());
        containerList.add(empty);
      }
    }
    return containerList;
  }

  public int size(){
    return size;
  }
}
