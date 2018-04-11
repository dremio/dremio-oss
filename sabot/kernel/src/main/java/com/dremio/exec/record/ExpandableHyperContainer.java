/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class ExpandableHyperContainer extends VectorContainer {

  private int size;

  public ExpandableHyperContainer(BufferAllocator allocator, Schema schema) {
    super(allocator);
    for(Field f : schema.getFields()){
      this.addEmptyHyper(f);
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
  }

  public int size(){
    return size;
  }
}
