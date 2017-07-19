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
package com.dremio.exec.physical.impl.join;

import java.util.LinkedList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.types.Types;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.op.sort.external.RecordBatchData;

/**
 * Marks each record that starts a new cluster.
 */
public abstract class SequenceFinder {

//  private BufferAllocator allocator;
//  private final LinkedList<ClusteredBatch> data = new LinkedList<>();
//  private int batchesAdded;
//
//  private VectorAccessible incoming;
//  private VectorContainer active;
//
//  public void setup(VectorAccessible incoming){
//
//  }
//
//
//  private void consume(){
//    data.add(new ClusteredBatch(incoming));
//    batchesAdded++;
//  }
//
//  private class ClusteredBatch implements AutoCloseable {
//    private final RecordBatchData data;
//    private final BitVector newIndicator;
//
//    public ClusteredBatch(VectorAccessible accessible) {
//      this.data = new RecordBatchData(accessible, allocator);
//      this.newIndicator = new BitVector(MajorTypeHelper.getFieldForNameAndMajorType("newIndicator", Types.required(MinorType.BIT)), allocator);
//
//      final BitVector.Mutator mutator = newIndicator.getMutator();
//
//      boolean firstRecordIsNew = batchesAdded == 0 || isBoundaryEqual();
//      mutator.set(0, firstRecordIsNew ? 1 : 0);
//
//      final int count = data.getRecordCount();
//      for(int i = 1; i < count; i++){
//        mutator.set(0, firstRecordIsNew ? 1 : 0);
//      }
//    }
//
//    @Override
//    public void close() throws Exception {
//      AutoCloseables.close(data, newIndicator);
//    }
//
//
//  }
//
//
//  public abstract boolean isBoundaryEqual(){
//
//  }
//  public abstract boolean checkBoundaryEqual();
//  public abstract boolean isEqual(int index1, int index2);
//

}
