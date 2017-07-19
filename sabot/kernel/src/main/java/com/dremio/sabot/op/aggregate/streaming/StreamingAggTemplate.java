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
package com.dremio.sabot.op.aggregate.streaming;

import javax.inject.Named;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.aggregate.streaming.StreamingAggOperator.TransferOnDeckAtBat;

public abstract class StreamingAggTemplate implements StreamingAggregator {

  private VectorAccessible atBat;
  private int targetBatchSize;
  private TransferOnDeckAtBat transfer;
  private int atBatIndex = 0;
  private int outputIndex = 0;

  @Override
  public void setup(
      FunctionContext context,
      VectorAccessible onDeck,
      VectorAccessible atBat,
      VectorAccessible output,
      int targetBatchSize,
      TransferOnDeckAtBat transfer) {
    this.atBat = atBat;
    this.transfer = transfer;
    this.targetBatchSize = targetBatchSize;
    setupAtBat(context, atBat, output);
    setupOnDeck(context, onDeck, atBat, output);
  }


  @Override
  public void consumeFirstRecord() {
    addAtBatRecord(atBatIndex++);
  }

  @Override
  public int consumeBoundaryRecordAndSwap() {
    if (compareOnDeckAndAtBat(0, atBatIndex)) {
      // boundary record was the same
      transfer.transfer();
      atBatIndex = 0;
      addAtBatRecord(atBatIndex++);
    } else {
      // boundary record was different.
      outputAggregation(outputIndex);
      outputAtBatKeys(atBatIndex, outputIndex);
      outputIndex++;
      resetValues();
      transfer.transfer();
      atBatIndex = 0;
      addAtBatRecord(atBatIndex++);

      // check if we should return the output batch.
      if(outputIndex == targetBatchSize) {
        int localOutputIndex = outputIndex;
        outputIndex = 0;
        return localOutputIndex;
      }
    }

    return 0;
  }

  @Override
  public int consumeRecords() {
    final int recordCount = atBat.getRecordCount();
    final int targetBatchSize = this.targetBatchSize;

    int outputIndex = this.outputIndex;
    int cur = atBatIndex;

    for(; cur < recordCount; cur++){
      final int prev = cur - 1;
      if(compareAtBat(prev, cur)) { // keys matched.
        addAtBatRecord(cur);
      } else { // keys didn't match, output a record.
        outputAggregation(outputIndex);
        outputAtBatKeys(prev, outputIndex);
        outputIndex++;
        resetValues();
        addAtBatRecord(cur);

        if(outputIndex == targetBatchSize) {
          // reset class variable
          this.outputIndex = 0;
          this.atBatIndex = cur + 1;
          // return local variable.
          return outputIndex;
        }
      }
    }

    // write local variables back to class.
    this.atBatIndex = recordCount - 1;
    this.outputIndex = outputIndex;

    // we didn't hit the output max so we're not going to 'return' any records.
    return 0;
  }

  @Override
  public int closeLastAggregation() {
    outputAggregation(outputIndex);
    outputAtBatKeys(atBatIndex, outputIndex);
    resetValues();
    outputIndex++;
    return outputIndex;
  }

  // Methods associated only with atBat data.
  public abstract void setupAtBat(@Named("context") FunctionContext context, @Named("atBat") VectorAccessible atBat, @Named("output") VectorAccessible output);
  public abstract boolean compareAtBat(@Named("index1") int index1, @Named("index2") int index2);
  public abstract void addAtBatRecord(@Named("atBatIndex") int index);
  public abstract void outputAtBatKeys(@Named("atBatIndex") int inIndex, @Named("outputIndex") int outputIndex);

  // Methods associated with onDeck data.
  public abstract void setupOnDeck(
      @Named("context") FunctionContext context,
      @Named("onDeck") VectorAccessible onDeck,
      @Named("atBat") VectorAccessible atBat,
      @Named("output") VectorAccessible output);
  public abstract boolean compareOnDeckAndAtBat(@Named("onDeckIndex") int onDeckIndex, @Named("atBatIndex") int atBatIndex);

  public abstract void outputAggregation(@Named("outputIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();



}
