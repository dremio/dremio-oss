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
package com.dremio.sabot.op.join.merge;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.commons.lang3.tuple.Pair;

/** MarkedAsyncIterator implementation using in-memory memory storage only */
class InMemoryMarkedIterator implements MarkedAsyncIterator {

  // batches stored internally in the iterator
  private final NavigableMap<Integer, VectorContainer> storedBatches;
  private int newBatchIndexCounter = 0; // used to generate unique index for batches

  // batches just submitted by the user that have not been transferred to the iterator
  private VectorAccessible batchNotStored = null;

  // a dummy batch based on first batch seen, with no data
  private final VectorContainer dummyBatch;

  // special value -1 means we use batchNotStored
  private static final int BATCH_NOT_STORED_INDEX = -1;

  // current position of the iterator
  private Integer currentBatchIndex = BATCH_NOT_STORED_INDEX;
  private int currentBatchOffset;

  // marked position of the iterator
  private boolean marked = false;
  private Integer markedBatchIndex = BATCH_NOT_STORED_INDEX;
  private int markedBatchOffset;

  private final BufferAllocator allocator;

  InMemoryMarkedIterator(BufferAllocator allocator, BatchSchema schema) {
    this.allocator = allocator;
    this.storedBatches = new TreeMap<Integer, VectorContainer>();
    dummyBatch = new VectorContainer(allocator);

    dummyBatch.addSchema(schema);
    dummyBatch.buildSchema(SelectionVectorMode.NONE);
    dummyBatch.allocateNew();
    dummyBatch.setAllCount(0);
  }

  @Override
  public Pair<VectorAccessible, Integer> peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)) {
      return Pair.of(batchNotStored, currentBatchOffset);
    } else {
      return Pair.of(storedBatches.get(currentBatchIndex), currentBatchOffset);
    }
  }

  @Override
  public Pair<VectorAccessible, Integer> next() {
    Pair<VectorAccessible, Integer> ret = peek();

    if (currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)) {
      currentBatchOffset++;
    } else {
      if (currentBatchOffset + 1 < storedBatches.get(currentBatchIndex).getRecordCount()) {
        currentBatchOffset++;
      } else {
        tryReleaseBatch(currentBatchIndex);

        currentBatchIndex = storedBatches.higherKey(currentBatchIndex);
        if (currentBatchIndex == null) {
          currentBatchIndex = BATCH_NOT_STORED_INDEX;
        }

        currentBatchOffset = 0;
      }
    }

    return ret;
  }

  @Override
  public boolean hasNext() {
    if (currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)) {
      return batchNotStored != null && currentBatchOffset < batchNotStored.getRecordCount();
    } else {
      if (currentBatchOffset < this.storedBatches.get(currentBatchIndex).getRecordCount()) {
        return true;
      }

      if (batchNotStored != null && batchNotStored.getRecordCount() > 0) {
        return true;
      }

      Integer key = this.storedBatches.higherKey(currentBatchIndex);
      while (key != null) {
        if (this.storedBatches.get(key).getRecordCount() > 0) {
          return true;
        }

        key = this.storedBatches.higherKey(key);
      }
      return false;
    }
  }

  @Override
  public void mark() {
    if (marked) {
      // release mark-related resources
      clearMark();
    }

    Preconditions.checkState(
        hasNext(), "Cannot mark iterator when its current position is illegal");

    marked = true;
    markedBatchIndex = currentBatchIndex;
    markedBatchOffset = currentBatchOffset;
  }

  @Override
  public void resetToMark() {
    Preconditions.checkState(marked, "Iterator is not marked, cannot reset to marked position");
    currentBatchIndex = markedBatchIndex;
    currentBatchOffset = markedBatchOffset;
  }

  @Override
  public Runnable asyncAcceptBatch(VectorAccessible batch) {
    Preconditions.checkState(
        batchNotStored == null, "Iterator has a batch not yet fully processed");

    batchNotStored = batch;
    if (currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)) {
      currentBatchOffset = 0;
    }

    return new Runnable() {

      @Override
      public void run() {
        // save the current not stored batch if necessary
        if (marked
            || (currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)
                && currentBatchOffset < batchNotStored.getRecordCount())) {
          final VectorContainer batchTransfered = transferBatch(batchNotStored);
          storedBatches.put(newBatchIndexCounter, batchTransfered);

          currentBatchIndex =
              currentBatchIndex.equals(BATCH_NOT_STORED_INDEX)
                  ? newBatchIndexCounter
                  : currentBatchIndex;
          markedBatchIndex =
              markedBatchIndex.equals(BATCH_NOT_STORED_INDEX)
                  ? newBatchIndexCounter
                  : markedBatchIndex;

          if (currentBatchIndex.equals(newBatchIndexCounter)
              && currentBatchOffset >= batchTransfered.getRecordCount()) {
            currentBatchIndex = BATCH_NOT_STORED_INDEX;
            currentBatchOffset = 0;
          }

          newBatchIndexCounter++;
        }

        batchNotStored = null;
      }
    };
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("does not support remove");
  }

  @Override
  public void clearMark() {
    marked = false;
    tryReleaseBatch(currentBatchIndex);
  }

  // check if batch prior to given index may be needed by mark, and release it if not
  // needed. not including the given index
  private void tryReleaseBatch(Integer index) {
    ArrayList<Integer> keysToRemove = new ArrayList<Integer>();
    for (Integer key : this.storedBatches.keySet()) {
      if (key.equals(index)) {
        return;
      }

      if (!(marked
          && !markedBatchIndex.equals(BATCH_NOT_STORED_INDEX)
          && markedBatchIndex <= key)) {
        storedBatches.get(key).close();
        keysToRemove.add(key);
      }
    }

    for (Integer key : keysToRemove) {
      storedBatches.remove(key);
    }
  }

  // transfer ownership of a batch
  private VectorContainer transferBatch(VectorAccessible batch) {
    @SuppressWarnings("resource") // TODO better way to write this?
    VectorContainer container = new VectorContainer();

    final List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper<?> v : batch) {
      if (v.isHyper()) {
        throw new UnsupportedOperationException(
            "Record batch data can't be created based on a hyper batch.");
      }
      TransferPair tp = v.getValueVector().getTransferPair(allocator);
      tp.transfer();
      vectors.add(tp.getTo());
    }

    container.addCollection(vectors);
    container.setRecordCount(batch.getRecordCount());
    container.buildSchema(SelectionVectorMode.NONE);

    return container;
  }

  @Override
  public void close() throws Exception {
    for (Integer key : storedBatches.keySet()) {
      storedBatches.get(key).close();
    }

    if (this.dummyBatch != null) {
      this.dummyBatch.close();
    }
  }

  @Override
  public Pair<VectorAccessible, Integer> peekMark() {
    Preconditions.checkState(marked, "Iterator is not marked, cannot reset to marked position");
    if (markedBatchIndex.equals(BATCH_NOT_STORED_INDEX)) {
      return Pair.of(batchNotStored, markedBatchOffset);
    } else {
      return Pair.of(storedBatches.get(markedBatchIndex), markedBatchOffset);
    }
  }

  @Override
  public VectorAccessible getDummyBatch() {
    return dummyBatch;
  }
}
