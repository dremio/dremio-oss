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
package com.dremio.sabot.op.sort.external;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Stopwatch;

public abstract class QuickSorterTemplate implements QuickSorterInterface, IndexedSortable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QuickSorterTemplate.class);

  private FunctionContext context;
  private ExpandableHyperContainer hyperBatch;
  private SimpleIntVector intVector;
  private int totalCount;

  @Override
  public void init(FunctionContext context, ExpandableHyperContainer hyperContainer) throws SchemaChangeException {
    this.context = context;
    this.hyperBatch = hyperContainer;
    doSetup(context, hyperContainer, null);
  }

  public void setDataBuffer(SimpleIntVector intVectorBuffer) {
    intVector = intVectorBuffer;
    totalCount = 0;
  }

  @Override
  public void add(final RecordBatchData batch) throws SchemaChangeException {
    final Stopwatch watch = Stopwatch.createStarted();

    final int batchIndex = hyperBatch.size();
    hyperBatch.addBatch(batch.getContainer());
    doSetup(context, hyperBatch, null);

    final SelectionVector2 incomingSv2 = batch.getSv2();
    final int recordCount = batch.getRecordCount();
    for (int count = 0; count < recordCount; count++) {
      int index = (batchIndex << 16) |
        ((incomingSv2 != null ? incomingSv2.getIndex(count) : count) & 65535);
      intVector.set(totalCount, index);
      totalCount++;
    }
    assert totalCount <= intVector.getValueCapacity();

    logger.debug("Took {} us to add {} records for batch number {}",
      watch.elapsed(TimeUnit.MICROSECONDS), batch.getRecordCount(), batchIndex);
  }

  @Override
  public SelectionVector4 getFinalSort(BufferAllocator allocator, int targetBatchSize){
    Stopwatch watch = Stopwatch.createStarted();

    intVector.setValueCount(totalCount);
    QuickSort qs = new QuickSort();
    if (totalCount > 0) {
      qs.sort(this, 0, totalCount);
    }

    SelectionVector4 finalSortedSV4 = new SelectionVector4(allocator.buffer(totalCount * 4), totalCount, targetBatchSize);
    for (int i = 0; i < totalCount; i++) {
      finalSortedSV4.set(i, intVector.get(i));
    }

    logger.debug("Took {} us to final sort {} records in {} batches",
      watch.elapsed(TimeUnit.MICROSECONDS), totalCount, hyperBatch.size());

    return finalSortedSV4;
  }

  @Override
  public ExpandableHyperContainer getHyperBatch() {
    return hyperBatch;
  }

  @Override
  public void close() throws Exception{
    AutoCloseables.close(hyperBatch);
  }

  @Override
  public void swap(int val1, int val2) {
    final int tmpVal = intVector.get(val1);
    intVector.set(val1, intVector.get(val2));
    intVector.set(val2, tmpVal);
  }

  @Override
  public int compare(int leftIndex, int rightIndex) {
    final int leftVal = intVector.get(leftIndex);
    final int rightVal = intVector.get(rightIndex);
    return doEval(leftVal, rightVal);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming,
      @Named("outgoing") VectorAccessible outgoing);

  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);

}
