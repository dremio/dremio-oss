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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.sort.external.SplayTree.SplayIterator;
import com.google.common.base.Stopwatch;


public abstract class SplaySorterTemplate implements SplaySorterInterface {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplaySorterTemplate.class);

  private FunctionContext context;
  private ExpandableHyperContainer hyperBatch;
  private final SplayTreeImpl tree = new SplayTreeImpl();

  @Override
  public void init(FunctionContext context, ExpandableHyperContainer hyperContainer) throws SchemaChangeException {
    this.context = context;
    this.hyperBatch = hyperContainer;
    doSetup(context, hyperContainer, null);
  }

  private class SplayTreeImpl extends SplayTree {
    @Override
    public int compareValues(int leftVal, int rightVal) {
      return doEval(leftVal, rightVal);
    }
  }

  @Override
  public void add(final SelectionVector2 sv2, final RecordBatchData batch) throws SchemaChangeException {
    final Stopwatch watch = Stopwatch.createStarted();

    final int batchIndex = hyperBatch.size();
    hyperBatch.addBatch(batch.getContainer());
    doSetup(context, hyperBatch, null);
    final int batchCount = hyperBatch.size();
    if(batchCount == 1){
      doSetup(context, hyperBatch, null);
    }

    final int recordCount = batch.getRecordCount();
    final SplayTreeImpl tree = this.tree;
    for (int count = 0; count < recordCount; count++) {
      int index = (batchIndex << 16) | (sv2.getIndex(count) & 65535);
      tree.put(index);
    }
    logger.debug("Took {} us to add {} records", watch.elapsed(TimeUnit.MICROSECONDS), batch.getRecordCount());
  }

  @Override
  public void setDataBuffer(ArrowBuf data){
    tree.setData(data);
  }

  @Override
  public SelectionVector4 getFinalSort(BufferAllocator allocator, int targetBatchSize){
    Stopwatch watch = Stopwatch.createStarted();

    final int totalCount = tree.getTotalCount();
    final SelectionVector4 sortVector = new SelectionVector4(allocator.buffer(totalCount * 4), totalCount, targetBatchSize);
    final SplayIterator iterator = tree.iterator();
    int i = 0;
    while(iterator.hasNext()){
      final int index = iterator.next();
//      logger.debug("Setting {}={}", i, index);
      sortVector.set(i, index);
      i++;
    }
    logger.debug("Took {} us to generate final order", watch.elapsed(TimeUnit.MICROSECONDS));

    // release the memory associated with the tree.
    return sortVector;
  }

  @Override
  public ExpandableHyperContainer getHyperBatch() {
    return hyperBatch;
  }

  @Override
  public void close() throws Exception{
    AutoCloseables.close(hyperBatch);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming,
      @Named("outgoing") VectorAccessible outgoing);

  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);


}
