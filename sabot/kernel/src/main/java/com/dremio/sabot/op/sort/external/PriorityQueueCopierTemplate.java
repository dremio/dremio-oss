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
package com.dremio.sabot.op.sort.external;

import io.netty.buffer.ArrowBuf;

import java.io.IOException;

import javax.inject.Named;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.sort.external.DiskRunManager.DiskRunIterator;
import com.google.common.collect.Iterables;

import org.apache.arrow.vector.DensityAwareVector;
import org.apache.arrow.vector.ValueVector;

public abstract class PriorityQueueCopierTemplate implements PriorityQueueCopier {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PriorityQueueCopierTemplate.class);

  private SelectionVector4 vector4;
  private DiskRunIterator[] iterators;
  private Sv4HyperContainer incoming;
  private VectorContainer outgoing;
  private int size;
  private int queueSize = 0;

  /**
   * Last density parameter used to successfully allocate memory for outgoing vectors. We keep track of this parameter
   * to use it across copy calls.
   */
  private double lastSuccessfulDensity = 1.0d;

  @Override
  public void setup(
      FunctionContext context,
      BufferAllocator allocator,
      DiskRunIterator[] iterators,
      VectorAccessible incoming,
      VectorContainer outgoing) throws SchemaChangeException, IOException {
    this.incoming = new Sv4HyperContainer(allocator, incoming.getSchema());
    this.size = iterators.length;
    final ArrowBuf arrowBuf = allocator.buffer(4 * size);
    this.vector4 = new SelectionVector4(arrowBuf, size, Character.MAX_VALUE);
    this.iterators = iterators;
    this.outgoing = outgoing;

    doSetup(context, incoming, outgoing);

    queueSize = 0;
    for (int i = 0; i < size; i++) {
      vector4.set(i, i, iterators[i].getNextId());
      siftUp();
      queueSize++;
    }
  }

  @Override
  public int copy(int targetRecordCount) {
    allocateVectors(targetRecordCount);
    int outgoingIndex = 0;
    try{
      for (; outgoingIndex < targetRecordCount; outgoingIndex++) {

        if (queueSize == 0) {
          return 0;
        }

        final int compoundIndex = vector4.get(0);
        final int batch = compoundIndex >>> 16;
        assert batch < iterators.length : String.format("batch: %d batchGroups: %d", batch, iterators.length);
        doCopy(compoundIndex, outgoingIndex);

        int nextIndex = iterators[batch].getNextId();
        if (nextIndex < 0) {
          vector4.set(0, vector4.get(--queueSize));
        } else {
          vector4.set(0, batch, nextIndex);
        }
        if (queueSize == 0) {
          setValueCount(++outgoingIndex);
          return outgoingIndex;
        }
        siftDown();
      }
      setValueCount(targetRecordCount);
      return targetRecordCount;

    }catch(IOException ex) {
      throw UserException
        .dataReadError(ex)
        .message("Failure while reading sort spilling files.")
        .build(logger);
    }catch(OutOfMemoryException ex) {
      if(outgoingIndex > 0){
        // TODO we may not be able to recover from an OOM
        // DiskRunManager.getNextId() may throw an OOM in the middle of VectorAccessibleSerializable.readFromStream()
        // trying to load that batch again will throw an exception in protobuf
        return outgoingIndex;
      }else{
        throw ex;
      }
    }
  }

  private void setValueCount(int count) {
    for (VectorWrapper<?> w: outgoing) {
      w.getValueVector().setValueCount(count);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(
      Iterables.concat(
          AutoCloseables.iter(vector4),
          AutoCloseables.iter(outgoing),
          incoming,
          AutoCloseables.iter(iterators)
      )
    );
  }

  private void siftUp() {
    int p = queueSize;
    while (p > 0) {
      if (compare(p, (p - 1) / 2) < 0) {
        swap(p, (p - 1) / 2);
        p = (p - 1) / 2;
      } else {
        break;
      }
    }
  }

  private void allocateVectors(int targetRecordCount) {
    boolean memoryAllocated = false;
    double density = lastSuccessfulDensity;
    while (!memoryAllocated) {
      try {
        for (VectorWrapper<?> w : outgoing) {
          final ValueVector v = w.getValueVector();
          if (v instanceof DensityAwareVector) {
            ((DensityAwareVector) v).setInitialCapacity(targetRecordCount, density);
          } else {
            v.setInitialCapacity(targetRecordCount);
          }
          v.allocateNew();
        }
        memoryAllocated = true;
        lastSuccessfulDensity = density;
      } catch (OutOfMemoryException ex) {
        // halve the density and try again
        density = density / 2;
        if (density < 0.01) {
          logger.debug("PriorityQueueCopierTemplate ran out of memory to allocate outgoing batch. " +
              "Records: {}, density: {}", targetRecordCount, density);
          throw ex;
        }
        // try allocating again with lower density
        logger.debug("PriorityQueueCopierTemplate: Ran out of memory. Retrying allocation with lower density.");
      }
    }
  }

  private void siftDown() {
    int p = 0;
    int next;
    while (p * 2 + 1 < queueSize) { // While the current node has at least one child
      if (p * 2 + 2 >= queueSize) { // if current node has only one child, then we only look at it
        next = p * 2 + 1;
      } else {
        if (compare(p * 2 + 1, p * 2 + 2) <= 0) {//if current node has two children, we must first determine which one has higher priority
          next = p * 2 + 1;
        } else {
          next = p * 2 + 2;
        }
      }
      if (compare(p, next) > 0) { // compare current node to highest priority child and swap if necessary
        swap(p, next);
        p = next;
      } else {
        break;
      }
    }
  }

  public void swap(int sv0, int sv1) {
    int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }

  public int compare(int leftIndex, int rightIndex) {
    int sv1 = vector4.get(leftIndex);
    int sv2 = vector4.get(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);
  public abstract void doCopy(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
