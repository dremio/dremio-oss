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
package com.dremio.sabot;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationReservation;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.TestTools;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.dremio.sabot.op.sort.external.Sv4HyperContainer;

public class TestCopiers extends BaseTestOperator {
  static final int targetBatchSize = 1023;

  private BufferAllocator inputAlloc;
  private BufferManager bufferManager;
  private ClassProducer producer;
  ComplexDataGenerator dataGenerator;
  Sv4HyperContainer inputHyper;
  SelectionVector4 sv4;
  private final int recordCount = 100_000;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(400, TimeUnit.SECONDS);

  @Before
  public void prepare() {
    inputAlloc = getTestAllocator().newChildAllocator("input", 0, Integer.MAX_VALUE);
    bufferManager = new BufferManagerImpl(allocator);
    producer = testContext.newClassProducer(bufferManager);
    dataGenerator = new ComplexDataGenerator(recordCount, inputAlloc);

    inputHyper = new Sv4HyperContainer(inputAlloc, dataGenerator.getSchema());
    sv4 = new SelectionVector4(inputAlloc.buffer(recordCount*4), recordCount, targetBatchSize);

    int record = 0;
    for(int i = 0; i < 10; i++) {
      int numRecords = dataGenerator.next(targetBatchSize);
      inputHyper.addBatch(dataGenerator.getOutput());
      for(int j = 0; j < numRecords; j++) {
        sv4.set(record++, i, j);
      }
    }
    inputHyper.setSelectionVector4(sv4);
  }

  @Test
  public void test1Mb() throws Exception {
    testHelper(1_000_000, targetBatchSize);
  }

  @Test
  public void test500K() throws Exception {
    testHelper(500_000, targetBatchSize);
  }

  @Test
  public void test100K() throws Exception {
    testHelper(100_000, targetBatchSize);
  }

  @Test(expected = OutOfMemoryException.class)
  public void test1K() throws Exception {
    testHelper(1_000, targetBatchSize);
  }

  private void testHelper(int allocatorSize, int targetBatchSize) throws Exception {
    try (
        AllocatorWrapper outputAlloc = new AllocatorWrapper(getTestAllocator().newChildAllocator("output", 0, allocatorSize));
        final VectorContainer outgoing = VectorContainer.create(outputAlloc, dataGenerator.getSchema());
        Copier copier = CopierOperator.getGenerated4Copier(producer, inputHyper, outgoing);
    ) {
      int toCopy = targetBatchSize;
      while (toCopy > 0) {
        int copied = copier.copyRecords(0, toCopy);
        toCopy -= copied;
        int totalAllocs = outputAlloc.getNumAllocs();
      }
    }
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(sv4, inputHyper, dataGenerator, bufferManager, inputAlloc);
  }

  private class AllocatorWrapper implements BufferAllocator {
    private final BufferAllocator actual;

    // stats
    int numAllocs;

    AllocatorWrapper(BufferAllocator actual) {
      this.actual = actual;
    }

    int getNumAllocs() {
      return numAllocs;
    }

    void resetNumAllocs() {
      numAllocs = 0;
    }

    @Override
    public ArrowBuf buffer(long i) {
      numAllocs++;
      return actual.buffer(i);
    }

    @Override
    public ArrowBuf buffer(long i, BufferManager bufferManager) {
      numAllocs++;
      return actual.buffer(i, bufferManager);
    }

    @Override
    public BufferAllocator newChildAllocator(String s, long l, long l1) {
      return actual.newChildAllocator(s, l, l1);
    }

    @Override
    public BufferAllocator newChildAllocator(String s, AllocationListener listener, long l, long l1) {
      return actual.newChildAllocator(s, listener, l, l1);
    }

    @Override
    public void close() {
      actual.close();
    }

    @Override
    public long getAllocatedMemory() {
      return actual.getAllocatedMemory();
    }

    @Override
    public long getLimit() {
      return actual.getLimit();
    }

    @Override
    public long getInitReservation() {
      return actual.getInitReservation();
    }

    @Override
    public void setLimit(long l) {
      actual.setLimit(l);
    }

    @Override
    public long getPeakMemoryAllocation() {
      return actual.getPeakMemoryAllocation();
    }

    @Override
    public long getHeadroom() {
      return actual.getHeadroom();
    }

    @Override
    public BufferAllocator getParentAllocator() {
      return actual.getParentAllocator();
    }

    @Override
    public Collection<BufferAllocator> getChildAllocators() {
      return actual.getChildAllocators();
    }

    @Override
    public AllocationReservation newReservation() {
      return actual.newReservation();
    }

    @Override
    public ArrowBuf getEmpty() {
      return actual.getEmpty();
    }

    @Override
    public String getName() {
      return actual.getName();
    }

    @Override
    public boolean isOverLimit() {
      return actual.isOverLimit();
    }

    @Override
    public String toVerboseString() {
      return actual.toVerboseString();
    }

    @Override
    public void assertOpen() { actual.assertOpen(); }

    @Override
    public AllocationListener getListener() {
      return actual.getListener();
    }

    @Override
    public void releaseBytes(long size) {
      actual.releaseBytes(size);
    }

    @Override
    public boolean forceAllocate(long size) {
      return actual.forceAllocate(size);
    }

    @Override
    public BufferAllocator getRoot() {
      return actual.getRoot();
    }
  }
}
