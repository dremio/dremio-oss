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

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector4;
import com.google.common.collect.Lists;

/**
 * Insert each batch into a QuickSorter as it arrives, they will be totally sorted only at the end.
 */
public class QuickSorter implements Sorter {
  private final ExternalSort sortConfig;
  private final ClassProducer classProducer;
  private final Schema schema;
  private final BufferAllocator allocator;

  private QuickSorterInterface quickSorter;
  private SimpleIntVector quickSorterBuffer;

  public QuickSorter(ExternalSort sortConfig, ClassProducer classProducer, Schema schema, BufferAllocator allocator) {
    this.sortConfig = sortConfig;
    this.classProducer = classProducer;
    this.schema = schema;
    this.allocator = allocator;
    quickSorterBuffer = new SimpleIntVector("QuickSorterSimpleIntVector", allocator);
  }

  @Override
  public boolean expandMemoryIfNecessary(int newRequiredSize) {
    try {
      // Realloc QuickSorter SimpleIntVector, doubles size each time.
      while (quickSorterBuffer.getValueCapacity() < newRequiredSize) {
        quickSorterBuffer.reAlloc();
      }
    } catch (OutOfMemoryException ex) {
      return false;
    }

    return true;
  }

  @Override
  public void setup(VectorAccessible batch) throws ClassTransformationException, SchemaChangeException, IOException {
    // Compile sorting classes.
    CodeGenerator<QuickSorterInterface> cg = classProducer.createGenerator(QuickSorterInterface.TEMPLATE_DEFINITION);
    ClassGenerator<QuickSorterInterface> g = cg.getRoot();
    final Sv4HyperContainer container = new Sv4HyperContainer(allocator, schema);
    ExternalSortOperator.generateComparisons(g, container, sortConfig.getOrderings(), classProducer);
    this.quickSorter = cg.getImplementationClass();
    quickSorter.init(classProducer.getFunctionContext(), container);
    quickSorter.setDataBuffer(quickSorterBuffer);
  }

  @Override
  public void addBatch(RecordBatchData data, BufferAllocator copyTargetAllocator) throws SchemaChangeException {
    // No need to sort individual batches here, will sort all at end, just insert the values into the
    // quick sorter implementation here.
    quickSorter.add(data);
  }

  @Override
  public ExpandableHyperContainer getHyperBatch() {
    if (quickSorter != null) {
      return quickSorter.getHyperBatch();
    } else {
      return null;
    }
  }

  @Override
  public int getHyperBatchSize() {
    if (quickSorter != null) {
      return quickSorter.getHyperBatch().size();
    } else {
      return 0;
    }
  }

  @Override
  public SelectionVector4 getFinalSort(BufferAllocator copyTargetAllocator, int targetBatchSize) {
    return quickSorter.getFinalSort(copyTargetAllocator, targetBatchSize);
  }

  @Override
  public void close() throws Exception {
    final List<AutoCloseable> closeables = Lists.newArrayList();

    closeables.add(quickSorterBuffer);
    AutoCloseables.close(closeables);

    quickSorterBuffer = null;
  }
}
