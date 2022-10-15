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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;

/**
 * Merger for page batches.
 */
public class PageBatchMerger {
  private final Merger merger;

  public PageBatchMerger(VectorAccessible incoming, BufferAllocator allocator) {
    List<Merger> mergerList = new ArrayList<>();

    int index = 0;
    for (VectorWrapper<?> vectorWrapper : incoming) {
      mergerList.add(Merger.get(vectorWrapper.getValueVector(), index, allocator));
      ++index;
    }
    this.merger = new CombinedMerger(mergerList);
  }

  public VectorContainer merge(List<VectorContainer> srcBatches, Page dstPage) {
    VectorContainerList containerList = new VectorContainerList(srcBatches);
    final List<FieldVector> output = new ArrayList<>();
    merger.merge(containerList, dstPage, output);

    int recordCount = containerList.getRecordCount();
    VectorContainer container = new VectorContainer();
    container.addCollection(new ArrayList<>(output));
    container.setAllCount(recordCount);
    container.buildSchema();

    StreamSupport.stream(container.spliterator(), false)
      .map(v -> ( (FieldVector) v.getValueVector()))
      .filter(v -> (v instanceof BaseVariableWidthVector))
      .forEach(v -> VariableLengthValidator.validateVariable(v, recordCount));
    // Useful for debugging, but has cost.
    // VectorValidator.validate(data.getContainer());
    return container;
  }
}
