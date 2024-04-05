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
package com.dremio.exec.record;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.selection.SelectionVector2;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

/**
 * Holds the data for a particular record batch for later manipulation.
 *
 * <p>This represents a read-only interface to a record batch
 */
public class RecordBatchData implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchData.class);

  private SelectionVector2 sv2;
  private VectorContainer container = new VectorContainer();

  protected RecordBatchData(List<? extends ValueVector> vectors, int recordCount) {
    this.container.addCollection(new ArrayList<>(vectors));
    this.container.setAllCount(recordCount);
    this.container.buildSchema();
  }

  public RecordBatchData(VectorAccessible batch, BufferAllocator allocator) {
    if (batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE) {
      this.sv2 = batch.getSelectionVector2().clone();
    } else {
      this.sv2 = null;
    }

    for (VectorWrapper<?> v : batch) {
      if (v.isHyper()) {
        throw new UnsupportedOperationException(
            "Record batch data can't be created based on a hyper batch.");
      }
    }

    container.transferFrom(batch, allocator);
  }

  public int getRecordCount() {
    return container.getRecordCount();
  }

  public List<ValueVector> getVectors() {
    List<ValueVector> vectors = Lists.newArrayList();
    for (VectorWrapper w : container) {
      vectors.add(w.getValueVector());
    }
    return vectors;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }

  public VectorAccessible getVectorAccessible() {
    return container;
  }

  @Deprecated
  public void clear() {
    close();
  }

  @Override
  public void close() {
    if (sv2 != null) {
      sv2.clear();
    }
    container.clear();
  }

  public BatchSchema getSchema() {
    return this.container.getSchema();
  }
}
