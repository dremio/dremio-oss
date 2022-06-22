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
package com.dremio.sabot.op.join.vhash.spill.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.VectorContainer;

/**
 * A chunk (set of records) from spill : either build-side or probe-side. Each chunk contains :
 * - pivoted data : fixed (mandatory) and variable (optional)
 * - container for unpivoted data i.e carry-along columns (optional)
 */
public class SpillChunk implements AutoCloseable {
  // number of records in the chunk
  private final int numRecords;

  // pivoted fixed block
  private final ArrowBuf fixed;

  // pivoted variable block
  private final ArrowBuf variable;

  // Un-pivoted record batch
  private final VectorContainer container;

  // release these when closing the chunk.
  // Typically, contain pages that the fixed/variable/container refer to.
  private final List<AutoCloseable> toRelease;

  private boolean closed;

  public SpillChunk(int numRecords, ArrowBuf fixed, ArrowBuf variable, VectorContainer container, List<AutoCloseable> toRelease) {
    this.numRecords = numRecords;
    this.fixed = fixed;
    this.variable = variable;
    this.container = container;
    this.toRelease = new ArrayList<>(toRelease);
  }

  public int getNumRecords() {
    return numRecords;
  }

  public ArrowBuf getFixed() {
    return fixed;
  }

  public ArrowBuf getVariable() {
    return variable;
  }

  public VectorContainer getContainer() {
    return container;
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      List<AutoCloseable> autoCloseables = new ArrayList<>();
      autoCloseables.add(fixed);
      autoCloseables.add(variable);
      autoCloseables.addAll(toRelease);
      AutoCloseables.close(autoCloseables);
      closed = true;
    }
  }
}
