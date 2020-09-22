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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.copier.CopierTemplate4;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.copier.FieldBufferCopier4;

/**
 * Replacement class for vector copying that behaves the same as a compiled CoperTemplate4.
 */
class VectorCopier4 extends CopierTemplate4 {

  private final List<FieldBufferCopier> copiers;

  public VectorCopier4(VectorAccessible incoming, VectorAccessible outgoing) {
    this.sv4 = incoming.getSelectionVector4();
    this.outgoing = outgoing;

    @SuppressWarnings("unchecked")
    List<FieldVector[]> inputVectors = (List<FieldVector[]>) (Object) StreamSupport.stream(incoming.spliterator(), false).map(w -> w.getValueVectors()).collect(Collectors.toList());

    @SuppressWarnings("unchecked")
    List<FieldVector> outputVectors = (List<FieldVector>) StreamSupport.stream(outgoing.spliterator(), false).map(w -> w.getValueVector()).collect(Collectors.toList());

    copiers = FieldBufferCopier4.getFourByteCopiers(inputVectors, outputVectors);
  }

  @Override
  protected int evalLoop(int index, int recordCount) {
    final long sv4Addr = sv4.getMemoryAddress() + index * 4;
    for(FieldBufferCopier fbc : copiers) {
      fbc.copy(sv4Addr, recordCount);
    }
    return outgoingPosition + recordCount;
  }

  @Override
  public void doSetup(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doEval(int inIndex, int outIndex) {
    throw new UnsupportedOperationException();
  }

}
