/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.copier;

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import javax.inject.Named;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;

import com.dremio.common.AutoCloseables;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;

public abstract class CopierTemplate4 implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate4.class);

  private SelectionVector4 sv4;
  private VectorAccessible outgoing;

  @Override
  public void setupRemover(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) throws SchemaChangeException{
    this.outgoing = outgoing;
    this.sv4 = incoming.getSelectionVector4();
    doSetup(context, incoming, outgoing);
  }

  @Override
  public int copyRecords(int index, int recordCount){
    int outgoingPosition = 0;
    try{

      for(VectorWrapper<?> out : outgoing){
        MajorType type = getMajorTypeForField(out.getField());
        if (!Types.isFixedWidthType(type) || Types.isRepeated(type)) {
          out.getValueVector().allocateNew();
        } else {
          AllocationHelper.allocate(out.getValueVector(), recordCount, 1);
        }
      }

      for(int svIndex = index; svIndex < index + recordCount; svIndex++, outgoingPosition++){
        int deRefIndex = sv4.get(svIndex);
        doEval(deRefIndex, outgoingPosition);
      }
    }catch(OutOfMemoryException ex){
      if(outgoingPosition == 0) {
        throw ex;
      }
      logger.debug("Ran out of space in copy, returning early.");
    }
    return outgoingPosition;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(sv4);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);



}
