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
package com.dremio.sabot.op.copier;

import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

import javax.inject.Named;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.DensityAwareVector;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class CopierTemplate4 implements Copier {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate4.class);

  protected SelectionVector4 sv4;
  protected VectorAccessible outgoing;
  protected int outgoingPosition;
  private long oomAllocation;
  private long oomCopy;
  private double density = 0.01;

  /**
   * Use this flag to control when to set new capacity and density in outgoing vectors. If we are successful in
   * allocating the vectors with given capacity and density, we continue to use the same capacity and density with
   * allocateNew() to allocate memory. If we fail in allocate, we start from initial density and given capacity.
   */
  private boolean lastAllocationSucceeded;



  @Override
  public void setupRemover(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing) throws SchemaChangeException{
    this.outgoing = outgoing;
    this.sv4 = incoming.getSelectionVector4();
    doSetup(context, incoming, outgoing);
  }

  @Override
  public void setAllocationDensity(double density) {
    Preconditions.checkArgument(density > 0);
    this.density = density;
  }

  @Override
  public int copyRecords(int index, int recordCount){
    logger.debug("Copier4: Position to copy from {} records to copy {}", index, recordCount);
    outgoingPosition = 0;
    final double density = this.density;
    boolean memoryAllocated = false;
    try {
      while (!memoryAllocated) {
        try {
          for (VectorWrapper<?> out : outgoing) {
            MajorType type = getMajorTypeForField(out.getField());
            if (!lastAllocationSucceeded) {
              final ValueVector v = out.getValueVector();
              if (v instanceof DensityAwareVector) {
                ((DensityAwareVector) v).setInitialCapacity(recordCount, density);
              } else {
                v.setInitialCapacity(recordCount);
              }
              logger.debug("Copier4: setting initial capacity for {} allocating memory for vector {} MajorType {}", recordCount, v.getClass(), type);
            }
            if (!Types.isFixedWidthType(type)) {
              /* VARCHAR, VARBINARY, UNION */
              out.getValueVector().allocateNew();
            } else {
              /* fixed width, list etc */
              AllocationHelper.allocate(out.getValueVector(), recordCount, 1);
            }
          }
          lastAllocationSucceeded = true;
          memoryAllocated = true;
        }catch (OutOfMemoryException ex) {
          oomAllocation++;
          logger.debug("Copier4: Failed to allocate memory for outgoing batch, retrying with reduced capacity");
          recordCount = recordCount/2;
          if (recordCount < 1) {
            /* DiskRunManager will collect extensive tracing information upon catching OOM */
            logger.debug("Copier4: Unable to allocate memory for even 1 record");
            throw ex;
          }
          clearVectors();
          lastAllocationSucceeded = false;
        }
      }

      logger.debug("Copier4: allocated memory for all vectors in outgoing.");
      outgoingPosition = evalLoop(index, recordCount);
    }catch(OutOfMemoryException ex){
      oomCopy++;
      if(outgoingPosition == 0) {
        /* DiskRunManager will collect extensive tracing information upon catching OOM */
        logger.debug("Copier4: Ran out of space in copy without copying a single record");
        throw ex;
      }
      /* DiskRunManager will spill whatever was copied and no need to throw back the exception */
      logger.debug("Copier4: Ran out of space in copy, returning early");
    }

    return outgoingPosition;
  }

  protected int evalLoop(int index, int recordCount) {
    for(int svIndex = index; svIndex < index + recordCount; svIndex++, outgoingPosition++){
      int deRefIndex = sv4.get(svIndex);
      doEval(deRefIndex, outgoingPosition);
    }
    return outgoingPosition;
  }

  private void clearVectors() {
    for (VectorWrapper<?> vw : outgoing) {
      final ValueVector v = vw.getValueVector();
      v.clear();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(sv4);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

  @Override
  public long getOOMCountDuringAllocation() {
    return oomAllocation;
  }

  @Override
  public long getOOMCountDuringCopy() {
    return oomCopy;
  }


}
