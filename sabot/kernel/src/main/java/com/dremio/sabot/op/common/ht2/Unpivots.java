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
package com.dremio.sabot.op.common.ht2;

import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;

import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldMode;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import io.netty.util.internal.PlatformDependent;

public class Unpivots {

  private Unpivots(){}

  private static void unpivotBits1(long srcFixedAddr, int blockWidth, final long target,
      final int bitByteOffset, final int bitOffset, final int start, final int count, final int seekInOutput){

    final long startAddr = srcFixedAddr + (start * blockWidth) + bitByteOffset;
    long maxAddr = startAddr + (count * blockWidth);
    int targetIndex = seekInOutput;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetIndex++){
      final int byteValue = PlatformDependent.getInt(srcAddr);
      int bitVal = ((byteValue >>> bitOffset) & 1) << (targetIndex & 31);
      final long addr = target + ((targetIndex >>> 5) * 4);
      PlatformDependent.putInt(addr, PlatformDependent.getInt(addr) | bitVal);
    }
  }

  private static void unpivotBytes4(final long srcFixedAddr, final int blockWidth,
      final long target, final int byteOffset, final int start, final int count, final int seekInOutput) {
    final long startAddr = srcFixedAddr + (start * blockWidth);
    long maxAddr = startAddr + (count * blockWidth);
    long targetAddr = target + seekInOutput * 4;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetAddr+=4){
      final int value = PlatformDependent.getInt(srcAddr + byteOffset);
      PlatformDependent.putInt(targetAddr, value);
    }
  }

  private static void unpivotBytes8(final long srcFixedAddr, final int blockWidth,
      final long target, final int byteOffset, final int start, final int count, final int seekInOutput) {
    final long startAddr = srcFixedAddr + (start * blockWidth);
    long maxAddr = startAddr + (count * blockWidth);
    long targetAddr = target + seekInOutput * 8;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetAddr+=8){
      final long value = PlatformDependent.getLong(srcAddr + byteOffset);
      PlatformDependent.putLong(targetAddr, value);
    }
  }

  private static void unpivotBytes16(final long srcFixedAddr, final int blockWidth,
      final long target, final int byteOffset, final int start, final int count, final int seekInOutput) {
    final long startAddr = srcFixedAddr + (start * blockWidth);
    long maxAddr = startAddr + (count * blockWidth);
    long targetAddr = target + seekInOutput * 16;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetAddr+=16){
      PlatformDependent.putLong(targetAddr, PlatformDependent.getLong(srcAddr + byteOffset));
      PlatformDependent.putLong(targetAddr + 8, PlatformDependent.getLong(srcAddr + 8 + byteOffset));
    }
  }

  private static void unpivotVariable(final long srcFixedAddr, final long srcVarAddr, final int blockWidth,
                                      final FieldVector[] targets, final int start, final int count, final int seekInOutput) {
    final int dataWidth = blockWidth - LBlockHashTable.VAR_OFFSET_SIZE;
    final long startVarOffset = srcFixedAddr + (blockWidth * start) + dataWidth;
    final long maxAddr = startVarOffset + (count * blockWidth);
    final long srcVarAddrBase = srcVarAddr;
    final int fieldCount = targets.length;

    final long[] targetAddrs = new long[fieldCount];
    final long[] offsetAddrs = new long[fieldCount];
    final long[] maxTargetAddrs = new long[fieldCount];
    final Reallocator[] reallocs = new Reallocator[fieldCount];

    for(int i = 0; i < fieldCount; i++){
      FieldVector vect = targets[i];

      long startOffsetInTarget = 0;
      offsetAddrs[i] = vect.getOffsetBufferAddress();
      if (seekInOutput > 0) {
        offsetAddrs[i] += (seekInOutput * BaseVariableWidthVector.OFFSET_WIDTH);
        startOffsetInTarget = PlatformDependent.getInt(offsetAddrs[i]);
      }
      Reallocator realloc = Reallocators.getReallocator(vect);
      reallocs[i] = realloc;
      targetAddrs[i] = realloc.addr() + startOffsetInTarget;
      maxTargetAddrs[i] = realloc.max();
    }

    // loop per record.
    for(long varOffsetAddr = startVarOffset; varOffsetAddr < maxAddr; varOffsetAddr += blockWidth){
      int varOffset = PlatformDependent.getInt(varOffsetAddr);
      long varPos = srcVarAddrBase + varOffset + LBlockHashTable.VAR_LENGTH_SIZE; // skip the complete varlength since not needed for unpivoting (this shifts the offsets outside the loop).;

      // loop per field
      for(int i = 0; i < fieldCount; i++){
        int len = PlatformDependent.getInt(varPos);
        varPos+=4;

        long target = targetAddrs[i];
        // resize as necessary.
        if(maxTargetAddrs[i] < target + len){
          Reallocator realloc = reallocs[i];
          final int shift = (int) (target - realloc.addr());
          target = realloc.ensure(shift + len) + shift;
          targetAddrs[i] = target;
          maxTargetAddrs[i] = realloc.max();
        }

        // copy variable data.
        long offsetAddr = offsetAddrs[i];
        int startIdx = PlatformDependent.getInt(offsetAddr);
        offsetAddr +=4;
        PlatformDependent.putInt(offsetAddr, startIdx + len);
        PlatformDependent.copyMemory(varPos, target, len);
        offsetAddrs[i] = offsetAddr;
        targetAddrs[i] += len;
        varPos += len;
      }
    }

    for(Reallocator r : reallocs){
      r.setCount(seekInOutput + count);
    }
  }

  public static void unpivotBatches(PivotDef pivot, final FixedBlockVector[] fixedVectors,
                                    final VariableBlockVector[] variableVectors, int[] recordsInBatches) {
    Preconditions.checkArgument(fixedVectors.length == recordsInBatches.length);
    Preconditions.checkArgument(variableVectors.length == recordsInBatches.length);
    int totalRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      totalRecords += recordsInBatches[i];
    }
    for (FieldVector v : pivot.getOutputVectors()) {
      AllocationHelper.allocate(v, totalRecords, 15);
    }
    totalRecords = 0;
    for (int i = 0; i < recordsInBatches.length; i++) {
      unpivotToAllocedOutput(pivot, fixedVectors[i], variableVectors[i],
        0, recordsInBatches[i], totalRecords);
      totalRecords += recordsInBatches[i];
    }
  }

  public static void unpivot(PivotDef pivot, final FixedBlockVector fixedVector,
                             final VariableBlockVector variableVector, final int start, final int count) {
    for (FieldVector v : pivot.getOutputVectors()) {
      AllocationHelper.allocate(v, count, 15);
    }
    unpivotToAllocedOutput(pivot, fixedVector, variableVector, start, count, 0);
  }

  public static void unpivotToAllocedOutput(PivotDef pivot, final FixedBlockVector fixedVector,
                                            final VariableBlockVector variableVector, final int start, final int count, final int seekInOutput) {
    final int blockWidth = pivot.getBlockWidth();
    final long fixedAddr = fixedVector.getMemoryAddress();
    final long variableAddr = variableVector.getMemoryAddress();

    // unpivots bit arrays
    for(VectorPivotDef v : pivot.getVectorPivots()){
      final List<ArrowBuf> buffers = v.getOutgoingVector().getFieldBuffers();
      unpivotBits1(fixedAddr, blockWidth, buffers.get(0).memoryAddress(), v.getNullByteOffset(),
        v.getNullBitOffset(), start, count, seekInOutput);
      if(v.getType().mode == FieldMode.BIT){
        unpivotBits1(fixedAddr, blockWidth, buffers.get(1).memoryAddress(), v.getNullByteOffset(),
          v.getNullBitOffset() + 1, start, count, seekInOutput);
      }
    }

    // unpivot fixed values.
    for(VectorPivotDef def : pivot.getNonBitFixedPivots()){
      switch(def.getType()){
      case FOUR_BYTE:
        final long buf4ByteAddr = def.getOutgoingVector().getFieldBuffers().get(1).memoryAddress();
        unpivotBytes4(fixedAddr, blockWidth, buf4ByteAddr, def.getOffset(), start, count, seekInOutput);
        break;
      case EIGHT_BYTE:
        final long buf8ByteAddr = def.getOutgoingVector().getFieldBuffers().get(1).memoryAddress();
        unpivotBytes8(fixedAddr, blockWidth, buf8ByteAddr, def.getOffset(), start, count, seekInOutput);
        break;
      case SIXTEEN_BYTE:
        final long buf16ByteAddr = def.getOutgoingVector().getFieldBuffers().get(1).memoryAddress();
        unpivotBytes16(fixedAddr, blockWidth, buf16ByteAddr, def.getOffset(), start, count, seekInOutput);
        break;
      default:
        throw new IllegalStateException();
      }
    }

    // unpivot variable fields.
    final FieldVector[] varVectors = FluentIterable.from(pivot.getVariablePivots()).transform(new Function<VectorPivotDef, FieldVector>(){
      @Override
      public FieldVector apply(VectorPivotDef input) {
        return input.getOutgoingVector();
      }}).toArray(FieldVector.class);

    unpivotVariable(fixedAddr, variableAddr, blockWidth, varVectors, start, count, seekInOutput);
  }

  public static long[] addresses(Collection<FieldVector> vectors){
    final long[] addresses = new long[vectors.size()];
    int offset = 0;
    for(FieldVector vector : vectors){
      offset = fillAddresses(addresses, offset, vector);
    }

    return addresses;
  }

  public static long[] addresses(FieldVector... vectors){
    final long[] addresses = new long[vectors.length];
    int offset = 0;
    for(FieldVector vector : vectors){
      offset = fillAddresses(addresses, offset, vector);
    }

    return addresses;
  }

  public static int fillAddresses(long[] addresses, int offset, FieldVector vector){
    for(ArrowBuf ab : vector.getFieldBuffers()){
      addresses[offset] = ab.memoryAddress();
      offset++;
    }
    return offset;
  }
}
