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
package com.dremio.sabot.op.common.ht2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder.FieldMode;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class Unpivots {

  private Unpivots(){}

  public static void unpivotBits1(long srcFixedAddr, int blockWidth, final long target, final int bitByteOffset, final int bitOffset, int count){

    final long startAddr = srcFixedAddr + bitByteOffset;
    long maxAddr = startAddr + (count * blockWidth);
    int targetIndex = 0;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetIndex++){
      final int byteValue = PlatformDependent.getInt(srcAddr);
      int bitVal = ((byteValue >>> bitOffset ) & 1 ) << (targetIndex & 31);
      final long addr = target + ((targetIndex >>> 5) * 4);
      PlatformDependent.putInt(addr, PlatformDependent.getInt(addr) | bitVal);
    }
  }

  public static void unpivotBytes4(final long srcFixedAddr, final int blockWidth, final long target, final int byteOffset, int count) {
    final long startAddr = srcFixedAddr;
    long maxAddr = startAddr + (count * blockWidth);
    long targetAddr = target;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetAddr+=4){
      final int value = PlatformDependent.getInt(srcAddr + byteOffset);
      PlatformDependent.putInt(targetAddr, value);
    }
  }

  public static void unpivotBytes8(final long srcFixedAddr, final int blockWidth, final long target, final int byteOffset, int count) {
    final long startAddr = srcFixedAddr;
    long maxAddr = startAddr + (count * blockWidth);
    long targetAddr = target;

    for(long srcAddr = startAddr; srcAddr < maxAddr; srcAddr += blockWidth, targetAddr+=8){
      final long value = PlatformDependent.getLong(srcAddr + byteOffset);
      PlatformDependent.putLong(targetAddr, value);
    }
  }


  public static void unpivotVariable(final long srcFixedAddr, final long srcVarAddr, final int blockWidth, FieldVector[] targets, int count) {
    final int dataWidth = blockWidth - LBlockHashTable.VAR_OFFSET_SIZE;
    final long startVarOffset = srcFixedAddr + dataWidth;
    final long maxAddr = startVarOffset + (count * blockWidth);
    final long srcVarAddrBase = srcVarAddr;
    final int fieldCount = targets.length;

    final long[] targetAddrs = new long[fieldCount];
    final long[] offsetAddrs = new long[fieldCount];
    final long[] maxTargetAddrs = new long[fieldCount];
    final Reallocator[] reallocs = new Reallocator[fieldCount];

    for(int i = 0; i < fieldCount; i++){
      FieldVector vect = targets[i];
      offsetAddrs[i] = vect.getFieldBuffers().get(1).memoryAddress();
      Reallocator realloc = Reallocators.getReallocator(vect);
      reallocs[i] = realloc;
      targetAddrs[i] = realloc.addr();
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
        int start = PlatformDependent.getInt(offsetAddr);
        offsetAddr +=4;
        PlatformDependent.putInt(offsetAddr, start + len);
        PlatformDependent.copyMemory(varPos, target, len);
        offsetAddrs[i] = offsetAddr;
        targetAddrs[i] += len;
        varPos += len;
      }
    }

    for(Reallocator r : reallocs){
      r.setCount(count);
    }
  }

  public static void unpivot(PivotDef pivot, final FixedBlockVector fixedVector, final VariableBlockVector variableVector, final int count){
    final int blockWidth = pivot.getBlockWidth();
    final int totalBitCount = pivot.getBitCount();
    int bitCount = pivot.getBitCount();
    for(FieldVector v : pivot.getOutputVectors()){
      AllocationHelper.allocate(v, LBlockHashTable.MAX_VALUES_PER_BATCH, 15);
    }
    List<ArrowBuf> bitBufs = new ArrayList<>();
    for(VectorPivotDef v : pivot.getVectorPivots()){
      final List<ArrowBuf> buffers = v.getOutgoingVector().getFieldBuffers();
      bitBufs.add(buffers.get(0));
      if(v.getType().mode == FieldMode.BIT){
        bitBufs.add(buffers.get(1));
      }
    }
    final long fixedAddr = fixedVector.getMemoryAddress();
    final long variableAddr = variableVector.getMemoryAddress();

    // unpivots bit arrays
    for(int i =0; i < bitCount; i++){
      unpivotBits1(fixedAddr, blockWidth, bitBufs.get(totalBitCount - bitCount + i).memoryAddress(), (i/32), i, count);
    }

    // unpivot fixed values.
    for(VectorPivotDef def : pivot.getNonBitFixedPivots()){
      switch(def.getType()){
      case FOUR_BYTE:
        unpivotBytes4(fixedAddr, blockWidth, def.getOutgoingVector().getFieldBuffers().get(1).memoryAddress(), def.getOffset(), count);
        break;
      case EIGHT_BYTE:
        unpivotBytes8(fixedAddr, blockWidth, def.getOutgoingVector().getFieldBuffers().get(1).memoryAddress(), def.getOffset(), count);
        break;

      case SIXTEEN_BYTE:
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

    unpivotVariable(fixedAddr, variableAddr, blockWidth, varVectors, count);
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
