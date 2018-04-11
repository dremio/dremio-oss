/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.ht2.Reallocators;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;


public abstract class FieldBufferCopier {

  private static final int STEP_SIZE = 2;

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;
  private static final int OFFSET_BUFFER_ORDINAL = 1;
  private static final int VARIABLE_DATA_BUFFER_ORDINAL = 2;

  public abstract void allocate(int records);
  public abstract void copy(long offsetAddr, int count);

  static class FourByteCopier extends FieldBufferCopier {
    private static final int SIZE = 4;
    private final FieldVector source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;

    public FourByteCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      final List<ArrowBuf> sourceBuffers = source.getFieldBuffers();
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * 2;
      final long srcAddr = sourceBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        PlatformDependent.putInt(dstAddr, PlatformDependent.getInt(srcAddr + ((char) PlatformDependent.getShort(addr)) * SIZE));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class EightByteCopier extends FieldBufferCopier {

    private static final int SIZE = 8;
    private final FieldVector source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;


    public EightByteCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      final List<ArrowBuf> sourceBuffers = source.getFieldBuffers();
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * STEP_SIZE;
      final long srcAddr = sourceBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr + ((char) PlatformDependent.getShort(addr)) * SIZE));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class SixteenByteCopier extends FieldBufferCopier {
    private static final int SIZE = 16;
    private final FieldVector source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;

    public SixteenByteCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      final List<ArrowBuf> sourceBuffers = source.getFieldBuffers();
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * STEP_SIZE;
      final long srcAddr = sourceBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        final int offset = ((char) PlatformDependent.getShort(addr)) * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr + offset));
        PlatformDependent.putLong(dstAddr+8, PlatformDependent.getLong(srcAddr + + offset + 8));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class VariableCopier extends FieldBufferCopier {
    private static final int AVG_VAR_WIDTH = 15;
    private final FieldVector source;
    private final FieldVector target;
    private final VariableWidthVector targetAlt;
    private final Reallocator realloc;

    public VariableCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.targetAlt = (VariableWidthVector) target;
      this.target = target;
      this.realloc = Reallocators.getReallocator(target);
    }

    @Override
    public void copy(long sv2, int count) {
      final Reallocator realloc = this.realloc;
      // make sure vectors are internally consistent
      VariableLengthValidator.validateVariable(source, source.getValueCount());

      final List<ArrowBuf> sourceBuffers = source.getFieldBuffers();


      final long maxSv2 = sv2 + count * STEP_SIZE;
      final long srcOffsetAddr = sourceBuffers.get(OFFSET_BUFFER_ORDINAL).memoryAddress();
      final long srcDataAddr = sourceBuffers.get(VARIABLE_DATA_BUFFER_ORDINAL).memoryAddress();

      targetAlt.allocateNew(AVG_VAR_WIDTH * count, count);
      long dstOffsetAddr = target.getFieldBuffers().get(OFFSET_BUFFER_ORDINAL).memoryAddress() + 4;
      long curDataAddr = realloc.addr(); // start address for next copy in target
      long maxDataAddr = realloc.max(); // max bytes we can copy to target before we need to reallocate
      int lastOffset = 0; // total bytes copied in target so far

      for(; sv2 < maxSv2; sv2 += STEP_SIZE, dstOffsetAddr += 4){
        // copy from recordIndex to last available position in target
        final int recordIndex = (char) PlatformDependent.getShort(sv2);
        // retrieve start offset and length of value we want to copy
        final long startAndEnd = PlatformDependent.getLong(srcOffsetAddr + recordIndex * 4);
        final int firstOffset = (int) startAndEnd;
        final int secondOffset = (int) (startAndEnd >> 32);
        final int len = secondOffset - firstOffset;
        // check if we need to reallocate target buffer
        if (curDataAddr + len > maxDataAddr) {
          curDataAddr = realloc.ensure(lastOffset + len) + lastOffset;
          maxDataAddr = realloc.max();
        }

        lastOffset += len;
        PlatformDependent.putInt(dstOffsetAddr, lastOffset);
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddr + firstOffset, curDataAddr, len);
        curDataAddr += len;
      }

      realloc.setCount(count);
    }

    public void allocate(int records){
      targetAlt.allocateNew(records * 15, records);
    }
  }

  static class BitCopier extends FieldBufferCopier {

    private final FieldVector source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final int bufferOrdinal;
    private final boolean allocateAsFixed;

    public BitCopier(FieldVector source, FieldVector target, int bufferOrdinal, boolean allocateAsFixed){
      this.source = source;
      this.target = target;
      this.targetAlt = allocateAsFixed ? (FixedWidthVector) target : null;
      this.allocateAsFixed = allocateAsFixed;
      this.bufferOrdinal = bufferOrdinal;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      if(allocateAsFixed){
        targetAlt.allocateNew(count);
      }
      final long srcAddr = source.getFieldBuffers().get(bufferOrdinal).memoryAddress();
      final long dstAddr = target.getFieldBuffers().get(bufferOrdinal).memoryAddress();

      final long maxAddr = offsetAddr + count * STEP_SIZE;
      int targetIndex = 0;
      for(; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++){
        final int recordIndex = (char) PlatformDependent.getShort(offsetAddr);
        final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
        final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
        final long addr = dstAddr + (targetIndex >>> 3);
        PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
      }
    }

    public void allocate(int records){
      if(targetAlt != null){
        targetAlt.allocateNew(records);
      }
    }

  }

  static class GenericCopier extends FieldBufferCopier {
    private final TransferPair transfer;
    private final FieldVector dst;

    public GenericCopier(FieldVector source, FieldVector dst){
      this.transfer = source.makeTransferPair(dst);
      this.dst = dst;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      dst.allocateNew();
      final long max = offsetAddr + count * STEP_SIZE;
      int target = 0;
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE) {
        int index = (char) PlatformDependent.getShort(addr);
        transfer.copyValueSafe(index, target);
        target++;
      }
    }

    public void allocate(int records){
      AllocationHelper.allocate(dst, records, 10);
    }
  }

  private static void addValueCopier(final FieldVector source, final FieldVector target, ImmutableList.Builder<FieldBufferCopier> copiers){
    Preconditions.checkArgument(source.getClass() == target.getClass(), "Input and output vectors must be same type.");
    switch(CompleteType.fromField(source.getField()).toMinorType()){

    case TIMESTAMP:
    case FLOAT8:
    case BIGINT:
    case INTERVALDAY:
    case DATE:
      copiers.add(new EightByteCopier(source, target));
      copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL, false));
      break;

    case BIT:
      copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL, true));
      copiers.add(new BitCopier(source, target, VALUE_BUFFER_ORDINAL, false));
      break;

    case TIME:
    case FLOAT4:
    case INT:
    case INTERVALYEAR:
      copiers.add(new FourByteCopier(source, target));
      copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL, false));
      break;

    case VARBINARY:
    case VARCHAR:
      copiers.add(new VariableCopier(source, target));
      copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL, false));
      break;

    case DECIMAL:
      copiers.add(new SixteenByteCopier(source, target));
      copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL, false));
      break;

    case LIST:
    case MAP:
    case UNION:
      copiers.add(new GenericCopier(source, target));
      break;

    default:
      throw new UnsupportedOperationException("Unknown type to copy.");
    }
  }

  public static ImmutableList<FieldBufferCopier> getCopiers(List<FieldVector> inputs, List<FieldVector> outputs){
    ImmutableList.Builder<FieldBufferCopier> copiers = ImmutableList.builder();

    Preconditions.checkArgument(inputs.size() == outputs.size(), "Input and output lists must be same size.");
    for(int i = 0; i < inputs.size(); i++){
      final FieldVector input = inputs.get(i);
      final FieldVector output = outputs.get(i);
      addValueCopier(input, output, copiers);
    }
    return copiers.build();
  }
}
