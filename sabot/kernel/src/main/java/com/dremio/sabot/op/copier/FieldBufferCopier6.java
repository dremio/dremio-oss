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

import static com.dremio.sabot.op.common.hashtable.HashTable.BUILD_RECORD_LINK_SIZE;

import java.util.List;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.sabot.op.common.ht2.Reallocators;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Vectorized copiers for 6 byte offset vectors (first 4 bytes for batch and next 2 bytes for offset in batch)
 */
public class FieldBufferCopier6 {

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;
  private static final int OFFSET_BUFFER_ORDINAL = 1;
  private static final int MAX_BATCH = 65535;
  private static final int BATCH_BITS = 16;

  private FieldBufferCopier6(){};

  static class FourByteCopier extends FieldBufferCopier {
    private static final int SIZE = 4;
    private final FieldVector[] source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final long[] srcAddrs;

    public FourByteCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
      this.srcAddrs = addresses(VALUE_BUFFER_ORDINAL, source);
    }

    @Override
    public void copy(long offsetAddr, int count) {
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        final int batchOffset = PlatformDependent.getShort(addr + 4);
        final long src = srcAddrs[batchIndex] + batchOffset * SIZE;
        PlatformDependent.putInt(dstAddr, PlatformDependent.getInt(src));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class EightByteCopier extends FieldBufferCopier {

    private static final int SIZE = 8;
    private final FieldVector[] source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final long[] srcAddrs;


    public EightByteCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
      this.srcAddrs = addresses(VALUE_BUFFER_ORDINAL, source);
    }

    @Override
    public void copy(long offsetAddr, int count) {
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        final int batchOffset = PlatformDependent.getShort(addr + 4);
        final long src = srcAddrs[batchIndex] + batchOffset * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(src));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class SixteenByteCopier extends FieldBufferCopier {
    private static final int SIZE = 16;
    private final FieldVector[] source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final long[] srcAddrs;

    public SixteenByteCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
      this.srcAddrs = addresses(VALUE_BUFFER_ORDINAL, source);
    }

    @Override
    public void copy(long offsetAddr, int count) {
      targetAlt.allocateNew(count);
      final List<ArrowBuf> targetBuffers = target.getFieldBuffers();
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = targetBuffers.get(VALUE_BUFFER_ORDINAL).memoryAddress();
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        final int batchOffset = PlatformDependent.getShort(addr + 4);
        final long src = srcAddrs[batchIndex] + batchOffset * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(src));
        PlatformDependent.putLong(dstAddr+8, PlatformDependent.getLong(src + 8));
      }
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class VariableCopier extends FieldBufferCopier {
    private static final int AVG_VAR_WIDTH = 15;
    private final FieldVector[] source;
    private final FieldVector target;
    private final VariableWidthVector targetAlt;
    private final Reallocator realloc;
    private final long[] srcOffsetAddrs;
    private final long[] srcDataAddrs;

    public VariableCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.targetAlt = (VariableWidthVector) target;
      this.target = target;
      this.realloc = Reallocators.getReallocator(target);
      this.srcOffsetAddrs =  addresses(OFFSET_BUFFER_ORDINAL, source);
      this.srcDataAddrs = addresses(2, source);
    }

    @Override
    public void copy(long srcAddr, int count) {
      final Reallocator realloc = this.realloc;

      final long maxSrcAddr = srcAddr + count * BUILD_RECORD_LINK_SIZE;
      final long srcOffsetAddrs[] = this.srcOffsetAddrs;
      final long srcDataAddrs[] = this.srcDataAddrs;

      targetAlt.allocateNew(AVG_VAR_WIDTH * count, count);
      long dstOffsetAddr = target.getFieldBuffers().get(OFFSET_BUFFER_ORDINAL).memoryAddress() + 4;
      long initDataAddr = realloc.addr();
      long curDataAddr = realloc.addr();
      long maxDataAddr = realloc.max();
      int lastOffset = 0;

      for(; srcAddr < maxSrcAddr; srcAddr += BUILD_RECORD_LINK_SIZE, dstOffsetAddr += 4){
        final int batchIndex = PlatformDependent.getInt(srcAddr);
        final int batchOffset = PlatformDependent.getShort(srcAddr+ 4);

        final long startAndEnd = PlatformDependent.getLong(srcOffsetAddrs[batchIndex] + batchOffset * 4);
        final int firstOffset = (int) startAndEnd;
        final int secondOffset = (int) (startAndEnd >> 32);
        final int len = secondOffset - firstOffset;
        if(curDataAddr + len > maxDataAddr){
          initDataAddr = realloc.ensure(lastOffset + len);
          curDataAddr = initDataAddr + lastOffset;
          maxDataAddr = realloc.max();
        }

        lastOffset += len;
        PlatformDependent.putInt(dstOffsetAddr, lastOffset);
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddrs[batchIndex] + firstOffset, curDataAddr, len);
        curDataAddr += len;
      }

      realloc.setCount(count);
    }

    public void allocate(int records){
      targetAlt.allocateNew(records * 15, records);
    }
  }

  static class BitCopier extends FieldBufferCopier {

    private final FieldVector[] source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final int bufferOrdinal;
    private final boolean allocateAsFixed;
    private final long[] srcAddrs;

    public BitCopier(FieldVector[] source, FieldVector target, int bufferOrdinal, boolean allocateAsFixed){
      this.source = source;
      this.target = target;
      this.targetAlt = allocateAsFixed ? (FixedWidthVector) target : null;
      this.allocateAsFixed = allocateAsFixed;
      this.bufferOrdinal = bufferOrdinal;
      this.srcAddrs = addresses(bufferOrdinal, source);

    }

    @Override
    public void copy(long offsetAddr, int count) {
      if(allocateAsFixed){
        targetAlt.allocateNew(count);
      }
      final long[] srcAddr = this.srcAddrs;
      final long dstAddr = target.getFieldBuffers().get(bufferOrdinal).memoryAddress();

      final long maxAddr = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      int targetIndex = 0;
      for(; offsetAddr < maxAddr; offsetAddr += BUILD_RECORD_LINK_SIZE, targetIndex++){
        final int batchIndex = PlatformDependent.getInt(offsetAddr);
        final int batchOffset = PlatformDependent.getShort(offsetAddr + 4);
        final int byteValue = PlatformDependent.getByte(srcAddr[batchIndex] + (batchOffset >>> 3));
        final int bitVal = ((byteValue >>> (batchOffset & 7)) & 1) << (targetIndex & 7);
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
    private final TransferPair[] transfer;
    private final FieldVector dst;

    public GenericCopier(FieldVector[] source, FieldVector dst){
      this.transfer = new TransferPair[source.length];
      for(int i =0; i < source.length; i++){
        this.transfer[i] = source[i].makeTransferPair(dst);
      }
      this.dst = dst;
    }

    @Override
    public void copy(long offsetAddr, int count) {
      dst.allocateNew();
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      int target = 0;
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE) {
        final int batchIndex = PlatformDependent.getInt(addr);
        final int batchOffset = PlatformDependent.getShort(addr + 4);
        transfer[batchIndex].copyValueSafe(batchOffset, target);
        target++;
      }
    }

    public void allocate(int records){
      AllocationHelper.allocate(dst, records, 10);
    }
  }

  private static void addValueCopier(final FieldVector[] source, final FieldVector target, ImmutableList.Builder<FieldBufferCopier> copiers){
    Preconditions.checkArgument(source.length > 0, "At least one source vector must be requested.");
    Preconditions.checkArgument(source[0].getClass() == target.getClass(), "Input and output vectors must be same type.");
    switch(CompleteType.fromField(source[0].getField()).toMinorType()){

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

  public static ImmutableList<FieldBufferCopier> getFourByteCopiers(List<FieldVector[]> inputs, List<FieldVector> outputs){
    ImmutableList.Builder<FieldBufferCopier> copiers = ImmutableList.builder();

    Preconditions.checkArgument(inputs.size() == outputs.size(), "Input and output lists must be same size.");
    for(int i = 0; i < inputs.size(); i++){
      final FieldVector[] input = inputs.get(i);
      final FieldVector output = outputs.get(i);
      addValueCopier(input, output, copiers);
    }
    return copiers.build();
  }


  public static long[] addresses(int offset, FieldVector... vectors){
    final long[] addresses = new long[vectors.length];
    for(int i = 0; i < vectors.length; i++){
      addresses[i] = vectors[i].getFieldBuffers().get(offset).memoryAddress();
    }

    return addresses;
  }
}
