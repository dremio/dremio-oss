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

import io.netty.util.internal.PlatformDependent;

/**
 * Vectorized copiers for 4 byte offset vectors (by sv4)
 */
public final class FieldBufferCopier4Util {

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;
  private static final int OFFSET_BUFFER_ORDINAL = 1;
  private static final int MAX_BATCH = 65535;
  private static final int BATCH_BITS = 16;
  private static final int STEP_SIZE = 4;

  private FieldBufferCopier4Util(){};

  static class FourByteCopier implements FieldBufferCopier {
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
      final long max = offsetAddr + count * STEP_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        final int sv4 = PlatformDependent.getInt(addr);
        PlatformDependent.putInt(dstAddr, PlatformDependent.getInt(srcAddrs[sv4 >>> BATCH_BITS] + (sv4 & MAX_BATCH) * SIZE));
      }
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class EightByteCopier implements FieldBufferCopier {

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
      final long max = offsetAddr + count * STEP_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        final int sv4 = PlatformDependent.getInt(addr);
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddrs[sv4 >>> BATCH_BITS] + (sv4 & MAX_BATCH) * SIZE));
      }
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class SixteenByteCopier implements FieldBufferCopier {
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
      final long max = offsetAddr + count * STEP_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress();
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE){
        final int sv4 = PlatformDependent.getInt(addr);
        final long src = srcAddrs[sv4 >>> BATCH_BITS] + (sv4 & MAX_BATCH) * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(src));
        PlatformDependent.putLong(dstAddr+8, PlatformDependent.getLong(src + 8));
      }
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      targetAlt.allocateNew(records);
    }
  }

  static class VariableCopier implements FieldBufferCopier {
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
    public void copy(long sv4Addr, int count) {
      final Reallocator realloc = this.realloc;

      final long maxSV4 = sv4Addr + count * STEP_SIZE;
      final long[] srcOffsetAddrs = this.srcOffsetAddrs;
      final long[] srcDataAddrs = this.srcDataAddrs;

      targetAlt.allocateNew(AVG_VAR_WIDTH * count, count);
      long dstOffsetAddr = target.getOffsetBufferAddress() + 4;
      long initDataAddr = realloc.addr();
      long curDataAddr = realloc.addr();
      long maxDataAddr = realloc.max();
      int lastOffset = 0;

      for(; sv4Addr < maxSV4; sv4Addr += STEP_SIZE, dstOffsetAddr += 4){
        final int batchNOFF = PlatformDependent.getInt(sv4Addr);
        final int batchNumber =  batchNOFF >>> BATCH_BITS;
        final int batchOffset = batchNOFF & MAX_BATCH;

        final long startAndEnd = PlatformDependent.getLong(srcOffsetAddrs[batchNumber] + batchOffset * 4);
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
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddrs[batchNumber] + firstOffset, curDataAddr, len);
        curDataAddr += len;
      }

      realloc.setCount(count);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      targetAlt.allocateNew(records * 15, records);
    }
  }

  static class BitCopier implements FieldBufferCopier {

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
      final long dstAddr;
      switch (bufferOrdinal) {
        case NULL_BUFFER_ORDINAL:
          dstAddr = target.getValidityBufferAddress();
          break;
        case VALUE_BUFFER_ORDINAL:
          dstAddr = target.getDataBufferAddress();
          break;
        default:
          throw new UnsupportedOperationException("unexpected buffer offset");
      }

      final long maxAddr = offsetAddr + count * STEP_SIZE;
      int targetIndex = 0;
      for(; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++){
        final int batchNOFF = PlatformDependent.getInt(offsetAddr);
        final int recordIndex = batchNOFF & MAX_BATCH;
        final int byteValue = PlatformDependent.getByte(srcAddr[batchNOFF >>> BATCH_BITS] + (recordIndex >>> 3));
        final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
        final long addr = dstAddr + (targetIndex >>> 3);
        PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
      }
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      if(targetAlt != null){
        targetAlt.allocateNew(records);
      }
    }

  }

  static class GenericCopier implements FieldBufferCopier {
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
      final long max = offsetAddr + count * STEP_SIZE;
      int target = 0;
      for(long addr = offsetAddr; addr < max; addr += STEP_SIZE) {
        final int batchNOFF = PlatformDependent.getInt(addr);
        transfer[batchNOFF >>> BATCH_BITS].copyValueSafe(batchNOFF & MAX_BATCH, target);
        target++;
      }
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void allocate(int records){
      AllocationHelper.allocate(dst, records, 10);
    }
  }

  public static void addValueCopier(final FieldVector[] source, final FieldVector target, ImmutableList.Builder<FieldBufferCopier> copiers){
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

    case MAP:
    case LIST:
    case STRUCT:
    case UNION:
      copiers.add(new GenericCopier(source, target));
      break;

    default:
      throw new UnsupportedOperationException("Unknown type to copy.");
    }
  }



  public static long[] addresses(int offset, FieldVector... vectors){
    final long[] addresses = new long[vectors.length];
    int i;
    switch (offset) {
      case 0:
        for(i = 0; i < vectors.length; i++){
          addresses[i] = vectors[i].getValidityBufferAddress();
        }
        break;
      case 1:
        for(i = 0; i < vectors.length; i++){
          if (vectors[i] instanceof VariableWidthVector) {
            addresses[i] = vectors[i].getOffsetBufferAddress();
          } else {
            addresses[i] = vectors[i].getDataBufferAddress();
          }
        }
        break;
      case 2:
        for(i = 0; i < vectors.length; i++){
          addresses[i] = vectors[i].getDataBufferAddress();
        }
        break;
      default:
        throw new UnsupportedOperationException("unexpected buffer offset");
    }

    return addresses;
  }
}
