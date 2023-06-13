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

import java.util.List;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos;
import com.dremio.sabot.op.common.ht2.Copier;
import com.dremio.sabot.op.common.ht2.Reallocators;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * Vectorized copiers for 6 byte offset vectors (first 4 bytes for batch and next 2 bytes for offset in batch)
 */
public final class ConditionalFieldBufferCopier6Util {

  private static final int SKIP = -1;

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;
  private static final int OFFSET_BUFFER_ORDINAL = 1;
  private static final int MAX_BATCH = 65535;
  private static final int BATCH_BITS = 16;

  private ConditionalFieldBufferCopier6Util(){};

  abstract static class FixedWidthCopier implements FieldBufferCopier {
    protected final FieldVector[] source;
    protected final FieldVector target;
    protected final FixedWidthVector targetAlt;
    protected final long[] srcAddrs;

    public FixedWidthCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
      this.srcAddrs = addresses(VALUE_BUFFER_ORDINAL, source);
    }

    abstract void seekAndCopy(long offsetAddr, int count, int seekTo);

    @Override
    public void copy(long offsetAddr, int count) {
      targetAlt.allocateNew(count);
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int curTargetIndex = cursor.getTargetIndex();
      resizeIfNeeded(targetAlt, curTargetIndex + count);
      seekAndCopy(offsetAddr, count, curTargetIndex);
      cursor.setTargetIndex(curTargetIndex + count);
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

  static class FourByteCopier extends FixedWidthCopier {
    private static final int SIZE = 4;

    public FourByteCopier(FieldVector[] source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        if(batchIndex != SKIP){
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(addr + 4));
          final long srcAddr = srcAddrs[batchIndex] + batchOffset * SIZE;
          PlatformDependent.putInt(dstAddr, PlatformDependent.getInt(srcAddr));
        }
      }
    }
  }

  static class EightByteCopier extends FixedWidthCopier {
    private static final int SIZE = 8;

    public EightByteCopier(FieldVector[] source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        if(batchIndex != SKIP){
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(addr + 4));
          final long srcAddr = srcAddrs[batchIndex] + batchOffset * SIZE;
          PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr));
        }
      }
    }
  }

  static class SixteenByteCopier extends FixedWidthCopier {
    private static final int SIZE = 16;

    public SixteenByteCopier(FieldVector[] source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcAddrs = this.srcAddrs;
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE, dstAddr += SIZE){
        final int batchIndex = PlatformDependent.getInt(addr);
        if(batchIndex != SKIP){
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(addr + 4));
          final long srcAddr = srcAddrs[batchIndex] + batchOffset * SIZE;
          PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr));
          PlatformDependent.putLong(dstAddr+8, PlatformDependent.getLong(srcAddr + 8));
        }
      }
    }
  }

  static class VariableCopier implements FieldBufferCopier {
    private final FieldVector[] source;
    private final FieldVector target;
    private final VariableWidthVector targetAlt;
    private final Reallocator realloc;
    private final long[] srcOffsetAddrs;
    private final long[] srcDataAddr;

    public VariableCopier(FieldVector[] source, FieldVector target) {
      this.source = source;
      this.targetAlt = (VariableWidthVector) target;
      this.target = target;
      this.realloc = Reallocators.getReallocator(target);
      this.srcOffsetAddrs = addresses(OFFSET_BUFFER_ORDINAL, source);
      this.srcDataAddr = addresses(2, source);
    }

    private void seekAndCopy(long srcAddr, int count, int seekTo) {
      int targetIndex = seekTo;
      int targetDataIndex = 0;
      if (targetIndex != 0) {
        long dstOffsetAddr = target.getOffsetBufferAddress() + targetIndex * 4;
        targetDataIndex = PlatformDependent.getInt(dstOffsetAddr);
      }

      final Reallocator realloc = this.realloc;

      final long maxSrcAddr = srcAddr + count * BUILD_RECORD_LINK_SIZE;
      final long[] srcOffsetAddrs = this.srcOffsetAddrs;
      final long[] srcDataAddr = this.srcDataAddr;

      long dstOffsetAddr = target.getOffsetBufferAddress() + (targetIndex + 1) * 4;
      long curDataAddr = realloc.addr() + targetDataIndex;
      long maxDataAddr = realloc.max();

      for(; srcAddr < maxSrcAddr; srcAddr += BUILD_RECORD_LINK_SIZE, dstOffsetAddr += 4){
        final int batchIndex = PlatformDependent.getInt(srcAddr);
        if(batchIndex == SKIP){
          PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
        } else {
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(srcAddr + 4));

          final long startAndEnd = PlatformDependent.getLong(srcOffsetAddrs[batchIndex] + batchOffset * 4);
          final int firstOffset = (int) startAndEnd;
          final int secondOffset = (int) (startAndEnd >> 32);
          final int len = secondOffset - firstOffset;
          if(curDataAddr + len > maxDataAddr){
            curDataAddr = realloc.ensure(targetDataIndex + len) + targetDataIndex;
            maxDataAddr = realloc.max();
          }

          targetDataIndex += len;
          PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
          Copier.copy(srcDataAddr[batchIndex] + firstOffset, curDataAddr, len);
          curDataAddr += len;
        }

      }

      realloc.setCount(targetIndex + count);
    }

    @Override
    public void copy(long srcAddr, int count) {
      targetAlt.allocateNew(AVG_VAR_WIDTH * count, count);
      seekAndCopy(srcAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      while (targetAlt.getValueCapacity() < targetIndex + count) {
        targetAlt.reAlloc();
      }
      seekAndCopy(offsetAddr, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    @Override
    public void allocate(int records){
      targetAlt.allocateNew(records * AVG_VAR_WIDTH, records);
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

    private void seekAndCopy(long offsetAddr, int count, int seekTo) {
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

      final long maxAddr = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      int targetIndex = seekTo;
      for(; offsetAddr < maxAddr; offsetAddr += BUILD_RECORD_LINK_SIZE, targetIndex++){
        final int batchIndex = PlatformDependent.getInt(offsetAddr);
        if(batchIndex != SKIP){
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr + 4));
          final int byteValue = PlatformDependent.getByte(srcAddr[batchIndex] + (batchOffset >>> 3));
          final int bitVal = ((byteValue >>> (batchOffset & 7)) & 1) << (targetIndex & 7);
          final long addr = dstAddr + (targetIndex >>> 3);
          PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
        }
      }
    }

    @Override
    public void copy(long offsetAddr, int count) {
      if (allocateAsFixed){
        targetAlt.allocateNew(count);
      }
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int curTargetIndex = cursor.getTargetIndex();
      if (allocateAsFixed) {
        while (targetAlt.getValueCapacity() < curTargetIndex + count) {
          targetAlt.reAlloc();
        }
      }
      seekAndCopy(offsetAddr, count, curTargetIndex);
      cursor.setTargetIndex(curTargetIndex +  count);
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

    private void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * BUILD_RECORD_LINK_SIZE;
      int target = seekTo;
      for(long addr = offsetAddr; addr < max; addr += BUILD_RECORD_LINK_SIZE) {
        final int batchIndex = PlatformDependent.getInt(addr);
        if(batchIndex != SKIP){
          final int batchOffset = Short.toUnsignedInt(PlatformDependent.getShort(addr + 4));
          transfer[batchIndex].copyValueSafe(batchOffset, target);
        }
        target++;
      }
    }

    @Override
    public void copy(long offsetAddr, int count) {
      dst.allocateNew();
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      if (cursor.getTargetIndex() == 0) {
        dst.allocateNew();
      }

      int curTargetIndex = cursor.getTargetIndex();
      seekAndCopy(offsetAddr, count, curTargetIndex);
      cursor.setTargetIndex(curTargetIndex +  count);
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

  /**
   * Used in HashJoin for a partition which has an empty build side table (and hence, no matches). In this
   * case, the copier should effectively set null values in the target, and update cursor.
   */
  static class NoOpEmptySourceCopier implements FieldBufferCopier {
    FieldVector target;

    NoOpEmptySourceCopier(FieldVector target) {
      this.target = target;
    }

    @Override
    public void allocate(int records) {
      target.allocateNew();
    }

    @Override
    public void copy(long offsetAddr, int count) {
      target.allocateNew();
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      if (cursor.getTargetIndex() == 0) {
        target.allocateNew();
      }
      int targetSize = count + cursor.getTargetIndex();
      cursor.setTargetIndex(targetSize);
    }
  }

  /**
   * Used in HashJoin for a partition which has an empty build side table (and hence, no matches). In this
   * case, the copier should update the offsets buffer and the cursor.
   */
  static class VariableEmptySourceCopier implements FieldBufferCopier {
    private final FieldVector target;
    private final VariableWidthVector targetAlt;
    private final Reallocator realloc;

    VariableEmptySourceCopier(FieldVector target) {
      this.target = target;
      this.targetAlt = (VariableWidthVector) target;
      this.realloc = Reallocators.getReallocator(target);
    }

    @Override
    public void allocate(int records) {
      targetAlt.allocateNew(records * AVG_VAR_WIDTH, records);
    }

    @Override
    public void copy(long offsetAddr, int count) {
      targetAlt.allocateNew(count * AVG_VAR_WIDTH, count);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      int targetDataIndex;

      if (targetIndex == 0) {
        allocate(count);
        targetDataIndex = 0;
      } else {
        while (targetAlt.getValueCapacity() < targetIndex + count) {
          targetAlt.reAlloc();
        }
        long dstOffsetAddr = target.getOffsetBufferAddress() + targetIndex * 4;
        targetDataIndex = PlatformDependent.getInt(dstOffsetAddr);
      }

      long dstOffsetAddr = target.getOffsetBufferAddress() + (targetIndex + 1) * 4;
      for (int index = 0; index < count; index++, dstOffsetAddr += 4){
        PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
      }

      realloc.setCount(targetIndex + count);
      cursor.setTargetIndex(targetIndex + count);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      throw new UnsupportedOperationException("set null not supported");
    }
  }

  public static ImmutableList<FieldBufferCopier> getEmptySourceFourByteCopiers(List<FieldVector> outputs){
    ImmutableList.Builder<FieldBufferCopier> copiers = ImmutableList.builder();

    for (final FieldVector target : outputs) {
      TypeProtos.MinorType minorType = CompleteType.fromField(target.getField()).toMinorType();
      if (minorType == TypeProtos.MinorType.VARCHAR || minorType == TypeProtos.MinorType.VARBINARY) {
        copiers.add(new VariableEmptySourceCopier(target));
      } else {
        copiers.add(new NoOpEmptySourceCopier(target));
      }
    }
    return copiers.build();
  }
}
