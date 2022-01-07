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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;


public abstract class FieldBufferPreAllocedCopier {

  private static final int STEP_SIZE = 2;

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;

  // Cursor into the target vector.
  public static class Cursor {
    // index in the target vector.
    private int targetIndex;

    // index in the target data vector (used in variable width copiers).
    private int targetDataIndex;
  }

  public abstract void copy(long offsetAddr, int count);

  // Copy data and set validity to 0 for all records in nullAddr
  // nullAddr is all the offsets of the records that are null
  // The action to set validity is only needed for BitCopier in FieldBufferCopier,
  // because validity data is copied in BitCopier
  public abstract void copy(long offsetAddr, int count, long nullAddr, int nullCount);

  /**
   * Copy starting from the previous cursor.
   * @param offsetAddr offset addr of the selection vector
   * @param count  number of entries to copy
   * @param cursor cursor returned in the previous call (set to null on first call).
   * @return cursor after the copy.
   */
  public Cursor copy(long offsetAddr, int count, Cursor cursor) {
    throw new UnsupportedOperationException("copy with cursor not supported");
  }

  // Ensure that the vector is sized upto capacity 'size'.
  protected void ensure(FixedWidthVector vector, int size) {
    if (vector.getValueCapacity() < size) {
      throw new IllegalArgumentException("vector size required " + size + ", but found " + vector.getValueCapacity());
    }
  }

  static abstract class FixedWidthCopier extends FieldBufferPreAllocedCopier {
    protected final FieldVector source;
    protected final FieldVector target;
    protected final FixedWidthVector targetAlt;

    public FixedWidthCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.target = target;
      this.targetAlt = (FixedWidthVector) target;
    }

    abstract void seekAndCopy(long offsetAddr, int count, int seekTo);

    @Override
    public void copy(long offsetAddr, int count) {
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    @Override
    public Cursor copy(long offsetAddr, int count, Cursor cursor) {
      if (cursor == null) {
        cursor = new Cursor();
      }
      ensure(targetAlt, cursor.targetIndex + count);
      seekAndCopy(offsetAddr, count, cursor.targetIndex);
      cursor.targetIndex += count;
      return cursor;
    }
  }

  static final class FourByteCopier extends FixedWidthCopier {
    private static final int SIZE = 4;

    public FourByteCopier(FieldVector source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * STEP_SIZE;
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for (long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE) {
        PlatformDependent.putInt(
          dstAddr,
          PlatformDependent.getInt(
            srcAddr + Short.toUnsignedInt(PlatformDependent.getShort(addr)) * SIZE));
      }
    }
  }

  static final class EightByteCopier extends FixedWidthCopier {
    private static final int SIZE = 8;

    public EightByteCopier(FieldVector source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * STEP_SIZE;
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for (long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE) {
        PlatformDependent.putLong(
          dstAddr,
          PlatformDependent.getLong(
            srcAddr + Short.toUnsignedInt(PlatformDependent.getShort(addr)) * SIZE));
      }
    }
  }

  static final class SixteenByteCopier extends FixedWidthCopier {
    private static final int SIZE = 16;

    public SixteenByteCopier(FieldVector source, FieldVector target) {
      super(source, target);
    }

    @Override
    void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * STEP_SIZE;
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      for (long addr = offsetAddr; addr < max; addr += STEP_SIZE, dstAddr += SIZE) {
        final int offset = Short.toUnsignedInt(PlatformDependent.getShort(addr)) * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr + offset));
        PlatformDependent.putLong(dstAddr+8, PlatformDependent.getLong(srcAddr + offset + 8));
      }
    }
  }

  static class VariableCopier extends FieldBufferPreAllocedCopier {
    private final FieldVector source;
    private final FieldVector target;
    private final VariableWidthVector targetAlt;

    public VariableCopier(FieldVector source, FieldVector target) {
      this.source = source;
      this.targetAlt = (VariableWidthVector) target;
      this.target = target;
    }

    private Cursor seekAndCopy(long sv2, int count, Cursor cursor) {
      int targetIndex;
      int targetDataIndex;
      if (cursor == null) {
        targetIndex =  targetDataIndex = 0;
      } else {
        targetIndex = cursor.targetIndex;
        targetDataIndex = cursor.targetDataIndex;
      }

      // make sure vectors are internally consistent
      VariableLengthValidator.validateVariable(source, source.getValueCount());

      final long maxSv2 = sv2 + count * STEP_SIZE;
      final long srcOffsetAddr = source.getOffsetBufferAddress();
      final long srcDataAddr = source.getDataBufferAddress();

      long dstOffsetAddr = target.getOffsetBufferAddress() + (targetIndex + 1) * 4;
      long curDataAddr = target.getDataBufferAddress() + targetDataIndex; // start address for next copy in target
      long maxDataAddr = target.getDataBufferAddress() + target.getDataBuffer().capacity(); // max bytes we can copy to target before we need to reallocate

      for(; sv2 < maxSv2; sv2 += STEP_SIZE, dstOffsetAddr += 4){
        // copy from recordIndex to last available position in target
        final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(sv2));
        // retrieve start offset and length of value we want to copy
        final long startAndEnd = PlatformDependent.getLong(srcOffsetAddr + recordIndex * 4);
        final int firstOffset = (int) startAndEnd;
        final int secondOffset = (int) (startAndEnd >> 32);
        final int len = secondOffset - firstOffset;
        // check if we need to reallocate target buffer
        if (curDataAddr + len > maxDataAddr) {
          throw new IllegalArgumentException("data size required " + (curDataAddr + len - target.getDataBufferAddress()) +
            ", but found " + target.getDataBuffer().capacity());
        }

        targetDataIndex += len;
        PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddr + firstOffset, curDataAddr, len);
        curDataAddr += len;
      }

      target.setValueCount(targetIndex + count);
      if (cursor != null) {
        cursor.targetIndex += count;
        cursor.targetDataIndex = targetDataIndex;
      }
      return cursor;
    }

    @Override
    public void copy(long sv2, int count) {
      seekAndCopy(sv2, count, null);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    @Override
    public Cursor copy(long sv2, int count, Cursor cursor) {
      if (cursor == null) {
        cursor = new Cursor();
      }
      if (targetAlt.getValueCapacity() < cursor.targetIndex + count) {
        throw new IllegalArgumentException("vector size required " + cursor.targetDataIndex + count +
            ", but found " + targetAlt.getValueCapacity());
      }
      return seekAndCopy(sv2, count, cursor);
    }
  }

  static class BitCopier extends FieldBufferPreAllocedCopier {

    private final FieldVector source;
    private final FieldVector target;
    private final int bufferOrdinal;

    public BitCopier(FieldVector source, FieldVector target, int bufferOrdinal) {
      this.source = source;
      this.target = target;
      this.bufferOrdinal = bufferOrdinal;
    }

    private void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long srcAddr;
      final long dstAddr;
      switch (bufferOrdinal) {
        case NULL_BUFFER_ORDINAL:
          srcAddr = source.getValidityBufferAddress();
          dstAddr = target.getValidityBufferAddress();
          break;
        case VALUE_BUFFER_ORDINAL:
          srcAddr = source.getDataBufferAddress();
          dstAddr = target.getDataBufferAddress();
          break;
        default:
          throw new UnsupportedOperationException("unexpected buffer offset");
      }

      final long maxAddr = offsetAddr + count * STEP_SIZE;
      int targetIndex = seekTo;
      for(; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++){
        final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
        final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
        final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
        final long addr = dstAddr + (targetIndex >>> 3);
        PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
      }
    }

    @Override
    public void copy(long offsetAddr, int count) {
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public Cursor copy(long offsetAddr, int count, Cursor cursor) {
      if (cursor == null) {
        cursor = new Cursor();
      }
      if (target.getValueCapacity() < cursor.targetIndex + count) {
        throw new IllegalArgumentException("vector size required " + (cursor.targetIndex + count) +
          ", but found " + target.getValueCapacity());
      }
      seekAndCopy(offsetAddr, count, cursor.targetIndex);
      cursor.targetIndex += count;
      return cursor;
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      final long srcAddr;
      final long dstAddr;
      switch (bufferOrdinal) {
        case NULL_BUFFER_ORDINAL:
          srcAddr = source.getValidityBufferAddress();
          dstAddr = target.getValidityBufferAddress();
          break;
        case VALUE_BUFFER_ORDINAL:
          srcAddr = source.getDataBufferAddress();
          dstAddr = target.getDataBufferAddress();
          break;
        default:
          throw new UnsupportedOperationException("unexpected buffer offset");
      }

      final long maxAddr = offsetAddr + count * STEP_SIZE;
      int targetIndex = 0;
      for(; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++){
        final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
        final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
        final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
        final long addr = dstAddr + (targetIndex >>> 3);
        PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
      }

      // Set the validity to 0 for all records in nullAddr after copy validity data
      if (bufferOrdinal == NULL_BUFFER_ORDINAL) {
        final long maxKeyAddr = nullAddr + nullCount * STEP_SIZE;
        for (; nullAddr < maxKeyAddr; nullAddr += STEP_SIZE) {
          targetIndex = Short.toUnsignedInt(PlatformDependent.getShort(nullAddr));
          final long addr = dstAddr + (targetIndex >>> 3);
          final int bitVal = ~(1 << (targetIndex & 7));
          PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) & bitVal));
        }
      }
    }
  }

  static class GenericCopier extends FieldBufferPreAllocedCopier {
    private final TransferPair transfer;
    private final FieldVector dst;

    public GenericCopier(FieldVector source, FieldVector dst){
      this.transfer = source.makeTransferPair(dst);
      this.dst = dst;
    }

    private void seekAndCopy(long offsetAddr, int count, int seekTo) {
      final long max = offsetAddr + count * STEP_SIZE;
      int targetDataIndex = seekTo;
      for (long addr = offsetAddr; addr < max; addr += STEP_SIZE) {
        int index = Short.toUnsignedInt(PlatformDependent.getShort(addr));
        transfer.copyValueSafe(index, targetDataIndex);
        ++targetDataIndex;
      }
    }

    @Override
    public void copy(long offsetAddr, int count) {
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullKeyAddr, int nullKeyCount) {
      copy(offsetAddr, count);
    }

    @Override
    public Cursor copy(long offsetAddr, int count, Cursor cursor) {
      if (cursor == null) {
        cursor = new Cursor();
      }
      seekAndCopy(offsetAddr, count, cursor.targetIndex);
      cursor.targetIndex += count;
      return cursor;
    }
  }

  private static void addValueCopier(final FieldVector source, final FieldVector target, ImmutableList.Builder<FieldBufferPreAllocedCopier> copiers) {
    Preconditions.checkArgument(source.getClass() == target.getClass(), "Input and output vectors must be same type.");
    switch(CompleteType.fromField(source.getField()).toMinorType()){

      case TIMESTAMP:
      case FLOAT8:
      case BIGINT:
      case INTERVALDAY:
      case DATE:
        copiers.add(new EightByteCopier(source, target));
        copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL));
        break;

      case BIT:
        copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL));
        copiers.add(new BitCopier(source, target, VALUE_BUFFER_ORDINAL));
        break;

      case TIME:
      case FLOAT4:
      case INT:
      case INTERVALYEAR:
        copiers.add(new FourByteCopier(source, target));
        copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL));
        break;

      case VARBINARY:
      case VARCHAR:
        copiers.add(new VariableCopier(source, target));
        copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL));
        break;

      case DECIMAL:
        copiers.add(new SixteenByteCopier(source, target));
        copiers.add(new BitCopier(source, target, NULL_BUFFER_ORDINAL));
        break;

      case LIST:
      case STRUCT:
      case UNION:
        copiers.add(new GenericCopier(source, target));
        break;

      default:
        throw new UnsupportedOperationException("Unknown type to copy.");
    }
  }

  public static ImmutableList<FieldBufferPreAllocedCopier> getCopiers(List<FieldVector> inputs, List<FieldVector> outputs){
    ImmutableList.Builder<FieldBufferPreAllocedCopier> copiers = ImmutableList.builder();

    Preconditions.checkArgument(inputs.size() == outputs.size(), "Input and output lists must be same size.");
    for(int i = 0; i < inputs.size(); i++){
      final FieldVector input = inputs.get(i);
      final FieldVector output = outputs.get(i);
      addValueCopier(input, output, copiers);
    }
    return copiers.build();
  }
}
