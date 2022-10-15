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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.ht2.Reallocators;
import com.dremio.sabot.op.common.ht2.Reallocators.Reallocator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 *  Utility class holding 2 byte vectorized copiers and a method to assign copiers between a pair of vectors
 */
public final class FieldBufferCopier2Util {
  private static final int STEP_SIZE = 2;

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;

  private FieldBufferCopier2Util() {};

  abstract static class FixedWidthCopier implements FieldBufferCopier {
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
      targetAlt.allocateNew(count);
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      resizeIfNeeded(targetAlt, targetIndex + count);
      seekAndCopy(offsetAddr, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      copy(offsetAddr, count, cursor);
    }

    public void allocate(int records){
      targetAlt.allocateNew(records);
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

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      long prevDataBufAddr = target.getDataBufferAddress();
      int dstIndex = seekTo;

      for (long addr = listOffsetBufAddr; addr < listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);
        dstIndex += endOffset - startOffset;
        if(resizeIfNeeded(targetAlt, dstIndex)) {
          dstAddr += target.getDataBufferAddress() - prevDataBufAddr;
          prevDataBufAddr = target.getDataBufferAddress();
        }

        assert target.getValueCapacity() >= dstIndex : "Target vector does not have enough capacity to copy records";
        PlatformDependent.copyMemory(srcAddr + startOffset * SIZE, dstAddr, (endOffset - startOffset) * SIZE);
        dstAddr += (endOffset - startOffset) * SIZE;
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

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      long prevDataBufAddr = target.getDataBufferAddress();
      int dstIndex = seekTo;

      for (long addr = listOffsetBufAddr; addr < listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);
        dstIndex += endOffset - startOffset;
        if(resizeIfNeeded(targetAlt, dstIndex)) {
          dstAddr += target.getDataBufferAddress() - prevDataBufAddr;
          prevDataBufAddr = target.getDataBufferAddress();
        }

        assert target.getValueCapacity() >= dstIndex : "Target vector does not have enough capacity to copy records";
        PlatformDependent.copyMemory(srcAddr + startOffset * SIZE, dstAddr, (endOffset - startOffset) * SIZE);
        dstAddr += (endOffset - startOffset) * SIZE;
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

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      final long srcAddr = source.getDataBufferAddress();
      long dstAddr = target.getDataBufferAddress() + (seekTo * SIZE);
      long prevDataBufAddr = target.getDataBufferAddress();
      int dstIndex = seekTo;

      for (long addr = listOffsetBufAddr; addr < listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);
        dstIndex += endOffset - startOffset;
        if(resizeIfNeeded(targetAlt, dstIndex)) {
          dstAddr += target.getDataBufferAddress() - prevDataBufAddr;
          prevDataBufAddr = target.getDataBufferAddress();
        }

        assert target.getValueCapacity() >= dstIndex : "Target vector does not have enough capacity to copy records";
        PlatformDependent.copyMemory(srcAddr + startOffset * SIZE, dstAddr, (endOffset - startOffset) * SIZE);
        dstAddr += (endOffset - startOffset) * SIZE;
      }
    }
  }

  static class VariableCopier implements FieldBufferCopier {
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

    private void seekAndCopy(long sv2, int count, int seekTo) {
      int targetIndex = seekTo;
      int targetDataIndex = 0;
      if (targetIndex != 0) {
        long dstOffsetAddr = target.getOffsetBufferAddress() + targetIndex * 4;
        targetDataIndex = PlatformDependent.getInt(dstOffsetAddr);
      }

      final Reallocator realloc = this.realloc;
      // make sure vectors are internally consistent
      VariableLengthValidator.validateVariable(source, source.getValueCount());

      final long maxSv2 = sv2 + count * STEP_SIZE;
      final long srcOffsetAddr = source.getOffsetBufferAddress();
      final long srcDataAddr = source.getDataBufferAddress();

      long dstOffsetAddr = target.getOffsetBufferAddress() + (targetIndex + 1) * 4;
      long curDataAddr = realloc.addr() + targetDataIndex; // start address for next copy in target
      long maxDataAddr = realloc.max(); // max bytes we can copy to target before we need to reallocate

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
          curDataAddr = realloc.ensure(targetDataIndex + len) + targetDataIndex;
          maxDataAddr = realloc.max();
        }

        targetDataIndex += len;
        PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddr + firstOffset, curDataAddr, len);
        curDataAddr += len;
      }

      realloc.setCount(targetIndex + count);
    }

    @Override
    public void copy(long sv2, int count) {
      targetAlt.allocateNew(AVG_VAR_WIDTH * count, count);
      seekAndCopy(sv2, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    @Override
    public void copy(long sv2, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      if (targetAlt.getValueCapacity() < targetIndex + count) {
        realloc.ensureValidityAndOffsets(cursor.getTargetIndex() + count);
      }
      seekAndCopy(sv2, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      copy(offsetAddr, count, cursor);
    }

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      if (source.getValueCount() == 0) {
        return;
      }

      final Reallocator realloc = this.realloc;
      // make sure vectors are internally consistent
      VariableLengthValidator.validateVariable(source, source.getValueCount());

      final long srcOffsetAddr = source.getOffsetBufferAddress();
      final long srcDataAddr = source.getDataBufferAddress();

      long dstOffsetAddr = target.getOffsetBufferAddress() + (seekTo + 1) * ListVector.OFFSET_WIDTH;
      long dstOffsetAddrPrev = target.getOffsetBufferAddress();
      int targetDataIndex = seekTo > 0 ? PlatformDependent.getInt(dstOffsetAddr - ListVector.OFFSET_WIDTH): 0;
      long curDataAddr = realloc.addr() + targetDataIndex; // start address for next copy in target
      long maxDataAddr = realloc.max(); // max bytes we can copy to target before we need to reallocate
      int offsetCount = 0;
      int recordsCopied = seekTo;

      for (int idx = 0; idx < count; ++idx) {
        final long listOffsets = PlatformDependent.getLong(listOffsetBufAddr + idx * Long.BYTES);
        final int listStartOffset = (int) listOffsets;
        final int listEndOffset = (int) (listOffsets >> 32);

        // Check enough capacity for offset buffer
        recordsCopied += listEndOffset - listStartOffset;
        while (targetAlt.getValueCapacity() <= recordsCopied) {
          targetAlt.reAlloc();
          dstOffsetAddr += target.getOffsetBufferAddress() - dstOffsetAddrPrev;
          dstOffsetAddrPrev = target.getOffsetBufferAddress();
          curDataAddr = realloc.addr() + targetDataIndex;
          maxDataAddr = realloc.max();
        }
        assert target.getValueCapacity() > recordsCopied : "Target vector does not have enough capacity to copy records";

        int lastOffset = PlatformDependent.getInt(srcOffsetAddr + listStartOffset * 4);
        for (int listOffset = listStartOffset + 1; listOffset <= listEndOffset; ++listOffset) {
          int currOffset = PlatformDependent.getInt(srcOffsetAddr + listOffset * 4);
          int len = currOffset - lastOffset;
          // check if we need to reallocate target data buffer
          if (curDataAddr + len > maxDataAddr) {
            curDataAddr = realloc.ensure(targetDataIndex + len) + targetDataIndex;
            maxDataAddr = realloc.max();
          }
          assert maxDataAddr >= (curDataAddr + len) : "Target vector does not have enough capacity to copy records";
          targetDataIndex += len;
          PlatformDependent.putInt(dstOffsetAddr, targetDataIndex);
          com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddr + lastOffset, curDataAddr, len);
          curDataAddr += len;
          dstOffsetAddr += 4;
          offsetCount++;

          lastOffset = currOffset;
        }
      }
      realloc.setCount(seekTo + offsetCount);
    }

    public void allocate(int records){
      targetAlt.allocateNew(records * AVG_VAR_WIDTH, records);
    }
  }

  static class BitCopier implements FieldBufferCopier {

    private final FieldVector source;
    private final FieldVector target;
    private final FixedWidthVector targetAlt;
    private final int bufferOrdinal;
    private final boolean allocateAsFixed;
    private final boolean isTargetVectorZeroedOut;

    public BitCopier(FieldVector source, FieldVector target, int bufferOrdinal, boolean allocateAsFixed, boolean isTargetVectorZeroedOut){
      this.source = source;
      this.target = target;
      this.targetAlt = allocateAsFixed ? (FixedWidthVector) target : null;
      this.allocateAsFixed = allocateAsFixed;
      this.bufferOrdinal = bufferOrdinal;
      this.isTargetVectorZeroedOut = isTargetVectorZeroedOut;
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
      if (isTargetVectorZeroedOut) {
        for (; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++) {
          final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
          final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
          final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
          final long addr = dstAddr + (targetIndex >>> 3);
          PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
        }
      }
      else {
        for (; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++) {
          final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
          final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
          final int bitVal1 = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
          final int bitVal2 = ~(bitVal1 ^ (1 << (targetIndex & 7)));
          final long addr = dstAddr + (targetIndex >>> 3);
          PlatformDependent.putByte(addr, (byte) ((PlatformDependent.getByte(addr) | bitVal1) & bitVal2));
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
      int targetIndex = cursor.getTargetIndex();
      if (allocateAsFixed) {
        while (targetAlt.getValueCapacity() < targetIndex + count) {
          targetAlt.reAlloc();
        }
      }
      seekAndCopy(offsetAddr, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    public void seekAndCopy(long offsetAddr, int count, long nullAddr, int nullCount, int seekTo) {
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
      if (isTargetVectorZeroedOut) {
        for (; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++) {
          final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
          final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
          final int bitVal = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
          final long addr = dstAddr + (targetIndex >>> 3);
          PlatformDependent.putByte(addr, (byte) (PlatformDependent.getByte(addr) | bitVal));
        }
      }
      else {
        for (; offsetAddr < maxAddr; offsetAddr += STEP_SIZE, targetIndex++) {
          final int recordIndex = Short.toUnsignedInt(PlatformDependent.getShort(offsetAddr));
          final int byteValue = PlatformDependent.getByte(srcAddr + (recordIndex >>> 3));
          final int bitVal1 = ((byteValue >>> (recordIndex & 7)) & 1) << (targetIndex & 7);
          final int bitVal2 = ~(bitVal1 ^ (1 << (targetIndex & 7)));
          final long addr = dstAddr + (targetIndex >>> 3);
          PlatformDependent.putByte(addr, (byte) ((PlatformDependent.getByte(addr) | bitVal1) & bitVal2));
        }
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

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      if (allocateAsFixed) {
        targetAlt.allocateNew(count);
      }
      seekAndCopy(offsetAddr, count, nullAddr, nullCount, 0);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      if (allocateAsFixed) {
        while (targetAlt.getValueCapacity() < targetIndex + count) {
          targetAlt.reAlloc();
        }
      }
      seekAndCopy(offsetAddr, count, nullAddr, nullCount, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      long srcAddr;
      long dstAddr;
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
          throw new UnsupportedOperationException("unexpected buffer ordinal");
      }

      int recordsCopied = seekTo;
      int targetIdx = seekTo;
      for (long addr = listOffsetBufAddr; addr < listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);

        recordsCopied += endOffset - startOffset;
        while (target.getValueCapacity() < recordsCopied) {
          target.reAlloc();
          switch (bufferOrdinal) {
            case NULL_BUFFER_ORDINAL:
              dstAddr = target.getValidityBufferAddress();
              break;
            case VALUE_BUFFER_ORDINAL:
              dstAddr = target.getDataBufferAddress();
              break;
          }
        }
        assert target.getValueCapacity() >= recordsCopied : "Target vector does not have enough capacity to copy records";
        // Optimize copy when startOffset and targetIdx are aligned
        // and when the bytes containing bits for startOffset and endOffset have atleast one byte between them
        if ((startOffset % 8 == targetIdx % 8) && (((endOffset >>> 3) - (startOffset >>> 3) - 1) > 0)) {

          // Do bit by bit copy from the bit for srcOffset to the end of the byte containing the bit for srcOffset
          int byteValue = PlatformDependent.getByte(srcAddr + (startOffset >>> 3));
          for (int offset = startOffset; offset < ((startOffset / 8 + 1) * 8); ++offset, ++targetIdx) {
            final int bitValue = ((byteValue >>> (offset & 7)) & 1) << (targetIdx & 7);
            final long finalDstAddr = dstAddr + (targetIdx >>> 3);
            PlatformDependent.putByte(finalDstAddr, (byte) (PlatformDependent.getByte(finalDstAddr) | bitValue));
          }

          // Do direct byte-wise memory copy for the bytes between the byte containing bit for startOffset and the byte containing bit for endOffset
          PlatformDependent.copyMemory(srcAddr + (startOffset >>> 3) + 1, dstAddr + (targetIdx >>> 3), ((endOffset >>> 3) - (startOffset >>> 3) - 1));
          targetIdx += ((endOffset >>> 3) - (startOffset >>> 3) - 1) * 8;

          // Do bit by bit copy from the start of byte containing the bit for endOffset to the bit for endOffset
          byteValue = PlatformDependent.getByte(srcAddr + (endOffset >>> 3));
          for (int offset = (endOffset / 8 * 8); offset < endOffset; ++offset, ++targetIdx) {
            final int bitValue = ((byteValue >>> (offset & 7)) & 1) << (targetIdx & 7);
            final long finalDstAddr = dstAddr + (targetIdx >>> 3);
            PlatformDependent.putByte(finalDstAddr, (byte) (PlatformDependent.getByte(finalDstAddr) | bitValue));
          }

        } else {
          // Do a bit by bit copy
          for (int offset = startOffset; offset < endOffset; ++offset, ++targetIdx) {
            final int byteValue = PlatformDependent.getByte(srcAddr + (offset >>> 3));
            final int bitValue = ((byteValue >>> (offset & 7)) & 1) << (targetIdx & 7);
            final long finalDstAddr = dstAddr + (targetIdx >>> 3);
            PlatformDependent.putByte(finalDstAddr, (byte) (PlatformDependent.getByte(finalDstAddr) | bitValue));
          }
        }
      }
    }

    public void allocate(int records){
      if(targetAlt != null){
        targetAlt.allocateNew(records);
      }
    }

  }

  static class StructCopier implements FieldBufferCopier {
    private final FieldVector source;
    private final FieldVector target;
    private final List<FieldBufferCopier> childCopiers;

    public StructCopier(FieldVector source, FieldVector target, OptionManager optionManager, boolean isTargetVectorZeroedOut) {
      this.source = source;
      this.target = target;
      childCopiers = new FieldBufferCopierFactory(optionManager).getTwoByteCopiers(source.getChildrenFromFields(), target.getChildrenFromFields(), isTargetVectorZeroedOut);
    }

    private void seekAndCopy(long offsetAddr, int count, Cursor cursor) {
      for (FieldBufferCopier childCopier: childCopiers) {
        childCopier.copy(offsetAddr, count, new Cursor(cursor));
      }
    }

    @Override
    public void allocate(int records) {
      target.setInitialCapacity(records);
      target.allocateNew();
    }

    @Override
    public void copy(long offsetAddr, int count) {
      allocate(count);
      childCopiers.forEach(c -> c.copy(offsetAddr, count));
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      copy(offsetAddr, count, cursor);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      while (target.getValueCapacity() < cursor.getTargetIndex() + count) {
        target.reAlloc();
      }
      seekAndCopy(offsetAddr, count, cursor);
      cursor.setTargetIndex(cursor.getTargetIndex() + count);
    }

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      long dstIndex = seekTo;
      final long max = listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH;

      /* This is to ensure that the validity buf capacity of the struct is properly adjusted */
      for (long addr = listOffsetBufAddr; addr < max; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);
        dstIndex += endOffset - startOffset;
      }
      while (target.getValueCapacity() < dstIndex) {
        target.reAlloc();
      }
      assert target.getValueCapacity() >= dstIndex : "Target vector does not have enough capacity to copy records";
      childCopiers.forEach(c -> c.copyInnerList(listOffsetBufAddr, count, seekTo));
    }
  }

  static class ListCopier implements FieldBufferCopier {
    private final FieldVector source;
    private final FieldVector target;
    private final List<FieldBufferCopier> childCopiers;

    public ListCopier(FieldVector source, FieldVector target, OptionManager optionManager, boolean isTargetVectorZeroedOut) {
      this.source = source;
      this.target = target;
      this.childCopiers = new FieldBufferCopierFactory(optionManager).getTwoByteCopiers(source.getChildrenFromFields(), target.getChildrenFromFields(), isTargetVectorZeroedOut);
    }

    @Override
    public void allocate(int records) {
      target.setInitialCapacity(records);
      target.allocateNew();
    }

    private void seekAndCopy(long offsetAddr, int count, int seekTo) {
      if (count == 0) {
        return;
      }

      /* addr of offset buffer of source list vector */
      long srcOffsetAddr = source.getOffsetBufferAddress();
      /* adrr of offset buffer of target list vector starting corresponding to the seekTo position */
      long targetOffsetAddr = target.getOffsetBufferAddress() + (seekTo + 1) * ListVector.OFFSET_WIDTH;

      /* seek position of child data vector */
      int childSeekto = seekTo > 0 ? PlatformDependent.getInt(targetOffsetAddr - ListVector.OFFSET_WIDTH) : 0;
      // This is Zero when it's a first time copy call
      // If not, then initialize this value to last offset set by previous call, thus making subsequent offset values consistent
      int targetOffsetIdx = PlatformDependent.getInt(targetOffsetAddr - ListVector.OFFSET_WIDTH);
      try (ArrowBuf tmpBuf = target.getAllocator().buffer(count * 2 * ListVector.OFFSET_WIDTH)) {
        int tmpBufIdx = 0;
        int oldsv = -2;
        int offsetStart, offsetEnd = -1; /* offsetStart and offsetEnd are list offsets corresponding to the current SV index */
        for (long addr = offsetAddr; addr < offsetAddr + count * STEP_SIZE; addr += STEP_SIZE, targetOffsetAddr += ListVector.OFFSET_WIDTH) {
          int sv = Short.toUnsignedInt(PlatformDependent.getShort(addr));
          /* reuse prev offsetEnd if possible to save one mem access */
          offsetStart = (sv == oldsv + 1) ? offsetEnd : PlatformDependent.getInt(srcOffsetAddr + sv * ListVector.OFFSET_WIDTH);
          offsetEnd = PlatformDependent.getInt(srcOffsetAddr + (sv + 1) * ListVector.OFFSET_WIDTH);
          /* fill up the tmp offset buffer */
          /* tmpBuf contains the start and end offsets into the inner data vector corresponding to the sv */
          /* setting this way allows us to copy nested list vectors */
          tmpBuf.setInt(tmpBufIdx * 4L, offsetStart);
          tmpBuf.setInt((tmpBufIdx + 1) * 4L, offsetEnd);
          tmpBufIdx += 2;

          /* set the list offsets in the target vector*/
          targetOffsetIdx += offsetEnd - offsetStart;
          PlatformDependent.putInt(targetOffsetAddr, targetOffsetIdx);
          oldsv = sv;
        }

        childCopiers.forEach(c -> c.copyInnerList(tmpBuf.memoryAddress(), count, childSeekto));

        ((ListVector) target).setLastSet(seekTo + count - 1);
      }
    }

    @Override
    public void copy(long offsetAddr, int count) {
      allocate(count);
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullAddr, int nullCount) {
      copy(offsetAddr, count);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      while (target.getValueCapacity() < targetIndex + count) {
        target.reAlloc();
      }
      seekAndCopy(offsetAddr, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      copy(offsetAddr, count, cursor);
    }

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      /* addr of source offset buffer */
      long srcOffsetAddr = source.getOffsetBufferAddress();
      /* addr of target offset buffer corresponding to the seek position */
      long targetOffsetAddr = target.getOffsetBufferAddress() + (seekTo + 1) * ListVector.OFFSET_WIDTH;
      long targetOffsetBufAddrPrev = target.getOffsetBufferAddress();

      /* seek position of the inner data vector */
      int childSeekTo = seekTo > 0 ? PlatformDependent.getInt(targetOffsetAddr - ListVector.OFFSET_WIDTH) : 0;

      try (ArrowBuf tmpBuf = target.getAllocator().buffer(2 * count * ListVector.OFFSET_WIDTH)) {
        /* addr of temp list offset buffer for the child copyInnerList call */
        long childListOffsetBufAddr = tmpBuf.memoryAddress();

        /* offest into the inner data vector already set */
        int offsetTilNow = childSeekTo;
        int offsetCount = seekTo;
        int recordsCopied = seekTo;

        for (int idx = 0; idx < count; ++idx) {
          /* startOffset and endOffset are offsets into the offset buffer of the current vector. This is derived
          * from the parent tmp offset buffer and basically marks the portion of the list to be copied based on the
          * actual selection vector */
          long startAndEndOffset = PlatformDependent.getLong(listOffsetBufAddr + (2 * idx) * ListVector.OFFSET_WIDTH);
          int startOffset = (int) (startAndEndOffset);
          int endOffset = (int) (startAndEndOffset >> 32);

          recordsCopied += endOffset - startOffset;
          while (target.getValueCapacity() <= recordsCopied) {
            target.reAlloc();
            targetOffsetAddr += target.getOffsetBufferAddress() - targetOffsetBufAddrPrev;
            targetOffsetBufAddrPrev = target.getOffsetBufferAddress();
          }
          assert target.getValueCapacity() > recordsCopied : "Target vector does not have enough capacity to copy records";

          int innerStartOffset = PlatformDependent.getInt(srcOffsetAddr + startOffset * ListVector.OFFSET_WIDTH);
          int innerEndOffset = innerStartOffset;
          int lastOffset = innerStartOffset;
          for (int offset = startOffset + 1; offset <= endOffset; ++ offset, targetOffsetAddr += ListVector.OFFSET_WIDTH, offsetCount++) {
            innerEndOffset = PlatformDependent.getInt(srcOffsetAddr + offset * ListVector.OFFSET_WIDTH);
            offsetTilNow += innerEndOffset - lastOffset;
            PlatformDependent.putInt(targetOffsetAddr, offsetTilNow);
            lastOffset = innerEndOffset;
          }

          PlatformDependent.putInt(childListOffsetBufAddr + (2 * idx) * ListVector.OFFSET_WIDTH, innerStartOffset);
          PlatformDependent.putInt(childListOffsetBufAddr + (2 * idx + 1) * ListVector.OFFSET_WIDTH, innerEndOffset);
        }

        childCopiers.forEach(c -> c.copyInnerList(tmpBuf.memoryAddress(), count, childSeekTo));

        ((ListVector) target).setLastSet(offsetCount - 1);
      }
    }
  }

  static class GenericCopier implements FieldBufferCopier {
    private final TransferPair transfer;
    private final FieldVector dst;
    private final FieldVector src;
    private final OptionManager optionManager;
    private final boolean isTargetVectorZeroedOut;

    public GenericCopier(FieldVector source, FieldVector dst, OptionManager optionManager, boolean isTargetVectorZeroedOut){
      this.transfer = source.makeTransferPair(dst);
      this.dst = dst;
      this.src = source;
      this.optionManager = optionManager;
      this.isTargetVectorZeroedOut = isTargetVectorZeroedOut;
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
      dst.allocateNew();
      seekAndCopy(offsetAddr, count, 0);
    }

    @Override
    public void copy(long offsetAddr, int count, long nullKeyAddr, int nullKeyCount) {
      copy(offsetAddr, count);
    }

    @Override
    public void copy(long offsetAddr, int count, Cursor cursor) {
      int targetIndex = cursor.getTargetIndex();
      if (targetIndex == 0 && isTargetVectorZeroedOut) {
        dst.allocateNew();
      }
      seekAndCopy(offsetAddr, count, targetIndex);
      cursor.setTargetIndex(targetIndex + count);
    }

    public void copy(long offsetAddr, int count, long nullAddr, int nullCount, Cursor cursor) {
      copy(offsetAddr, count, cursor);
    }

    @Override
    public void copyInnerList(long listOffsetBufAddr, int count, int seekTo) {
      // This method gets called only for Unions when the support key ENABLE_VECTORIZED_COMPLEX_COPIER is turned ON
      // This is because copyInnerList() can only be initiated from a copy() call of an ancestor ListCopier
      // ListVector will make use of ListCopier only when support key is ON.
      // If support key is turned OFF, ListVector, StructVector & UnionVector will make use of GenericCopier's seekAndCopy()

      // Get copiers associated with the child arrays of this union
      final List<FieldBufferCopier> childCopiers = new FieldBufferCopierFactory(optionManager).getTwoByteCopiers(src.getChildrenFromFields(), dst.getChildrenFromFields());

      // UNION does not have validity buffer
      long srcTypeBufAddr = ((UnionVector) src).getTypeBufferAddress();
      long dstTypeBufAddr = ((UnionVector) dst).getTypeBufferAddress() + (seekTo * UnionVector.TYPE_WIDTH);
      long dstTypeBufAddrPrev = ((UnionVector) dst).getTypeBufferAddress();
      long dstIndex = seekTo;
      for (long addr = listOffsetBufAddr; addr < listOffsetBufAddr + 2 * count * ListVector.OFFSET_WIDTH; addr += 2 * ListVector.OFFSET_WIDTH) {
        long startAndEndOffset = PlatformDependent.getLong(addr);
        int startOffset = (int) startAndEndOffset;
        int endOffset = (int) (startAndEndOffset >> 32);
        dstIndex += endOffset - startOffset;
        // Ensure target type buffer has enough size
        while (dst.getValueCapacity() < dstIndex) {
          dst.reAlloc();
          dstTypeBufAddr += ((UnionVector) dst).getTypeBufferAddress() - dstTypeBufAddrPrev;
          dstTypeBufAddrPrev = ((UnionVector) dst).getTypeBufferAddress();
        }
        assert dst.getValueCapacity() >= dstIndex : "Target vector does not have enough capacity to copy records";
        // Copy type buffer
        PlatformDependent.copyMemory(srcTypeBufAddr + startOffset * UnionVector.TYPE_WIDTH, dstTypeBufAddr, (endOffset - startOffset) * UnionVector.TYPE_WIDTH);
        dstTypeBufAddr += (endOffset - startOffset) * UnionVector.TYPE_WIDTH;
      }

      childCopiers.forEach(c -> c.copyInnerList(listOffsetBufAddr, count, seekTo));
    }

    public void allocate(int records){
      AllocationHelper.allocate(dst, records, 10);
    }
  }

  public static void addValueCopier(FieldVector source, FieldVector target, ImmutableList.Builder<FieldBufferCopier> copiers, OptionManager optionManager, boolean isTargetVectorZeroedOut) {
    Preconditions.checkArgument(source.getClass() == target.getClass(), "Input and output vectors must be same type.");
    switch (CompleteType.fromField(source.getField()).toMinorType()) {

      case TIMESTAMP:
      case FLOAT8:
      case BIGINT:
      case INTERVALDAY:
      case DATE:
        copiers.add(new FieldBufferCopier2Util.EightByteCopier(source, target));
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        break;

      case BIT:
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, true, isTargetVectorZeroedOut));
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, VALUE_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        break;

      case TIME:
      case FLOAT4:
      case INT:
      case INTERVALYEAR:
        copiers.add(new FieldBufferCopier2Util.FourByteCopier(source, target));
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        break;

      case VARBINARY:
      case VARCHAR:
        copiers.add(new FieldBufferCopier2Util.VariableCopier(source, target));
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        break;

      case DECIMAL:
        copiers.add(new FieldBufferCopier2Util.SixteenByteCopier(source, target));
        copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        break;

      case STRUCT:
        if (optionManager.getOption(ExecConstants.ENABLE_VECTORIZED_COMPLEX_COPIER)) {
          copiers.add(new FieldBufferCopier2Util.StructCopier(source, target, optionManager, isTargetVectorZeroedOut));
          copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        } else {
          copiers.add(new FieldBufferCopier2Util.GenericCopier(source, target, optionManager, isTargetVectorZeroedOut));
        }
        break;

      case MAP:
      case LIST:
        if (optionManager.getOption(ExecConstants.ENABLE_VECTORIZED_COMPLEX_COPIER)) {
          copiers.add(new FieldBufferCopier2Util.ListCopier(source, target, optionManager, isTargetVectorZeroedOut));
          copiers.add(new FieldBufferCopier2Util.BitCopier(source, target, NULL_BUFFER_ORDINAL, false, isTargetVectorZeroedOut));
        } else {
          copiers.add(new FieldBufferCopier2Util.GenericCopier(source, target, optionManager, isTargetVectorZeroedOut));
        }
        break;

      case UNION:
        copiers.add(new FieldBufferCopier2Util.GenericCopier(source, target, optionManager, isTargetVectorZeroedOut));
        break;

      case NULL:
        // No copiers are added for null type as there are no buffers to copy.
        break;

      default:
        throw new UnsupportedOperationException("Unknown type to copy. Could not assign a copier for " + CompleteType.fromField(source.getField()).toMinorType());
    }
  }
}
