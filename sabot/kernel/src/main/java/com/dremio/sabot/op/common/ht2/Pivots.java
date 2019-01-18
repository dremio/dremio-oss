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
package com.dremio.sabot.op.common.ht2;

import java.util.List;

import org.apache.arrow.vector.FieldVector;

import com.dremio.common.expression.Describer;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * TODO: Currently pivots created here process the entire input batch. In this process they may expand the output buffers.
 * We are adding version of pivot function that process only a subset of records that can fit in existing output buffers.
 * We may end up deleting this once HashAgg or HashJoin spilling is complete.
 */
public class Pivots {

  public static int FOUR_BYTE = 4;
  public static int EIGHT_BYTE = 8;
  public static int SIXTEEN_BYTE = 16;
  private static final int WORD_BITS = 64;
  private static final int WORD_BYTES = 8;
  private static final long ALL_SET = 0xFFFFFFFFFFFFFFFFL;
  private static final long NONE_SET = 0;

  private static void pivotVariableLengths(
      final List<VectorPivotDef> fields,
      final FixedBlockVector targetFixed,
      final VariableBlockVector targetVariable,
      final int count) {
    final int dataWidth = targetFixed.getBlockWidth() - LBlockHashTable.VAR_OFFSET_SIZE;
    final int fieldCount = fields.size();
    final long[] bitAddresses = new long[fieldCount];
    final long[] offsetAddresses = new long[fieldCount];
    final long[] dataAddresses = new long[fieldCount];
    final int[] nullByteOffset = new int[fieldCount];
    final int[] nullBitOffset = new int[fieldCount];
    long targetFixedAddress = targetFixed.getMemoryAddress();

    int i = 0;
    long totalData = 0;
    for(VectorPivotDef vpd : fields){

      nullByteOffset[i] = vpd.getNullByteOffset();
      nullBitOffset[i] = vpd.getNullBitOffset();

      List<ArrowBuf> buffers = vpd.getIncomingVector().getFieldBuffers();
      Preconditions.checkArgument(buffers.size() == 3, "A variable length vector should have three field buffers. %s has %s buffers.", Describer.describe(vpd.getIncomingVector().getField()), buffers.size());

      // convert to bit offsets. Overflows shouldn't exist since no system (yet) has memory of Long.MAX_VALUE / 8
      bitAddresses[i] = buffers.get(0).memoryAddress() * 8;

      offsetAddresses[i] = buffers.get(1).memoryAddress();

      ArrowBuf dataBuf = buffers.get(2);
      totalData += dataBuf.writerIndex();
      dataAddresses[i] = dataBuf.memoryAddress();
      i++;
    }

    Preconditions.checkArgument(totalData < Integer.MAX_VALUE);
    targetVariable.ensureAvailableDataSpace(((int) totalData) + 4 * fields.size() * count + (LBlockHashTable.VAR_LENGTH_SIZE * count));

    final int blockWidth = targetFixed.getBlockWidth();


    long varOffsetAddress = targetFixedAddress + dataWidth;
    long startingVariableAddr = targetVariable.getMemoryAddress();
    long targetVariableAddr = targetVariable.getMemoryAddress();

    for(int record = 0; record < count; record++){

      int varLen = 0;

      // write the starting position of the variable width data.
      PlatformDependent.putInt(varOffsetAddress, (int) (targetVariableAddr - startingVariableAddr));
      final long varLenAddress = targetVariableAddr;
      targetVariableAddr += LBlockHashTable.VAR_LENGTH_SIZE; // we'll write length last.

      for(int field = 0; field < fieldCount; field++){

        final long offsetAddress = offsetAddresses[field];
        final long startAndEnd = PlatformDependent.getLong(offsetAddress);
        final int firstOffset = (int) startAndEnd;
        final int secondOffset = (int) (startAndEnd >> 32);
        final long len = secondOffset - firstOffset;

        // update bit address.
        final long bitAddress = bitAddresses[field];
        final int bitVal = (PlatformDependent.getByte(bitAddress >>> 3) >>> (bitAddress & 7)) & 1;
        long targetNullByteAddress = targetFixedAddress + nullByteOffset[field];
        PlatformDependent.putInt(targetNullByteAddress, PlatformDependent.getInt(targetNullByteAddress) | (bitVal << nullBitOffset[field]));

        // update length.
        final int copyLength = (int) (bitVal * len);
        PlatformDependent.putInt(targetVariableAddr, copyLength);
        targetVariableAddr += 4;
        varLen += 4;

        // copy data
        final long dataAddress = dataAddresses[field];
        final long srcDataStart = dataAddress + firstOffset;
        Copier.copy(srcDataStart, targetVariableAddr, copyLength);

        // update pointers.
        targetVariableAddr += copyLength;
        varLen += copyLength;
        offsetAddresses[field] += 4;
        bitAddresses[field]++;
      }

      // set total varlen in fixed block.
      PlatformDependent.putInt(varLenAddress, varLen);
      varOffsetAddress += blockWidth;
      targetFixedAddress += blockWidth;
    }
  }

  public static void pivot(PivotDef pivot, int count, FixedBlockVector fixedBlock, VariableBlockVector variable) {
    fixedBlock.ensureAvailableBlocks(count);
    for(VectorPivotDef def : pivot.getFixedPivots()){
      switch(def.getType()){
      case BIT:
        pivotBit(def, fixedBlock, count);
        break;
      case FOUR_BYTE:
        pivot4Bytes(def, fixedBlock, count);
        break;
      case EIGHT_BYTE:
        pivot8Bytes(def, fixedBlock, count);
        break;
      case SIXTEEN_BYTE:
        pivot16Bytes(def, fixedBlock, count);
        break;
      case VARIABLE:
      default:
        throw new UnsupportedOperationException("Pivot: unknown type: " + Describer.describe(def.getIncomingVector().getField()));
      }
    }
    if(pivot.getVariableCount() > 0){
      pivotVariableLengths(pivot.getVariablePivots(), fixedBlock, variable, count);
    }
  }

  static void pivotBit(
      VectorPivotDef def,
      FixedBlockVector fixedBlock,
      final int count
      ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2,
      "A Bit vector should have two field buffers. %s has %s buffers.", Describer.describe(field.getField()), buffers.size());
    Preconditions.checkArgument(def.getNullBitOffset() + 1 == def.getOffset(),
      "A BIT definition should define the null bit next to the value bit. Instead: bit offset=%d, val offset=%d",
      def.getNullBitOffset(), def.getOffset());

    final int blockLength = fixedBlock.getBlockWidth();
    final int validityBitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();
    long targetAddr = fixedBlock.getMemoryAddress();

    // both validity bits and value bits processed a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BYTES);
    long bitTargetAddr = targetAddr + def.getNullByteOffset();

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long validityBitValues = PlatformDependent.getLong(srcBitsAddr);
      final long bitValues = PlatformDependent.getLong(srcDataAddr);
      if (validityBitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (WORD_BITS * blockLength);
      } else {
        // at least some are set
        final long nextBitTargetAddr = bitTargetAddr + WORD_BITS * blockLength;
        for (long remainingValidity = validityBitValues, remainingValue = bitValues;
             remainingValidity != 0;
             remainingValidity = remainingValidity >>> 1, remainingValue = remainingValue >>> 1, bitTargetAddr += blockLength) {
          // Valid and value bits are next to each other. Setting them together
          int valid = (int)(remainingValidity & 0x01l);
          int isSet = (int)(remainingValue & 0x01l);
          int bitPair = (((isSet * valid) << 1) | valid) << validityBitOffset;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitPair);
        }
        bitTargetAddr = nextBitTargetAddr;
      }
      srcBitsAddr += WORD_BYTES;
      srcDataAddr += WORD_BYTES;
    }

    // do the remaining inputs
    if (remainCount > 0) {
      final long validityBitValues = PlatformDependent.getLong(srcBitsAddr);
      final long bitValues = PlatformDependent.getLong(srcDataAddr);
      if (validityBitValues == NONE_SET) {
        // noop (all nulls).
      } else {
        // at least some are set
        for (long remainingValidity = validityBitValues, remainingValue = bitValues, i = 0;
             i < remainCount;
             remainingValidity = remainingValidity >>> 1, remainingValue = remainingValue >>> 1, bitTargetAddr += blockLength, i++) {
          // Valid and value bits are next to each other. Setting them together
          int valid = (int)(remainingValidity & 0x01l);
          int isSet = (int)(remainingValue & 0x01l);
          int bitPair = (((isSet * valid) << 1) | valid) << validityBitOffset;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitPair);
        }
      }
    }

  }

  static void pivot4Bytes(
    VectorPivotDef def,
    FixedBlockVector fixedBlock,
    final int count
  ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2, "A four byte vector should have two field buffers. %s has %s buffers.", Describer.describe(field.getField()), buffers.size());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();
    long targetAddr = fixedBlock.getMemoryAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * FOUR_BYTE);

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    // NB: DX-12276, DX-122458 workaround: explicitly using longs in the arithmetic below to avoid crash in the C2 JIT compiler
    final long wordBitsL = WORD_BITS;
    final long fourByteL = FOUR_BYTE;
    final long wordBytesL = WORD_BYTES;

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);

      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (wordBitsL * blockLength);
        valueTargetAddr += (wordBitsL * blockLength);
        srcDataAddr += (wordBitsL * fourByteL);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += fourByteL) {
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += fourByteL) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr) * bitVal);
        }
      }
      srcBitsAddr += wordBytesL;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < remainCount; i++, valueTargetAddr += blockLength, srcDataAddr += fourByteL) {
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += fourByteL) {
          int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr) * bitVal);
        }
      }
    }

  }

  static void pivot8Bytes(
      VectorPivotDef def,
      FixedBlockVector fixedBlock,
      final int count
      ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2, "A eight byte vector should have two field buffers. %s has %s buffers.", Describer.describe(field.getField()), buffers.size());

    final long blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();
    long targetAddr = fixedBlock.getMemoryAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    // NB: DX-12276 workaround: explicitly using longs in the arithmetic below to avoid crash in the C2 JIT compiler
    final long wordBitsL = WORD_BITS;
    final long eightByteL = EIGHT_BYTE;
    final long wordBytesL = WORD_BYTES;

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);

      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (wordBitsL * blockLength);
        valueTargetAddr += (wordBitsL * blockLength);
        srcDataAddr += (wordBitsL * eightByteL);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += eightByteL) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += eightByteL) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
        }
      }
      srcBitsAddr += wordBytesL;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < remainCount; i++, valueTargetAddr += blockLength, srcDataAddr += eightByteL) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += eightByteL) {
          int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
        }
      }
    }

  }

  static void pivot16Bytes(
    VectorPivotDef def,
    FixedBlockVector fixedBlock,
    final int count
  ) {
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2, "A sixteen byte vector should have two field buffers. %s has %s buffers.", Describer.describe(field.getField()), buffers.size());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress();
    long srcDataAddr = buffers.get(1).memoryAddress();
    long targetAddr = fixedBlock.getMemoryAddress();

    // determine number of null values to work through a word at a time.
    final int remainCount = count % WORD_BITS;
    final int wordCount = (count - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * SIXTEEN_BYTE);

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    // NB: DX-12276, DX-12458 workaround: explicitly using longs in the arithmetic below to avoid crash in the C2 JIT compiler
    final long wordBitsL = WORD_BITS;
    final long eightByteL = EIGHT_BYTE;
    final long sixteenByteL = SIXTEEN_BYTE;
    final long wordBytesL = WORD_BYTES;

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);

      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (wordBitsL * blockLength);
        valueTargetAddr += (wordBitsL * blockLength);
        srcDataAddr += (wordBitsL * sixteenByteL);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += sixteenByteL) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
          PlatformDependent.putLong(valueTargetAddr + eightByteL, PlatformDependent.getLong(srcDataAddr + eightByteL));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += sixteenByteL) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
          PlatformDependent.putLong(valueTargetAddr + eightByteL, PlatformDependent.getLong(srcDataAddr + eightByteL) * bitVal);
        }
      }
      srcBitsAddr += wordBytesL;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant AND. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < remainCount; i++, valueTargetAddr += blockLength, srcDataAddr += sixteenByteL) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
          PlatformDependent.putLong(valueTargetAddr + eightByteL, PlatformDependent.getLong(srcDataAddr + eightByteL));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < remainCount; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += sixteenByteL) {
          int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
          PlatformDependent.putLong(valueTargetAddr + eightByteL, PlatformDependent.getLong(srcDataAddr + eightByteL) * bitVal);
        }
      }
    }
  }
}

