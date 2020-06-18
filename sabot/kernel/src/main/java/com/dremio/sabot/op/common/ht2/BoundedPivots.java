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

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_LENGTH_SIZE;

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;

import com.dremio.common.expression.Describer;
import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * Build pivots from incoming batch starting at a particular offset and fill output buffers until output buffers are
 * full or required count is reached.
 */
public class BoundedPivots {
  private static final int FOUR_BYTE = 4;
  private static final int EIGHT_BYTE = 8;
  private static final int SIXTEEN_BYTE = 16;
  private static final int WORD_BITS = 64;
  private static final int WORD_BYTES = 8;
  private static final long ALL_SET = 0xFFFFFFFFFFFFFFFFL;
  private static final long NONE_SET = 0;

  private static int pivotVariableLengths(
      final List<VectorPivotDef> fields,
      final FixedBlockVector targetFixed,
      final VariableBlockVector targetVariable,
      final int start,
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
    for(VectorPivotDef vpd : fields){
      nullByteOffset[i] = vpd.getNullByteOffset();
      nullBitOffset[i] = vpd.getNullBitOffset();

      List<ArrowBuf> buffers = vpd.getIncomingVector().getFieldBuffers();
      Preconditions.checkArgument(buffers.size() == 3,
          "A variable length vector should have three field buffers. %s has %s buffers.",
          Describer.describe(vpd.getIncomingVector().getField()), buffers.size());

      // convert to bit offsets. Overflows shouldn't exist since no system (yet) has memory of Long.MAX_VALUE / 8
      bitAddresses[i] = buffers.get(0).memoryAddress() * 8;
      offsetAddresses[i] = buffers.get(1).memoryAddress();
      dataAddresses[i] = buffers.get(2).memoryAddress();

      // Move addresses to starting record in input
      bitAddresses[i] += start;
      offsetAddresses[i] += (start * 4); // 4 is width of value offset space
      // no need to offset dataAddr as we use values from offset vector to refer to data addresses.

      i++;
    }

    final int blockWidth = targetFixed.getBlockWidth();

    final long startingVariableAddr = targetVariable.getMemoryAddress();
    final long maxTargetVariableAddr = targetVariable.getMaxMemoryAddress();
    long varOffsetAddress = targetFixedAddress + dataWidth;
    long targetVariableAddr = targetVariable.getMemoryAddress();

    int outputRecordIdx = 0;

    ReachedTargetBufferLimit:
    while(outputRecordIdx < count) {
      int varLen = 0;
      // write the starting position of the variable width data.
      PlatformDependent.putInt(varOffsetAddress, (int) (targetVariableAddr - startingVariableAddr));
      final long varLenAddress = targetVariableAddr;
      targetVariableAddr += VAR_LENGTH_SIZE; // we'll write length last.

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

        // check if we are still within the buffer capacity. If not exit
        if (targetVariableAddr + VAR_LENGTH_SIZE + copyLength > maxTargetVariableAddr) {
          // leaving at this point it is possible that we may have partial list written, thats ok as the caller is
          // expected to read based on the output count returned which doesn't include the partially filled record.
          break ReachedTargetBufferLimit;
        }

        PlatformDependent.putInt(targetVariableAddr, copyLength);
        targetVariableAddr += VAR_LENGTH_SIZE;
        varLen += VAR_LENGTH_SIZE;

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

      outputRecordIdx++;
    }

    if (outputRecordIdx == 0) {
     throw new StringIndexOutOfBoundsException("Not enough space to pivot single record. Allocated capacity for pivot: "
       + Long.toString(targetVariable.getMaxMemoryAddress() - targetVariable.getMemoryAddress()) + " bytes.");
    }

    return outputRecordIdx;
  }

  public static int pivot(PivotDef pivot, int start, int count, FixedBlockVector fixedBlock, VariableBlockVector variable) {
    // We are constrained by the capacity of variable block vector and count.
    // First fill the variable width vectors to find how many records we can fit in.
    if (pivot.getVariableCount() > 0) {
      int updatedCount = pivotVariableLengths(pivot.getVariablePivots(), fixedBlock, variable, start, count);
      Preconditions.checkState(updatedCount <= count);
      count = updatedCount;
    }

    for(VectorPivotDef def : pivot.getFixedPivots()){
      switch(def.getType()){
        case BIT:
          pivotBit(def, fixedBlock, start, count);
          break;
        case FOUR_BYTE:
          pivot4Bytes(def, fixedBlock, start, count);
          break;
        case EIGHT_BYTE:
          pivot8Bytes(def, fixedBlock, start, count);
          break;
        case SIXTEEN_BYTE:
          pivot16Bytes(def, fixedBlock, start, count);
          break;
        case VARIABLE:
        default:
          throw new UnsupportedOperationException("Pivot: unknown type: " + Describer.describe(def.getIncomingVector().getField()));
      }
    }

    return count;
  }

  static void pivotBit(
    VectorPivotDef def,
    FixedBlockVector fixedBlock,
    final int start,
    final int count
  ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2,
                                "A Bit vector should have two field buffers. %s has %s buffers.", Describer.describe(field.getField()), buffers.size());
    Preconditions.checkArgument(def.getNullBitOffset() + 1 == def.getOffset(),
                                "A BIT definition should define the null bit next to the value bit. Instead: bit offset=%s, val offset=%s",
                                def.getNullBitOffset(), def.getOffset());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress() + (start / WORD_BITS) * WORD_BYTES;
    long srcDataAddr = buffers.get(1).memoryAddress() + (start / WORD_BITS) * WORD_BYTES;
    long targetAddr = fixedBlock.getMemoryAddress();

    long bitTargetAddr = targetAddr + def.getNullByteOffset();

    final int partialCountInFirstWord = (WORD_BITS - start % WORD_BITS) % WORD_BITS;
    if (partialCountInFirstWord > 0) {
      final int maxCopy = Math.min(partialCountInFirstWord, count);
      int offsetInFirstWord = start % WORD_BITS;
      long validityBitValues = PlatformDependent.getLong(srcBitsAddr);
      long dataBitValues = PlatformDependent.getLong(srcDataAddr);
      validityBitValues = validityBitValues >>> offsetInFirstWord;
      dataBitValues = dataBitValues >>> offsetInFirstWord;
      for (long remainingValidity = validityBitValues, remainingValue = dataBitValues, i = 0;
           i < maxCopy;
           remainingValidity = remainingValidity >>> 1, remainingValue = remainingValue >>> 1, bitTargetAddr += blockLength, i++) {
        // Valid and value bits are next to each other. Setting them together
        int valid = (int)(remainingValidity & 0x01l);
        int isSet = (int)(remainingValue & 0x01l);
        int bitPair = (((isSet * valid) << 1) | valid) << bitOffset;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitPair);
      }
      srcBitsAddr += WORD_BYTES;
      srcDataAddr += WORD_BYTES;
    }

    // move to start of the next word
    final int newStart = start + partialCountInFirstWord;
    if (newStart > start + count) {
      return; // we already copied the needed from first partial word it self
    }
    final int newCount = count - partialCountInFirstWord;

    // determine number of null values to work through a word at a time.
    final int remainCount = newCount % WORD_BITS;
    final int wordCount = (newCount - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BYTES);

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long validityBitValues = PlatformDependent.getLong(srcBitsAddr);
      final long bitValues = PlatformDependent.getLong(srcDataAddr);
      if (validityBitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (WORD_BITS * blockLength);
      } else {
        // at least some are set
        final long newBitTargetAddr = bitTargetAddr + (WORD_BITS * blockLength);
        for (long remainingValidity = validityBitValues, remainingValue = bitValues;
             remainingValidity != 0;
             remainingValidity = remainingValidity >>> 1, remainingValue = remainingValue >>> 1, bitTargetAddr += blockLength) {
          // Valid and value bits are next to each other. Setting them together
          int valid = (int)(remainingValidity & 0x01l);
          int isSet = (int)(remainingValue & 0x01l);
          int bitPair = (((isSet * valid) << 1) | valid) << bitOffset;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitPair);
        }
        bitTargetAddr = newBitTargetAddr;
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
          int bitPair = (((isSet * valid) << 1) | valid) << bitOffset;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitPair);
        }
      }
    }

  }

  static void pivot4Bytes(
      VectorPivotDef def,
      FixedBlockVector fixedBlock,
      final int start,
      final int count
  ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2,
        "A four byte vector should have two field buffers. %s has %s buffers.",
        Describer.describe(field.getField()), buffers.size());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress() + (start / WORD_BITS) * WORD_BYTES;
    long srcDataAddr = buffers.get(1).memoryAddress() + start * FOUR_BYTE;
    long targetAddr = fixedBlock.getMemoryAddress();

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    final int partialCountInFirstWord = (WORD_BITS - start % WORD_BITS) % WORD_BITS;
    if (partialCountInFirstWord > 0) {
      final int maxCopy = Math.min(partialCountInFirstWord, count);
      int offsetInFirstWord = start % WORD_BITS;
      long bitValues = PlatformDependent.getLong(srcBitsAddr);
      bitValues = bitValues >>> offsetInFirstWord;
      for (int i = 0; i < maxCopy;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += FOUR_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr) * bitVal);
      }
      srcBitsAddr += WORD_BYTES;
    }

    // move to start of the next word
    final int newStart = start + partialCountInFirstWord;
    if (newStart > start + count) {
      return; // we already copied the needed from first partial word it self
    }
    final int newCount = count - partialCountInFirstWord;

    // determine number of null values to work through a word at a time.
    final int remainCount = newCount % WORD_BITS;
    final int wordCount = (newCount - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * FOUR_BYTE);

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (WORD_BITS * blockLength);
        valueTargetAddr += (WORD_BITS * blockLength);
        srcDataAddr += (WORD_BITS * FOUR_BYTE);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += FOUR_BYTE) {
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += FOUR_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr) * bitVal);
        }
      }
      srcBitsAddr += WORD_BYTES;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      for (int i = 0; i < remainCount;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += FOUR_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putInt(valueTargetAddr, PlatformDependent.getInt(srcDataAddr) * bitVal);
      }
    }
  }

  static void pivot8Bytes(
      VectorPivotDef def,
      FixedBlockVector fixedBlock,
      final int start,
      final int count
  ){
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2,
        "A four byte vector should have two field buffers. %s has %s buffers.",
        Describer.describe(field.getField()), buffers.size());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress() + (start / WORD_BITS) * WORD_BYTES;
    long srcDataAddr = buffers.get(1).memoryAddress() + start * EIGHT_BYTE;
    long targetAddr = fixedBlock.getMemoryAddress();

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    final int partialCountInFirstWord = (WORD_BITS - start % WORD_BITS) % WORD_BITS;
    if (partialCountInFirstWord > 0) {
      int offsetInFirstWord = start % WORD_BITS;
      long bitValues = PlatformDependent.getLong(srcBitsAddr);
      bitValues = bitValues >>> offsetInFirstWord;
      final int maxCopy = Math.min(partialCountInFirstWord, count);
      for (int i = 0; i < maxCopy;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += EIGHT_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
      }
      srcBitsAddr += WORD_BYTES;
    }

    // move to start of the next word
    final int newStart = start + partialCountInFirstWord;
    if (newStart > start + count) {
      return; // we already copied the needed from first partial word it self
    }
    final int newCount = count - partialCountInFirstWord;

    // determine number of null values to work through a word at a time.
    final int remainCount = newCount % WORD_BITS;
    final int wordCount = (newCount - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * EIGHT_BYTE);

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (WORD_BITS * blockLength);
        valueTargetAddr += (WORD_BITS * blockLength);
        srcDataAddr += (WORD_BITS * EIGHT_BYTE);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += EIGHT_BYTE) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += EIGHT_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
        }
      }
      srcBitsAddr += WORD_BYTES;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      for (int i = 0; i < remainCount;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += EIGHT_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
      }
    }
  }

  static void pivot16Bytes(
      VectorPivotDef def,
      FixedBlockVector fixedBlock,
      final int start,
      final int count
  ) {
    final FieldVector field = def.getIncomingVector();
    final List<ArrowBuf> buffers = field.getFieldBuffers();

    Preconditions.checkArgument(buffers.size() == 2,
        "A four byte vector should have two field buffers. %s has %s buffers.",
        Describer.describe(field.getField()), buffers.size());

    final int blockLength = fixedBlock.getBlockWidth();
    final int bitOffset = def.getNullBitOffset();

    long srcBitsAddr = buffers.get(0).memoryAddress() + (start / WORD_BITS) * WORD_BYTES;
    long srcDataAddr = buffers.get(1).memoryAddress() + start * SIXTEEN_BYTE;
    long targetAddr = fixedBlock.getMemoryAddress();

    long bitTargetAddr = targetAddr + def.getNullByteOffset();
    long valueTargetAddr = targetAddr + def.getOffset();

    final int partialCountInFirstWord = (WORD_BITS - start % WORD_BITS) % WORD_BITS;
    if (partialCountInFirstWord > 0) {
      int offsetInFirstWord = start % WORD_BITS;
      long bitValues = PlatformDependent.getLong(srcBitsAddr);
      bitValues = bitValues >>> offsetInFirstWord;
      final int maxCopy = Math.min(partialCountInFirstWord, count);
      for (int i = 0; i < maxCopy;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += SIXTEEN_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
        PlatformDependent.putLong(valueTargetAddr + EIGHT_BYTE, PlatformDependent.getLong(srcDataAddr + EIGHT_BYTE) * bitVal);
      }
      srcBitsAddr += WORD_BYTES;
    }

    // move to start of the next word
    final int newStart = start + partialCountInFirstWord;
    if (newStart > start + count) {
      return; // we already copied the needed from first partial word it self
    }
    final int newCount = count - partialCountInFirstWord;

    // determine number of null values to work through a word at a time.
    final int remainCount = newCount % WORD_BITS;
    final int wordCount = (newCount - remainCount) / WORD_BITS;
    final long finalWordAddr = srcDataAddr + (wordCount * WORD_BITS * SIXTEEN_BYTE);

    // decode word at a time.
    while (srcDataAddr < finalWordAddr) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      if (bitValues == NONE_SET) {
        // noop (all nulls).
        bitTargetAddr += (WORD_BITS * blockLength);
        valueTargetAddr += (WORD_BITS * blockLength);
        srcDataAddr += (WORD_BITS * SIXTEEN_BYTE);

      } else if (bitValues == ALL_SET) {
        // all set, set the bit values using a constant. Independently set the data values without transformation.
        final int bitVal = 1 << bitOffset;
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength) {
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | bitVal);
        }

        for (int i = 0; i < WORD_BITS; i++, valueTargetAddr += blockLength, srcDataAddr += SIXTEEN_BYTE) {
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr));
          PlatformDependent.putLong(valueTargetAddr + EIGHT_BYTE, PlatformDependent.getLong(srcDataAddr + EIGHT_BYTE));
        }

      } else {
        // some nulls, some not, update each value to zero or the value, depending on the null bit.
        for (int i = 0; i < WORD_BITS; i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += SIXTEEN_BYTE) {
          final int bitVal = ((int) (bitValues >>> i)) & 1;
          PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
          PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
          PlatformDependent.putLong(valueTargetAddr + EIGHT_BYTE, PlatformDependent.getLong(srcDataAddr + EIGHT_BYTE) * bitVal);
        }
      }
      srcBitsAddr += WORD_BYTES;
    }

    // do the remaining bits..
    if(remainCount > 0) {
      final long bitValues = PlatformDependent.getLong(srcBitsAddr);
      for (int i = 0; i < remainCount;
           i++, bitTargetAddr += blockLength, valueTargetAddr += blockLength, srcDataAddr += SIXTEEN_BYTE) {
        int bitVal = ((int) (bitValues >>> i)) & 1;
        PlatformDependent.putInt(bitTargetAddr, PlatformDependent.getInt(bitTargetAddr) | (bitVal << bitOffset));
        PlatformDependent.putLong(valueTargetAddr, PlatformDependent.getLong(srcDataAddr) * bitVal);
        PlatformDependent.putLong(valueTargetAddr + EIGHT_BYTE, PlatformDependent.getLong(srcDataAddr + EIGHT_BYTE) * bitVal);
      }
    }
  }
}

