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

package org.apache.arrow.vector;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.util.internal.PlatformDependent;


/**
 * MutableVarcharVector implements a variable width vector of VARCHAR
 * values which could be NULL. A validity buffer (bit vector) is maintained
 * to track which elements in the vector are null. The main difference between
 * a VarCharVector is that the values may be mutated over time. Internally the values
 * are always appended at the end of the buffer.The index to those values is maintained
 * internally in another vector viz. 'fwdIndex'. Thus over time it may have unreferenced
 * values (called as garbage). Compaction must be preformed over time to recover space in the buffer.
 */
public class MutableVarcharVector extends BaseVariableWidthVector {
  private int garbageSizeInBytes;
  private double compactionThreshold;

  private UInt2Vector fwdIndex;
  private int head; //the index at which a new value may be appended

  /**
   * Instantiate a MutableVarcharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name                name of the vector
   * @param allocator           allocator for memory management.
   * @param compactionThreshold this value bounds the ratio of garbage data to capacity of the buffer. If the ratio
   *                            is more than the threshold, then compaction gets triggered on next update.
   */
  public MutableVarcharVector(String name, BufferAllocator allocator, double compactionThreshold) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator, compactionThreshold);
  }

  /**
   * Instantiate a MutableVarcharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name                name of the vector
   * @param fieldType           type of Field materialized by this vector
   * @param allocator           allocator for memory management.
   * @param compactionThreshold this value bounds the ratio of garbage data to capacity of the buffer. If the ratio
   *                            is more than the threshold, then compaction gets triggered on next update.
   */
  public MutableVarcharVector(String name, FieldType fieldType, BufferAllocator allocator, double compactionThreshold) {
    super(new Field(name, fieldType, null), allocator);

    this.compactionThreshold = compactionThreshold;
    fwdIndex = new UInt2Vector(name, allocator);
  }

  public final void setCompactionThreshold(double in) {
    compactionThreshold = in;
  }

  public final boolean needsCompaction() {
    return (((garbageSizeInBytes * (1.0D)) / getDataBuffer().capacity()) > compactionThreshold);
  }

  /**
   * @return The offset at which the next valid data may be put in the buffer.
   */
  public final int getCurrentOffset() {
    return getstartOffset(head);
  }


  /**
   * This value gets updated as the previous values in the buffer are updated with newer ones.
   * The length of the old value is added to the garbageSizeInBytes.
   *
   * @return the garbage size accumulated so far in the buffer.
   */
  public final int getGarbageSizeInBytes() {
    return garbageSizeInBytes;
  }

  /**
   * @return the size used in the buffer
   */
  public final int getUsedByteCapacity() {
    return (getByteCapacity() - getGarbageSizeInBytes());
  }


  /**
   * Get a reader that supports reading values from this vector.
   *
   * @return Field Reader for this vector
   */
  @Override
  public FieldReader getReader() {
    //return reader;
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Get minor type for this vector. The vector holds values belonging
   * to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VARCHAR;
  }


  /**
   * zero out the vector and the data in associated buffers.
   */
  public void zeroVector() {
    super.zeroVector();

    fwdIndex.zeroVector();
    head = 0;
    garbageSizeInBytes = 0;
  }

  /**
   * Reset the vector to initial state. Same as {@link #zeroVector()}.
   * Note that this method doesn't release any memory.
   */
  public void reset() {
    super.reset();

    fwdIndex.reset();
    head = 0;
    garbageSizeInBytes = 0;
  }

  /**
   * Close the vector and release the associated buffers.
   */
  @Override
  public void close() {
    this.clear();
  }

  /**
   * Same as {@link #close()}.
   */
  @Override
  public void clear() {
    super.clear();

    fwdIndex.clear();
    head = 0;
    garbageSizeInBytes = 0;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Get the variable length element at specified index as byte array.
   *
   * @param index position of element to get
   * @return array of bytes for non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;

    final int actualIndex = fwdIndex.get(index);
    assert actualIndex >= 0;

    if (super.isSet(actualIndex) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    final int startOffset = getStartOffset(actualIndex);
    final int dataLength =
      offsetBuffer.getInt((actualIndex + 1) * OFFSET_WIDTH) - startOffset;
    final byte[] result = new byte[dataLength];
    valueBuffer.getBytes(startOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index position of element to get
   * @return Text object for non-null element, null otherwise
   */
  public Text getObject(int index) {
    Text result = new Text();
    byte[] b;
    try {
      b = get(index);
    } catch (IllegalStateException e) {
      return null;
    }
    result.set(b);
    return result;
  }

  /**
   * Get the variable length element at specified index and sets the state
   * in provided holder.
   *
   * @param index  position of element to get
   * @param holder data holder to be populated by this function
   */
  public void get(int index, NullableVarCharHolder holder) {
    assert index >= 0;

    if (fwdIndex.isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }

    final int actualIndex = fwdIndex.get(index);
    assert actualIndex >= 0;

    if (super.isSet(actualIndex) == 0) {
      throw new IllegalStateException("Value at actualIndex is null ");
    }

    holder.isSet = 1;
    holder.start = getstartOffset(actualIndex);
    holder.end = offsetBuffer.getInt((actualIndex + 1) * OFFSET_WIDTH);
    holder.buffer = valueBuffer;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from      source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, VarCharVector from) {
        /*
        final int start = from.offsetBuffer.getInt(fromIndex * OFFSET_WIDTH);
        final int end = from.offsetBuffer.getInt((fromIndex + 1) * OFFSET_WIDTH);
        final int length = end - start;
        fillHoles(thisIndex);
        BitVectorHelper.setValidityBit(this.validityBuffer, thisIndex, from.isSet(fromIndex));
        final int copyStart = offsetBuffer.getInt(thisIndex * OFFSET_WIDTH);
        from.valueBuffer.getBytes(start, this.valueBuffer, copyStart, length);
        offsetBuffer.setInt((thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
        lastSet = thisIndex;
        */
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Same as {@link #copyFrom(int, int, VarCharVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from      source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, VarCharVector from) {
        /*
        final int start = from.offsetBuffer.getInt(fromIndex * OFFSET_WIDTH);
        final int end = from.offsetBuffer.getInt((fromIndex + 1) * OFFSET_WIDTH);
        final int length = end - start;
        handleSafe(thisIndex, length);
        fillHoles(thisIndex);
        BitVectorHelper.setValidityBit(this.validityBuffer, thisIndex, from.isSet(fromIndex));
        final int copyStart = offsetBuffer.getInt(thisIndex * OFFSET_WIDTH);
        from.valueBuffer.getBytes(start, this.valueBuffer, copyStart, length);
        offsetBuffer.setInt((thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
        lastSet = thisIndex;
        */
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * This api is invoked during the 'set/setSafe' operations.
   * If the index is already valid, then increment the garbageSizeInBytes, as the value
   * is being overwritten. If then the compaction threshold is met, then
   * trigger the compaction.
   *
   * @param index the position at which the set operation is being done.
   */
  private final void updateGarbageAndCompact(final int index) {
    final boolean isUpdate = (fwdIndex.isSafe(index) && !fwdIndex.isNull(index));
    if (isUpdate) {
      final int actualIndex = fwdIndex.get(index);
      final int dataOffset = getstartOffset(actualIndex);
      final int dataLength =
        offsetBuffer.getInt((actualIndex + 1) * OFFSET_WIDTH) - dataOffset;
      garbageSizeInBytes += dataLength;

      //mark the value in buffer as invalid
      setNull(actualIndex);

      //treat this index also as invalid, as its going to get overwritten
      fwdIndex.setNull(index);
    }

    if (needsCompaction()) {
      compactInternal();
    }
  }

  /*
   * Compact the data buffer by moving all the valid entries
   * to the top. First check if the threshold has been met.
   *
   * */
  public void compact() {
    //threshold not met
    if (!needsCompaction()) {
      return;
    }

    compactInternal();
  }

  /*
   * Forces the compaction to run, even if the threshold
   * may not have been met.
   */
  public void forceCompact() {
    compactInternal();
  }

  /* Actual api that does compaction */
  final private void compactInternal() {
    //maps valid offset to its corresponding index
    final HashMap<Integer, Integer> validOffsetMap = new HashMap<> ();

    //populate the mapping
    final int idxCapacity = fwdIndex.getValueCapacity();
    for (int i = 0; i < idxCapacity; ++i) {
      if (!fwdIndex.isNull(i)) {
        validOffsetMap.put((int) fwdIndex.get(i), i);
      }
    }

    final ArrowBuf buffer = getDataBuffer();
    final ArrowBuf offsetBuffer = getOffsetBuffer();

    int lastOffset = -1;
    int current = 0;
    int target = 0;

    while (current < head) {
      //check the validity bitmap
      final boolean isValid = (super.isSet(current) !=0) ;

      if (isValid && (current == target)) {
        ++current;
        ++target;
        continue;
      }

      if (isValid && (current != target)) {

        //get the corresponding offsets
        final int validOffset = offsetBuffer.getInt(current * OFFSET_WIDTH);
        final int validLen = offsetBuffer.getInt((current + 1) * OFFSET_WIDTH) - validOffset;

        if (lastOffset == -1) {
          lastOffset = offsetBuffer.getInt(target * OFFSET_WIDTH);
        }

        //shift the valid data to lastOffset
        PlatformDependent.copyMemory(buffer.memoryAddress() + validOffset, buffer.memoryAddress() + lastOffset, validLen);

        //This is the new location where next valid data is put.
        lastOffset += validLen;

        //fix the length as per the valid size
        offsetBuffer.setInt(((target + 1) * OFFSET_WIDTH), lastOffset);

        //update the index to point to new position
        fwdIndex.set(validOffsetMap.get(current), target);

        //fix the validity bit
        BitVectorHelper.setValidityBit(validityBuffer, target, 1);
        setNull(current);

        ++target;
      }

      ++current;
    } //while

    head = target;

    //only valid data remains in the buffer now.
    garbageSizeInBytes = 0;
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder. Internally appends the data at the end.
   *
   * @param index  position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, VarCharHolder holder) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    //append at the end
    fillHoles(head);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int dataLength = holder.end - holder.start;
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, VarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index  position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, VarCharHolder holder) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    //append at the end
    final int dataLength = holder.end - holder.start;
    fillEmpties(head);
    handleSafe(head, dataLength);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index  position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, NullableVarCharHolder holder) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    //append at the end
    fillHoles(head);
    BitVectorHelper.setValidityBit(validityBuffer, head, holder.isSet);
    final int dataLength = holder.end - holder.start;
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, NullableVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index  position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, NullableVarCharHolder holder) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    //append at the end
    final int dataLength = holder.end - holder.start;
    fillEmpties(head);
    handleSafe(head, dataLength);
    BitVectorHelper.setValidityBit(validityBuffer, head, holder.isSet);
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied Text.
   *
   * @param index position of the element to set
   * @param text  Text object with data
   */
  public void set(int index, Text text) {
    set(index, text.getBytes(), 0, text.getLength());
  }

  /**
   * Same as {@link #set(int, NullableVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index position of the element to set.
   * @param text  Text object with data
   */
  public void setSafe(int index, Text text) {
    setSafe(index, text.getBytes(), 0, text.getLength());
  }


  /**
   * Set the variable length element at the specified index to the supplied
   * byte array. This is same as using {@link #set(int, byte[], int, int)}
   * with start as 0 and length as value.length
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  public void set(int index, byte[] value) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillHoles(head);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    setBytes(head, value, 0, value.length);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, byte[])} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  public void setSafe(int index, byte[] value) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillEmpties(head);
    handleSafe(head, value.length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    setBytes(head, value, 0, value.length);
    lastSet = head;

    ++head;
  }

  /**
   * Set the variable length element at the specified index to the supplied
   * byte array.
   *
   * @param index  position of the element to set
   * @param value  array of bytes to write
   * @param start  start index in array of bytes
   * @param length length of data in array of bytes
   */
  public void set(int index, byte[] value, int start, int length) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillHoles(head);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    setBytes(head, value, start, length);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, byte[], int, int)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index  position of the element to set
   * @param value  array of bytes to write
   * @param start  start index in array of bytes
   * @param length length of data in array of bytes
   */
  public void setSafe(int index, byte[] value, int start, int length) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillEmpties(head);
    handleSafe(head, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    setBytes(head, value, start, length);
    lastSet = head;

    ++head;
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied ByteBuffer.
   *
   * @param index  position of the element to set
   * @param value  ByteBuffer with data
   * @param start  start index in ByteBuffer
   * @param length length of data in ByteBuffer
   */
  public void set(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillHoles(head);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + length);
    valueBuffer.setBytes(startOffset, value, start, length);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, ByteBuffer, int, int)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index  position of the element to set
   * @param value  ByteBuffer with data
   * @param start  start index in ByteBuffer
   * @param length length of data in ByteBuffer
   */
  public void setSafe(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillEmpties(head);
    handleSafe(head, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int startOffset = getstartOffset(head);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + length);
    valueBuffer.setBytes(startOffset, value, start, length);
    lastSet = head;

    ++head;
  }


  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   *
   * @param index  position of the new value
   * @param isSet  0 for NULL value, 1 otherwise
   * @param start  start position of data in buffer
   * @param end    end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void set(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    final int dataLength = end - start;
    fillHoles(head);
    BitVectorHelper.setValidityBit(validityBuffer, head, isSet);
    final int startOffset = offsetBuffer.getInt(head * OFFSET_WIDTH);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, buffer, start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   *
   * @param index  position of the new value
   * @param isSet  0 for NULL value, 1 otherwise
   * @param start  start position of data in buffer
   * @param end    end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    final int dataLength = end - start;
    fillEmpties(head);
    handleSafe(head, end);
    BitVectorHelper.setValidityBit(validityBuffer, head, isSet);
    final int startOffset = offsetBuffer.getInt(head * OFFSET_WIDTH);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, buffer, start, dataLength);
    lastSet = head;

    ++head;
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   *
   * @param index  position of the new value
   * @param start  start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void set(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillHoles(head);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int startOffset = offsetBuffer.getInt(head * OFFSET_WIDTH);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = head;

    ++head;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   *
   * @param index  position of the new value
   * @param start  start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;

    updateGarbageAndCompact(index);

    //update the index
    fwdIndex.setSafe(index, head);

    fillEmpties(head);
    handleSafe(head, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, head);
    final int startOffset = offsetBuffer.getInt(head * OFFSET_WIDTH);
    offsetBuffer.setInt((head + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = head;

    ++head;
  }

  /**
   * Copies the entries pointed by fwdIndex into the varchar vector.
   * It assumes that the vector has been sized to hold the entries.
   * @param in Varchar vector where data is copied
   * @param from starting index (inclusive)
   * @param to end index (inclusive) (should be less than value capacity)
   */
  public void copyToVarchar(VarCharVector in, final int from, final int to)
  {
    final int valCapacity = fwdIndex.getValueCapacity();
    for (int i = from; i <= to && i < valCapacity; ++i) {
      if (!fwdIndex.isNull(i)) {
        final int actualIndex = fwdIndex.get(i);
        final int startOffset = getstartOffset(actualIndex);
        final int dataLength =
          offsetBuffer.getInt((actualIndex + 1) * OFFSET_WIDTH) - startOffset;
        in.set(i, startOffset, dataLength, valueBuffer);
      }
    }
  }

  public boolean isIndexSafe(int index)
  {
    return fwdIndex.isSafe(index);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising of this and and a target vector of
   * the same type.
   *
   * @param ref       name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    //return new VarCharVector.TransferImpl(ref, allocator);
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    //return new VarCharVector.TransferImpl((VarCharVector) to);
    throw new UnsupportedOperationException("not supported");
  }

    /*
    private class TransferImpl implements TransferPair {
        VarCharVector to;

        public TransferImpl(String ref, BufferAllocator allocator) {
            to = new VarCharVector(ref, field.getFieldType(), allocator);
        }

        public TransferImpl(VarCharVector to) {
            this.to = to;
        }

        @Override
        public VarCharVector getTo() {
            return to;
        }

        @Override
        public void transfer() {
            transferTo(to);
        }

        @Override
        public void splitAndTransfer(int startIndex, int length) {
            splitAndTransferTo(startIndex, length, to);
        }

        @Override
        public void copyValueSafe(int fromIndex, int toIndex) {
            to.copyFromSafe(fromIndex, toIndex, VarCharVector.this);
        }
    }
    */
}
