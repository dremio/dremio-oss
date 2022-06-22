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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.util.Numbers;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * FixedListVarcharVector implements a variable width vector of VARCHAR values.
 * These values may belong to one or more "group by "groups, identified
 * at same index at groupIndexBuf (2 byte(short) array), which have one entry
 * for each corresponding variable witdh VARCHAR entry.
 *
 * The main difference between a VarCharVector is that the values may be
 * grouped by an external entity as well as mutated/compacted over time
 * (along with garbase collection, by set of predefined rules/limites).
 */
public class FixedListVarcharVector extends BaseVariableWidthVector {
  public static final int FIXED_LISTVECTOR_SIZE_TOTAL = 1024 * 1024; /* 1MB */
  private static final int INDEX_MULTIPLY_FACTOR = 16;

  private ArrowBuf groupIndexBuf;
  private ArrowBuf overflowBuf;
  private final int maxValuesPerBatch;
  private int head = 0; /* Index at which to insert the next element */
  private final Stopwatch compactionTimer = Stopwatch.createUnstarted();
  private int numCompactions = 0;
  private final boolean distinct;
  private final boolean orderby;
  /* Valid only with orderby */
  private final boolean asc;

  public FixedListVarcharVector(String name, BufferAllocator allocator, int maxValuesPerBatch,
                                boolean distinct, boolean orderby, boolean asc) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator,
      maxValuesPerBatch, distinct, orderby, asc);
  }

  /**
   * Instantiate a FixedListVarcharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name                name of the vector
   * @param fieldType           type of Field materialized by this vector
   * @param allocator           allocator for memory management.
   * @param maxValuesPerBatch   maximum number of groups in each batch
   * @param distinct            keep only distinct values
   * @param orderby             keep values sorted
   * @param asc                 sorted values in ascending order or in decending order
   */
  private FixedListVarcharVector(String name, FieldType fieldType, BufferAllocator allocator,
                                 int maxValuesPerBatch, boolean distinct, boolean orderby, boolean asc) {
    super(new Field(name, fieldType, null), allocator);
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.distinct = distinct;
    this.orderby = orderby;
    this.asc = asc;
  }

  public static int getFixedListVectorPerEntryOverhead() {
    /*
     * On average 16 entries per index. The metadata size for each entry in a batch
     * 1. Overflow buffer         -              = 1 bit
     * 2. GroupIndex buffer       - 16 * 2       = 32 bytes
     * 3. Varchar validity buffer - 16 * 1 bit   = 2 bytes
     * 4. Varchar offset buffer   - 16 * 4 bytes = 64 bytes
     *                     total  - 98.125 bytes per entry
     *
     * For a default batchSize of 3968, this does not leave enough space for data hence using
     * this function HashAgg will adjust the batchSize based on exec.operator.aggregator.list.size
     */
    return 98;
  }

  private static int getOverflowBufSize(int batchSize, boolean orderby) {
    /* If orderby present, overflow bitmap is not created */
    return orderby ? 0 : Numbers.nextMultipleOfEight(BitVector.getValidityBufferSizeFromCount(batchSize));
  }

  private static int getDataBufValidityBufferSize(int batchSize, int multiplier) {
    return Numbers.nextMultipleOfEight(VarCharVector.getValidityBufferSizeFromCount(multiplier * batchSize));
  }

  private static int getDataBufValidityBufferSize(int batchSize) {
    return getDataBufValidityBufferSize(batchSize, INDEX_MULTIPLY_FACTOR);
  }

  public static int getValidityBufferSize(int batchSize, boolean orderby) {
    /* Validity bits for the data buffer + overflow bits. */
    return getDataBufValidityBufferSize(batchSize) +
      getOverflowBufSize(batchSize, orderby);
  }

  private static int getGroupIndexBufferSize(int batchSize, int multiplier) {
    return Numbers.nextPowerOfTwo(2 /* bytes per entry in groupIndexBuf */ * batchSize * multiplier);
  }

  private static int getGroupIndexBufferSize(int batchSize) {
    return getGroupIndexBufferSize(batchSize, INDEX_MULTIPLY_FACTOR);
  }

  private static int getOffsetBufferSize(int batchSize, int multiplier) {
    return Numbers.nextMultipleOfEight((batchSize * multiplier + 1) * 4);
  }

  private static int getOffsetBufferSize(int batchSize) {
    return getOffsetBufferSize(batchSize, INDEX_MULTIPLY_FACTOR);
  }

  /**
   * @param batchSize num elements to be stored
   *
   * @return the data buffer capacity required to hold the fixedlist vector
   */
  public static int getDataBufferSize(int batchSize, boolean orderby) {
    /*
     * Total vector size to 1MB. Expected total data buffer size is around 876KB.
     * Each group may have an average size of 226 bytes before any splice'ing.
     */
    int dataBufsize = FIXED_LISTVECTOR_SIZE_TOTAL -
      (getValidityBufferSize(batchSize, orderby) +
       getGroupIndexBufferSize(batchSize) +
       getOffsetBufferSize(batchSize));

    Preconditions.checkState(Numbers.nextMultipleOfEight(dataBufsize) == dataBufsize);

    return dataBufsize;
  }

  /* Total used capacity in the variable width vector */
  private int getUsedByteCapacity() {
    Preconditions.checkState(getLastSet() + 1 == head);
    return super.getStartOffset(head);
  }

  public final int getSizeInBytes() {
    /*
     * Used only to determine which partition to spill based on how big the accumulator
     * and/or HashAgg partition is, so no need to be super accurate.
     */
    return getUsedByteCapacity() + getOffsetBufferSize(head, 1) +
      getGroupIndexBufferSize(head, 1) +
      getDataBufValidityBufferSize(head, 1) +
      getOverflowBufSize(maxValuesPerBatch, orderby);
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
   * Get a reader that supports reading values from this vector.
   *
   * @return Field Reader for this vector
   */
  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * zero out the vector and the data in associated buffers.
   */
  public void zeroVector() {
    super.zeroVector();
    groupIndexBuf.setZero(0, groupIndexBuf.capacity());
    overflowBuf.setZero(0, overflowBuf.capacity());
    head = 0;
  }

  /**
   * Reset the vector to initial state. Same as {@link #zeroVector()}.
   * Note that this method doesn't release any memory.
   */
  public void reset() {
    super.reset();
    groupIndexBuf.setZero(0, groupIndexBuf.capacity());
    overflowBuf.setZero(0, overflowBuf.capacity());
    head = 0;
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
    groupIndexBuf = releaseBuffer(groupIndexBuf);
    overflowBuf = releaseBuffer(overflowBuf);
    head = 0;
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
   * @return array of bytes saved at the index.
   */
  public byte[] get(int index) {
    assert index >= 0;
    Preconditions.checkState(index < head);
    if (super.isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return super.get(getDataBuffer(), getOffsetBuffer(), index);
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

  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/

  private int getFreeSpace() {
    Preconditions.checkState(super.getLastSet() + 1 == head);
    if (head < super.getValueCapacity()) {
      return super.getByteCapacity() - getUsedByteCapacity();
    } else {
      /* Return -1 instead 0 so that the caller cannot even insert a null value */
      return -1;
    }
  }

  private boolean checkHasAvailableSpace(final int space) {
    return getFreeSpace() >= space;
  }

  public long getCompactionTime(TimeUnit unit) {
    return compactionTimer.elapsed(unit);
  }

  public int getNumCompactions() {
    return numCompactions;
  }

  /* Append the given length data within the inputBuf into this vector. */
  public void set(int group, int starOffset, int length, ArrowBuf inputBuf) {
    Preconditions.checkState(checkHasAvailableSpace(length));

    //update the group index
    SV2UnsignedUtil.write(groupIndexBuf.memoryAddress(), head, group);

    //append at the end
    super.set(head, 1, starOffset, starOffset + length, inputBuf);

    ++head;
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    /* XXX: Fill in details */
    throw new UnsupportedOperationException("not supported");
  }

  public void loadBuffers(int valueCount, final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    // load validity buffers
    final int dataValSize = getDataBufValidityBufferSize(valueCount);
    super.validityBuffer = validityBuffer.slice(0, dataValSize);
    super.validityBuffer.writerIndex(dataValSize);
    super.validityBuffer.getReferenceManager().retain(1);

    if (orderby) {
      final int overflowBufSize = getOverflowBufSize(valueCount, orderby);
      overflowBuf = validityBuffer.slice(dataValSize, overflowBufSize);
      overflowBuf.writerIndex(overflowBufSize);
      overflowBuf.getReferenceManager().retain(1);
    } else {
      overflowBuf = allocator.getEmpty();
    }

    //load offset buffer
    final int offsetSize = getOffsetBufferSize(valueCount);
    super.offsetBuffer = dataBuffer.slice(0, offsetSize);
    super.offsetBuffer.writerIndex(offsetSize);
    super.offsetBuffer.getReferenceManager().retain(1);

    //load group index buffer
    final int indexBufSize = getGroupIndexBufferSize(valueCount);
    groupIndexBuf = dataBuffer.slice(offsetSize, indexBufSize);
    groupIndexBuf.writerIndex(indexBufSize);
    groupIndexBuf.getReferenceManager().retain(1);

    //load value data buffer
    final int dataBufOffset = offsetSize + indexBufSize;
    final int dataBufSize = (int)dataBuffer.capacity() - dataBufOffset;
    super.valueBuffer = dataBuffer.slice(dataBufOffset, dataBufSize);
    super.valueBuffer.writerIndex(dataBufSize);
    super.valueBuffer.getReferenceManager().retain(1);

    reset();
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
}
