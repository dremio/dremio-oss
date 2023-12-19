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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntListIterator;

import com.dremio.common.util.Numbers;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.op.aggregate.vectorized.Accumulator;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;


/**
 * FixedListVarcharVector implements a variable width vector of VARCHAR values.
 * These values may belong to one or more "group by "groups, identified
 * at same index at groupIndexBuf (2 byte(short) array), which have one entry
 * for each corresponding variable width VARCHAR entry.
 *
 * The main difference between a VarCharVector is that the values may be
 * grouped by an external entity as well as mutated/compacted over time
 * (along with garbage collection, by set of predefined rules/limites).
 */
public class FixedListVarcharVector extends BaseVariableWidthVector {
  public static final int DELETED_VALUE_ROW_GROUP = 0xFFFF;
  public static final int FIXED_LISTVECTOR_SIZE_TOTAL = 1024 * 1024; /* 1MB */
  private static final int INDEX_MULTIPLY_FACTOR = 16;
  private static final String OVERFLOW_STRING = "\u2026"; // Unicode for ellipsis '...'

  private ArrowBuf groupIndexBuf;
  private ArrowBuf overflowBuf;
  private final int maxValuesPerBatch;
  private int head = 0; /* Index at which to insert the next element */
  private int valueCapacity;
  private final Stopwatch compactionTimer = Stopwatch.createUnstarted();
  private int delimterAndOverflowSize = 0;
  private final Stopwatch distinctTimer = Stopwatch.createUnstarted();
  private final Stopwatch orderByTimer = Stopwatch.createUnstarted();
  private final Stopwatch limitSizeTimer = Stopwatch.createUnstarted();
  private int numDistincts = 0;
  private int numOrderBys = 0;
  private int numLimitSize = 0;
  /* DX-52773: While we compact during listAGG we allow an extra value to be inserted into listVector.
   * During listAggMerge after accumulating and during compact we will know that there is an overflow due to the
   * extra value and set the overflow flag for the rowgroup.
   */
  private final String delimiter;
  private final int delimiterLength;
  private int maxListAggSize;
  private int actualMaxListAggSize;
  private final boolean distinct;
  private final boolean orderby;
  /* Valid only with orderby */
  private final boolean asc;
  private boolean singleNFull = false;
  private final ListVector tempSpace;
  private final Accumulator.AccumStats accumStats;


  public FixedListVarcharVector(String name, BufferAllocator allocator, int maxValuesPerBatch, String delimiter, int maxListAggSize,
                                boolean distinct, boolean orderby, boolean asc, ListVector tempSpace) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator, maxValuesPerBatch,
      delimiter, maxListAggSize, distinct, orderby, asc, null, tempSpace);
  }

  public FixedListVarcharVector(String name, BufferAllocator allocator, int maxValuesPerBatch, String delimiter, int maxListAggSize,
                                boolean distinct, boolean orderby, boolean asc, Accumulator.AccumStats accumStats, ListVector tempSpace) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator, maxValuesPerBatch,
      delimiter, maxListAggSize, distinct, orderby, asc, accumStats, tempSpace);
  }

  /**
   * Instantiate a FixedListVarcharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name                name of the vector
   * @param fieldType           type of Field materialized by this vector
   * @param allocator           allocator for memory management.
   * @param maxValuesPerBatch   maximum number of groups in each batch
   * @param delimiter           delimiter between list of values
   * @param maxListAggSize      Maximum size for list of values for each row group
   * @param distinct            keep only distinct values
   * @param orderby             keep values sorted
   * @param asc                 sorted values in ascending order or in descending order
   * @param accumStats          Accumulator statistics to be collected
   * @param tempSpace           temporary vector to store the output to spill
   */
  private FixedListVarcharVector(String name, FieldType fieldType, BufferAllocator allocator,
                                 int maxValuesPerBatch, String delimiter, int maxListAggSize,
                                 boolean distinct, boolean orderby, boolean asc, Accumulator.AccumStats accumStats,
                                 ListVector tempSpace) {
    super(new Field(name, fieldType, null), allocator);
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.delimiter = delimiter;
    this.delimiterLength = delimiter.length();
    this.maxListAggSize = maxListAggSize;
    this.actualMaxListAggSize = this.maxListAggSize;
    this.distinct = distinct;
    this.orderby = orderby;
    this.asc = asc;
    this.tempSpace = tempSpace;
    this.accumStats = accumStats;
  }

  public static ListVector allocListVector(BufferAllocator allocator, int maxValuesPerBatch) {
    /* Create a ListVector with a BaseVariableWidthVector as inner vector */
    ListVector listVector = ListVector.empty("tmp list-vector", allocator);
    BaseVariableWidthVector dataVector = (BaseVariableWidthVector)listVector.addOrGetVector(
      FieldType.nullable(MinorType.VARCHAR.getType())).getVector();

    /* Allocate the combined buffer and dole out it's memory to variable buffers in ListVector */
    ArrowBuf combinedBuf = allocator.buffer(FIXED_LISTVECTOR_SIZE_TOTAL);
    int combinedBufOffset = 0;

    /* Step 1: Allocate validity and offset buffers for ListVector */
    int validityBufSize = getDataBufValidityBufferSize(maxValuesPerBatch, 1);
    ArrowBuf validityBuffer = combinedBuf.slice(combinedBufOffset, validityBufSize);
    combinedBufOffset += validityBufSize;

    int offsetBufSize = getOffsetBufferSize(maxValuesPerBatch, 1);
    ArrowBuf offsetBuffer = combinedBuf.slice(combinedBufOffset, offsetBufSize);
    combinedBufOffset += offsetBufSize;

    /*
     * XXX: I really don't know why BitVectorHelper.loadValidityBuffer() allocate new buffer
     * for validity buffer with nullCount == 0 || nullCount == maxValuePerBatch.
     * Just to reuse the given validity buffer, providing nullCount as 1.
     */
    ArrowFieldNode fieldNode = new ArrowFieldNode(maxValuesPerBatch, 1);
    List<ArrowBuf> arrowBufs = Arrays.asList(validityBuffer, offsetBuffer);

    /* This will also retain a refCount for each of the buffers */
    listVector.loadFieldBuffers(fieldNode, arrowBufs);

    /* Step 2: Allocate buffers for inner BaseVariableWidthVector */
    validityBufSize = getDataBufValidityBufferSize(maxValuesPerBatch, INDEX_MULTIPLY_FACTOR);
    validityBuffer = combinedBuf.slice(combinedBufOffset, validityBufSize);
    combinedBufOffset += validityBufSize;

    offsetBufSize = getOffsetBufferSize(maxValuesPerBatch, INDEX_MULTIPLY_FACTOR);
    offsetBuffer = combinedBuf.slice(combinedBufOffset, offsetBufSize);
    combinedBufOffset += offsetBufSize;

    ArrowBuf valueBuffer = combinedBuf.slice(combinedBufOffset, combinedBuf.capacity() - combinedBufOffset);

    arrowBufs = Arrays.asList(validityBuffer, offsetBuffer, valueBuffer);

    /* This will also retain a refCount for each of the buffers */
    dataVector.loadFieldBuffers(fieldNode, arrowBufs);

    combinedBuf.getReferenceManager().release();

    listVector.reset();
    return listVector;
  }

  public static int getFixedListVectorPerEntryOverhead() {
    /*
     * On average 16 entries per index. The metadata size for each entry in a batch
     * 1. Overflow buffer         -              = 1 bit
     * 2. GroupIndex buffer       - 16 * 2       = 32 bytes
     * 3. Varchar validity buffer - 16 * 1 bit   =  2 bytes
     * 4. Varchar offset buffer   - 16 * 4 bytes = 64 bytes
     *                     total  - 98.125 bytes per entry
     *
     * For a default batchSize of 3968, this does not leave enough space for data hence using
     * this function HashAgg will adjust the batchSize based on exec.operator.aggregator.list.size
     */
    return INDEX_MULTIPLY_FACTOR * 2 + INDEX_MULTIPLY_FACTOR / 8 + INDEX_MULTIPLY_FACTOR * 4;
  }

  private static int getOverflowBufSize(int batchSize) {
    /* If orderby present, overflow bitmap is not created */
    return Numbers.nextMultipleOfEight(BitVector.getValidityBufferSizeFromCount(batchSize));
  }

  private static int getDataBufValidityBufferSize(int batchSize, int multiplier) {
    return Numbers.nextMultipleOfEight(VarCharVector.getValidityBufferSizeFromCount(multiplier * batchSize));
  }

  private static int getDataBufValidityBufferSize(int batchSize) {
    return getDataBufValidityBufferSize(batchSize, INDEX_MULTIPLY_FACTOR);
  }

  public static int getValidityBufferSize(int batchSize) {
    /* Validity bits for the data buffer + overflow bits. */
    return getDataBufValidityBufferSize(batchSize) +
      getOverflowBufSize(batchSize);
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
  public static int getDataBufferSize(int batchSize) {
    /*
     * Total vector size to 1MB. Expected total data buffer size is around 876KB.
     * Each group may have an average size of 226 bytes before any splice'ing.
     */
    int dataBufsize = FIXED_LISTVECTOR_SIZE_TOTAL - getValidityBufferSize(batchSize);

    Preconditions.checkState(Numbers.nextMultipleOfEight(dataBufsize) == dataBufsize);

    return dataBufsize;
  }

  /* Total used capacity in the variable width vector */
  public int getUsedByteCapacity() {
    Preconditions.checkState(getLastSet() + 1 == head);
    if (head == 0) {
      return 0;
    }
    return super.getStartOffset(head);
  }

  /* Total used capacity in the variable width vector + space required for delimiter and overflow string*/
  public int getRequiredByteCapacity() {
    return getUsedByteCapacity() + delimterAndOverflowSize;
  }

  public final int getSizeInBytes() {
    /*
     * Used only to determine which partition to spill based on how big the accumulator
     * and/or HashAgg partition is, so no need to be super accurate.
     */
    return getUsedByteCapacity() + getOffsetBufferSize(head, 1) +
      getGroupIndexBufferSize(head, 1) +
      getDataBufValidityBufferSize(head, 1) +
      getOverflowBufSize(maxValuesPerBatch);
  }

  @VisibleForTesting
  public int getOverflowReserveSpace() {
    /* Overflow string always prefixed with delimiter. */
    return delimiterLength + OVERFLOW_STRING.getBytes().length;
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
  protected FieldReader getReaderImpl() {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Reset the vector to initial state. Same as {@link #zeroVector()}.
   * Note that this method doesn't release any memory.
   */
  @Override
  public void reset() {
    resetNoOverflowBuf();
    overflowBuf.setZero(0, overflowBuf.capacity());
  }

  private void resetNoOverflowBuf() {
    super.reset();
    groupIndexBuf.setZero(0, groupIndexBuf.capacity());
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
    Preconditions.checkState(index >= 0 && index < head);
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
   @Override
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

  /**
   * If 'numOfRecords' number of records could fit in this vector, return free space left.
   * If these records don't fit (no space in offset buffer/validity buffer), return -1.
   * @param numOfRecords
   * @return
   */
  private int returnFreeSpaceIfRecordsFit(final int numOfRecords) {
    Preconditions.checkState(super.getLastSet() + 1 == head);

    if (head + numOfRecords <= this.valueCapacity) {
      return super.getByteCapacity() - getUsedByteCapacity();
    } else {
      /* Return -1 instead 0 so that the caller cannot even insert a null value */
      return -1;
    }
  }

  @VisibleForTesting
  public boolean isOverflowSet(int rowGroup) {
    return (BitVectorHelper.get(overflowBuf, rowGroup) != 0);
  }

  private void unsetOverflow(int rowGroup) {
    BitVectorHelper.unsetBit(overflowBuf, rowGroup);
  }

  private void setOverflow(int rowGroup) {
    BitVectorHelper.setBit(overflowBuf, (long) rowGroup);
  }

  private boolean isGroupOverflown(final int group) {
    return (!orderby && isOverflowSet(group));
  }

  public boolean hasSpace(final int space, final int numOfRecords, final int group) {
    return (isGroupOverflown(group) || returnFreeSpaceIfRecordsFit(numOfRecords) >= space);
  }

  public int delimterAndOverflowSize() {
    return delimterAndOverflowSize;
  }

  private void calculateDelimiterAndOverflowSize(final IntArrayList[] rowGroups) {
    int delimiterCount = 0;
    int overFlowCount = 0;
    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }
      if (rowGroups[rowGroup].size() > 1) {
        delimiterCount += rowGroups[rowGroup].size() - 1;
      }
      if (isOverflowSet(rowGroup)) {
        overFlowCount++;
      }
    }
    delimterAndOverflowSize = delimiterCount * delimiterLength + overFlowCount * getOverflowReserveSpace();
  }

  @VisibleForTesting
  public void compact() {
    compact(0);
  }

  public void compact(final int nextRecSize) {
    compactionTimer.start();

    final IntArrayList[] rowGroups = extractRowGroups();

    if (distinct) {
      distinct(rowGroups);
    }
    if (orderby) {
      orderBy(rowGroups);
    }

    limitSize(rowGroups, nextRecSize);

    physicallyRearrangeValues(rowGroups);

    calculateDelimiterAndOverflowSize(rowGroups);

    compactionTimer.stop();
    if (accumStats != null) {
      accumStats.incNumCompactions();
      accumStats.updateTotalCompactionTimeBy(compactionTimer.elapsed(TimeUnit.NANOSECONDS));
    }
    compactionTimer.reset();
  }

  private void setGroupIndex(int index, int value) {
    SV2UnsignedUtil.writeAtIndex(groupIndexBuf, index, value);
  }

  @VisibleForTesting
  public int getGroupIndex(int index) {
    Preconditions.checkState(index >= 0 && index < head);
    return SV2UnsignedUtil.readAtIndex(groupIndexBuf, index);
  }

  /* Append the given length data within the inputBuf into this vector. */
  private void addValueToRowGroupInternal(int group, int startOffset, int length, ArrowBuf inputBuf) {
    Preconditions.checkState(hasSpace(length, 1, group));

    // update the group index
    setGroupIndex(head, group);

    //append at the end
    super.set(head, 1, startOffset, startOffset + length, inputBuf);

    ++head;
  }

  public void addValueToRowGroup(int group, int startOffset, int length, ArrowBuf inputBuf) {
    if (isGroupOverflown(group)) {
      return;
    }
    addValueToRowGroupInternal(group, startOffset, length, inputBuf);
  }

  public void addBytesToRowGroup(int group, byte[] bytes) {
    Preconditions.checkState(hasSpace(bytes.length, 1, group));

    // update the group index
    setGroupIndex(head, group);

    //append at the end
    super.set(head, bytes, 0, bytes.length);

    ++head;
  }

  public void addListVectorToRowGroup(int group, ListVector listVector, int listVecIndex) {
    Preconditions.checkState(!listVector.isNull(listVecIndex));

    if (isGroupOverflown(group)) {
      return;
    }

    UnionListReader reader = listVector.getReader();
    reader.setPosition(listVecIndex);

    while (reader.next()) {
      VarCharReader varCharReader = reader.reader();
      if (varCharReader.isSet()) {
        addBytesToRowGroup(group, varCharReader.readText().getBytes());
      }
    }
  }

  @VisibleForTesting
  public int size() {
    return head;
  }

  private void deleteValue(int index) {
    Preconditions.checkState(index >= 0 && index < head);
    SV2UnsignedUtil.writeAtIndex(groupIndexBuf, index, DELETED_VALUE_ROW_GROUP);
    super.setNull(index);
  }

  /**
   * Keep only distinct values for each row group.
   */
  @VisibleForTesting
  public void distinct(final IntArrayList[] rowGroups) {
    ++numDistincts;
    distinctTimer.start();

    for (int i = 0; i < rowGroups.length; i++) {
      if (rowGroups[i] == null) {
        continue;
      }
      Set<Text> h = new HashSet<>();
      IntListIterator iterator = rowGroups[i].iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        Text t = getObject(index);
        if (h.contains(t)) {
          deleteValue(index);
          iterator.remove();
        } else {
          h.add(t);
        }
      }
    }
    distinctTimer.stop();
  }

  public long getDistinctTime(TimeUnit unit) {
    return distinctTimer.elapsed(unit);
  }

  public int getNumDistincts() {
    return numDistincts;
  }

  /**
   * outputToVector() will concat individual entries, with given delimiter, for each rowgroup
   * and create final BaseVariableWidthVector as output.
   */
  @VisibleForTesting
  public void outputToVector(BaseVariableWidthVector outVector, final int targetStartIndex, final int numRecords) {
    final IntArrayList[] rowGroups = extractRowGroups();

    final ContentLengthChecker contentLengthChecker = new DelimitedContentLengthChecker(delimiterLength,
      actualMaxListAggSize - getOverflowReserveSpace(), false);
    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }

      contentLengthChecker.reset();
      Text t = new Text();
      boolean addDelimiter = false;
      for (int i = 0; i < rowGroups[rowGroup].size(); i++) {
        int index = rowGroups[rowGroup].getInt(i);
        byte[] bytes = get(index);


        if (!contentLengthChecker.hasSpaceFor(bytes.length) && i != 0) {
          setOverflow(rowGroup);
          break;
        }
        contentLengthChecker.addToTotalLength(bytes.length);
        if (addDelimiter) {
          t.append(delimiter.getBytes(), 0, delimiterLength);
        }
        t.append(bytes, 0, bytes.length);
        addDelimiter = true;
      }
      if (isOverflowSet(rowGroup)) {
        if (addDelimiter) {
          t.append(delimiter.getBytes(), 0, delimiterLength);
        }
        t.append(OVERFLOW_STRING.getBytes(), 0, OVERFLOW_STRING.getBytes().length);
      }
      outVector.set(targetStartIndex + rowGroup, t.getBytes(), 0, t.getLength());
    }
    outVector.setValueCount(targetStartIndex + numRecords);
  }

  /**
   * Started a new list, writes all values at indices in indicesToPick to the destination vector using
   * destinationVectorWriter, then ends the list
   */
  void writeRowGroupValues(final UnionListWriter destinationVectorWriter, final IntList indicesToPick) {
    destinationVectorWriter.startList();

    for (int i = 0; i < indicesToPick.size(); i++) {
      final int valueIndex = indicesToPick.getInt(i);
      final int start = getStartOffset(valueIndex);
      final int end = start + getValueLength(valueIndex);

      destinationVectorWriter.writeVarChar(start, end, getDataBuffer());
    }

    destinationVectorWriter.endList();
  }

  /**
   * Outputs the contents to a pre-allocated ListVector
   * The index of each list in the output ListVector corresponds to the row group the values belong to
   * The length of the output ListVector is likely to be greater than the number of row groups, nulls are used to
   * denote the missing row groups

   *
   * @param preallocatedVector the vector will be reset before being populated
   * @param targetStartIndex start index at which the records to append.
   */
  public int outputToListVector(final ListVector preallocatedVector, final int targetStartIndex, final int numRecords) {
    final IntArrayList[] rowGroups = extractRowGroups();
    final UnionListWriter writer = preallocatedVector.getWriter();
    int numValidGroups = 0;

    for (int rowGroup = 0; rowGroup < rowGroups.length; rowGroup++) {
      if (rowGroups[rowGroup] != null) {
        writer.setPosition(targetStartIndex + rowGroup);
        writeRowGroupValues(writer, rowGroups[rowGroup]);
        numValidGroups++;
      }
    }

    preallocatedVector.setValueCount(targetStartIndex + numRecords);
    return numValidGroups;
  }

  @VisibleForTesting
  public void orderBy(final IntArrayList[] rowGroups) {
    ++numOrderBys;
    orderByTimer.start();

    orderItemsInRowGroups(rowGroups);

    orderByTimer.stop();
  }

  public long getOrderByTime(final TimeUnit unit) {
    return orderByTimer.elapsed(unit);
  }

  public int getNumOrderBys() {
    return numOrderBys;
  }

  /**
   * Organizes indices to values under the same row groups, e.g.:
   *   0: 0, 2, 3
   *   1: 1, 5
   *   2: 4
   *   3: null
   */
  @VisibleForTesting
  public IntArrayList[] extractRowGroups() {
    final IntArrayList[] rowGroups = new IntArrayList[maxValuesPerBatch];

    for (int i = 0; i < size(); i++) {
      final int rowGroupIndex = getGroupIndex(i);

      // There's no guarantee the row groups are contiguous
      if (rowGroupIndex == DELETED_VALUE_ROW_GROUP) {
        continue;
      }

      if (rowGroups[rowGroupIndex] == null) {
        rowGroups[rowGroupIndex] = new IntArrayList();
      }

      rowGroups[rowGroupIndex].add(i);
    }

    return rowGroups;
  }

  /**
   * This method mutates the value indices within the row groups
   */
  @VisibleForTesting
  public void orderItemsInRowGroups(final IntArrayList[] rowGroups) {
    // Each row group gets sorted separately
    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }
      rowGroups[rowGroup].sort((itemAIndex, itemBIndex) -> {
        final int comparisonMultiplier = asc ? 1 : -1;
        final byte[] contentA = get(itemAIndex);
        final byte[] contentB = get(itemBIndex);

        return comparisonMultiplier * ComparisonUtils.compareByteArrays(contentA, contentB);
      });
    }
  }

  @VisibleForTesting
  public void limitSize(final IntArrayList[] rowGroups, final int nextRecSize) {
    ++numLimitSize;
    limitSizeTimer.start();

    deleteExcessItemsInGroups(rowGroups, nextRecSize);

    limitSizeTimer.stop();
  }

  public int getNumLimitSize() {
    return numLimitSize;
  }

  public Stopwatch getLimitSizeTimer() {
    return limitSizeTimer;
  }

  private void adjustMaxListAggSize(final IntArrayList[] rowGroups, final int reserveSpace) {
    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }

      /* Make sure we have space available for atleast one entry in the group. */
      int valueIndex = rowGroups[rowGroup].getInt(0);
      int valueLength = getValueLength(valueIndex);
      maxListAggSize = Math.max(maxListAggSize, valueLength + reserveSpace);
    }
  }

  /**
   * This method mutates the row groups and the values
   */
  @VisibleForTesting
  public void deleteExcessItemsInGroups(final IntArrayList[] rowGroups, final int nextRecSize) {
    int maxListAggReserveSpace = getOverflowReserveSpace();
    adjustMaxListAggSize(rowGroups, maxListAggReserveSpace);

    final ContentLengthChecker contentLengthChecker = new DelimitedContentLengthChecker(delimiterLength,
      maxListAggSize - maxListAggReserveSpace, true);

    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }
      contentLengthChecker.reset();

      IntListIterator iterator = rowGroups[rowGroup].iterator();
      int count = 0;
      boolean hasAnEntry = false;
      while (iterator.hasNext()) {
        final int valueIndex = iterator.next();
        final int valueLength = getValueLength(valueIndex);

        /*
         * DX-55423: With large maxListAggSize, the total values in single group
         * (especially when no group by), can be equal to the amount of valueCapacity.
         * In which case, there is no more space to add new entries and hasSpace()
         * will return false, which cause splice(). Since there is single group in this
         * vector, cannot splice. The solution is to limit the entries to valueCapacity - 1.
         *
         * This cause performance really suffer as it may cause compact() for each new entry however
         * no issues with correctness. maxListAggSize already set to max, so in general it is safe.
         */
        if (count >= valueCapacity - 1 || !contentLengthChecker.hasSpaceFor(valueLength)) {
          deleteValue(valueIndex);
          setOverflow(rowGroup);
          iterator.remove();
        } else {
          hasAnEntry = true;
        }

        // We want to add the content length even if we didn't have space for it
        // Otherwise we may end up in a situation where one value was too big to fit,
        // but the subsequent one wasn't, and we skipped a value from the middle
        contentLengthChecker.addToTotalLength(valueLength);
        count++;
      }
      /* Make sure each group has at least one entry */
      Preconditions.checkState(hasAnEntry);
    }
  }

  /**
   * Moves the values inside the vector according to the order by order, skips any deleted values
   */
  @VisibleForTesting
  public void physicallyRearrangeValues(final IntArrayList[] rowGroups) {
    // Making sure the temp space is empty
    tempSpace.reset();
    BaseVariableWidthVector dataVector = (BaseVariableWidthVector)tempSpace.getDataVector();

    // Copying data to the temp space first
    int tempDataWriteIndex = 0;
    for (int rowGroup = 0; rowGroup < rowGroups.length; ++rowGroup) {
      if (rowGroups[rowGroup] == null) {
        continue;
      }
      for (int oldValueIndex: rowGroups[rowGroup]) {
        dataVector.set(tempDataWriteIndex, get(oldValueIndex));

        tempDataWriteIndex++;
      }
    }

    // Now we can safely reset the values since all data has been copied out of it
    resetNoOverflowBuf();

    // Copying the data back
    int startOffset = 0;
    int tempDataReadIndex = 0;
    for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.length; rowGroupIndex++) {
      if (rowGroups[rowGroupIndex] == null) {
        continue;
      }
      for (int index: rowGroups[rowGroupIndex]) {
        final int valueLength = dataVector.getValueLength(tempDataReadIndex);

        addValueToRowGroupInternal(rowGroupIndex, startOffset, valueLength, dataVector.valueBuffer);

        startOffset += valueLength;
        tempDataReadIndex++;
      }
    }
  }

  public ArrowBuf[] getBuffers(int numRecords) {
    Preconditions.checkState(numRecords > 0);
    /* compact() apply all the rules and make read the vector to generate output */
    compact();

    tempSpace.reset();
    int numValidGroups = outputToListVector(tempSpace, 0, numRecords);
    Preconditions.checkState(tempSpace.getValueCount() == numRecords);

    ArrowBuf[] bufs = tempSpace.getBuffers(false);
    if (bufs.length != 5) {
      /* This is an extreme condition where every entry in the hashtable got dropped during accumulation as they are not valid */
      Preconditions.checkState(numValidGroups == 0);
      Preconditions.checkState(bufs.length == 2);
      ArrowBuf[] bufs1 = new ArrowBuf[5];
      bufs1[0] = bufs[0];
      bufs1[1] = bufs[1];
      bufs1[2] = allocator.getEmpty();
      bufs1[3] = allocator.getEmpty();
      bufs1[4] = allocator.getEmpty();
      bufs = bufs1;
    }
    return bufs;
  }

  public UserBitShared.SerializedField getSerializedField(int numRecords) {
    /*
     * HashAggPartitionWritableBatch.java:getNextWritableBatch() will call getBuffers()
     * followed by getSerializedField().
     * In the current context, the tempSpaceHolder already has the data saved,
     * especially the valueCount hence directly calling the TypeHelper.getMetadata()
     * is sufficient.
     */
    Preconditions.checkArgument(tempSpace.getValueCount() == numRecords);
    return TypeHelper.getMetadata(tempSpace);
  }

  public void loadBuffers(int valueCount, final ArrowBuf dataBuffer, final ArrowBuf validityBuffer) {
    // load validity buffers
    final int dataValSize = getDataBufValidityBufferSize(valueCount);
    super.validityBuffer = validityBuffer.slice(0, dataValSize);
    super.validityBuffer.writerIndex(dataValSize);
    super.validityBuffer.getReferenceManager().retain();

    final int overflowBufSize = getOverflowBufSize(valueCount);
    overflowBuf = validityBuffer.slice(dataValSize, overflowBufSize);
    overflowBuf.writerIndex(overflowBufSize);
    overflowBuf.getReferenceManager().retain();

    //load offset buffer
    final int offsetSize = getOffsetBufferSize(valueCount);
    super.offsetBuffer = dataBuffer.slice(0, offsetSize);
    super.offsetBuffer.writerIndex(offsetSize);
    super.offsetBuffer.getReferenceManager().retain();

    //load group index buffer
    final int indexBufSize = getGroupIndexBufferSize(valueCount);
    groupIndexBuf = dataBuffer.slice(offsetSize, indexBufSize);
    groupIndexBuf.writerIndex(indexBufSize);
    groupIndexBuf.getReferenceManager().retain();

    //load value data buffer
    final int dataBufOffset = offsetSize + indexBufSize;
    final int dataBufSize = (int)dataBuffer.capacity() - dataBufOffset;
    super.valueBuffer = dataBuffer.slice(dataBufOffset, dataBufSize);
    super.valueBuffer.writerIndex(dataBufSize);
    super.valueBuffer.getReferenceManager().retain();

    reset();

    this.valueCapacity = super.getValueCapacity();
  }

  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising of this and a target vector of
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

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator bufferAllocator) {
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

  private void moveValue(int srcIndex, int dstIndex, FixedListVarcharVector from, int groupIdx) {
    Preconditions.checkState(dstIndex == head);
    copyFrom(srcIndex, dstIndex, from);
    setGroupIndex(dstIndex, groupIdx);
    head++;
  }

  public void moveValuesAndFreeSpace(int srcStartGroupIdx, int dstStartGroupIdx, int numGroups, FieldVector accumulator) {
    Preconditions.checkState(dstStartGroupIdx == 0);
    FixedListVarcharVector flv = (FixedListVarcharVector)accumulator;

    int dstIndex = 0;
    for (int i = 0; i < head; i++) {
      int groupIdx = SV2UnsignedUtil.readAtIndex(groupIndexBuf, i);
      Preconditions.checkState(groupIdx != DELETED_VALUE_ROW_GROUP);
      if (groupIdx < srcStartGroupIdx) {
        continue;
      }

      Preconditions.checkState(groupIdx < srcStartGroupIdx + numGroups);
      flv.moveValue(i, dstIndex, this, groupIdx - srcStartGroupIdx);
      deleteValue(i);
      dstIndex++;
    }
    for (int i = srcStartGroupIdx; i < srcStartGroupIdx + numGroups; i++) {
      if (isOverflowSet(i)) {
        flv.setOverflow(i - srcStartGroupIdx);
        unsetOverflow(i);
      }
    }

    /* compact at end to free some space */
    if (dstIndex > 0) {
      compact();
    }
  }
}
