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
package com.dremio.sabot.op.join.vhash.spill.list;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * <p>This is akin to an off-heap LinkedListMultimap where the backing memory is a collection of equal-sized Pages
 * and the lists are populated backwards.
 *
 * <p>The more complete structure could be thought of as a LinkedListMultimap<int, element> where an element
 * has the following layout of <b>rr bbbb ee nnnn pppp</b> (16 bytes) where each item is defined as:
 * <li>rr - short recordIndex (index of record within the batch)
 * <li>bbbb - int batchId (index of the batch in the hyper-vector)
 * <li>ee - short empty
 * <li>nnnn - int next (index of next entry in the linked list)
 * <li>pppp - int key (in practice, it's ordinal in the hash-table)
 *
 * <p>The key is a positive integer. It should be dense from zero since the structure uses memory for all
 * values between the provided key and zero. It can be up to one less than the maximum positive value of an integer
 * since the maximum positive value is a sentinel value meaning no link.
 *
 * <p>This structure has three states:
 * <li>BUILD: A structure we can insert into (this is the initial state)
 * <li>READ A readable collection of lists. (Can only be entered from BUILD state)
 * <li>CLOSED - All memory released and no longer functional (can be entered from either other state)
 *
 * <p>The structure is built of the following items:
 * <li>A list of heads for each key in a set of pages.
 * <li>A shared list holding structure which contains elements for all lists
 *
 * <p>The lists are stored in reverse order so we don't have to traverse the list every time we add a new item to it.
 */
public class PageListMultimap implements AutoCloseable {

  enum State {BUILD, READ, CLOSED}

  // contains :
  // - start index into elements (4B)
  static final int HEAD_SIZE = 4;

  // contains :
  // - batchIndex & recordIndex within batch (8B)
  // - index of next into elements (4B)
  // - key (4B)
  static final int ELEMENT_SIZE = 16;
  static final int NEXT_OFFSET = 8;
  static final int NEXT_SIZE = 4;
  static final int KEY_SIZE = 4;

  static final int BATCH_OFFSET_SIZE = 2;
  static final int BATCH_INDEX_SIZE = 4;

  // means that the provided offset should be skipped.
  public static final int EMPTY = Integer.MAX_VALUE;

  private final int headsPerPage;
  private final int headShift;
  private final int headMask;

  private final int elementsPerPage;
  private final int elementShift;
  private final int elementMask;

  private final PagePool pool;

  private final List<Page> heads = new ArrayList<>();
  private final List<Page> elements = new ArrayList<>();
  private long[] headAddresses = new long[0];
  private long[] elementAddresses = new long[0];

  // the size of elements list
  private int totalListSize = 0;

  // the max key inserted into the list
  private int maxInsertedKey = 0;

  private State state = State.BUILD;

  public PageListMultimap(PagePool pool) {
    // we size things so the boundaries for both heads and elements are a power of
    // two. this wastes some space but allows bit operations to improve performance
    this.pool = pool;

    headsPerPage = pool.getPageSize() / HEAD_SIZE;
    headShift = shift(headsPerPage);
    headMask = mask(headsPerPage);

    elementsPerPage = pool.getPageSize() / ELEMENT_SIZE;
    elementShift = shift(elementsPerPage);
    elementMask = mask(elementsPerPage);
  }

  private static int shift(int count) {
    ensure2Power(count);
    return Integer.bitCount(count - 1);
  }

  private static int mask(int count) {
    ensure2Power(count);
    return count - 1;
  }

  private static void ensure2Power(int val) {
    if ((val & val - 1) != 0) {
      throw new IllegalArgumentException("Should be power of 2.");
    }
  }

  public static long getCarryAlongId(int batchId, int recordIndex) {
    Preconditions.checkArgument(recordIndex <= 0x0000ffff,
      "batch size should be <= 0x0000ffff");
    return (((long) batchId) << 16) | recordIndex;
  }

  public static int getBatchIdFromLong(long carryAlongId) {
    return ((int) (carryAlongId >> 16));
  }

  public static int getRecordIndexFromLong(long carryAlongId) {
    // truncate the least significant 16 bytes of data.
    return ((int) carryAlongId) & 0x0000FFFF;
  }

  /**
   * Compose a long that includes the key as well as the next
   * pointer.
   *
   * @param next
   *          The next item in the list or EMPTY if no more items.
   * @param key
   *          The key of this item.
   * @return The compounded data in little endian format. First four bytes are the next item,
   *         second four bytes are the back pointer.
   */
  private static long ptrs(int next, int key) {
    return (((long) key) << 32) | next;
  }

  private static int getNextFromPtrs(long ptrs) {
    return (int)(ptrs);
  }

  private static int getKeyFromPtrs(long ptrs) {
    return (int)(ptrs >> 32);
  }

  long[] getHeadAddresses() {
    return headAddresses;
  }

  long[] getElementAddresses() {
    return elementAddresses;
  }

  int getHeadShift() {
    return headShift;
  }

  int getHeadMask() {
    return headMask;
  }

  int getElementShift() {
    return elementShift;
  }

  int getElementMask() {
    return elementMask;
  }

  public void insert(int key, int batchId, int recordIndex) {
    insert(key, getCarryAlongId(batchId, recordIndex));
  }

  public void insert(int key, long carryAlongId) {
    Preconditions.checkArgument(state == State.BUILD);
    Preconditions.checkArgument(key < EMPTY && key >= 0);
    expandIfNecessary(1, key);
    insertElement(key, getHeadAddresses(), headShift, headMask, getElementAddresses(), elementShift, elementMask, totalListSize, carryAlongId);
    maxInsertedKey = Integer.max(maxInsertedKey, key);
    totalListSize++;
  }

  private void expandIfNecessary(int recordCount, int maximumKeyValue) {
    if (elements.size() * elementsPerPage >= totalListSize + recordCount &&
        heads.size() * headsPerPage >= maximumKeyValue + 1) {
      // fast-path
      return;
    }

    try (RollbackCloseable c = new RollbackCloseable()) {
      // add element page (16 bytes per element)
      List<Page> localElementPages = new ArrayList<>();
      while ((elements.size() + localElementPages.size()) * elementsPerPage < totalListSize + recordCount) {
        Page p = c.add(pool.newPage());
        localElementPages.add(p);

        // we don't need to initialize this memory since we always set the next pointer when inserting data.
      }

      // add head pointer page (4 bytes per pointer)
      List<Page> localHeadPages = new ArrayList<>();
      while ((heads.size() + localHeadPages.size()) * headsPerPage < maximumKeyValue + 1) {
        final Page p = c.add(pool.newPage());
        localHeadPages.add(p);
        long bufOffset = p.getAddress();
        final long maxBufOffset = bufOffset + headsPerPage * HEAD_SIZE;
        for(; bufOffset < maxBufOffset; bufOffset += HEAD_SIZE) {
          PlatformDependent.putInt(bufOffset, EMPTY);
        }
      }

      heads.addAll(localHeadPages);
      elements.addAll(localElementPages);
      c.commit();

      headAddresses = heads.stream().mapToLong(Page::getAddress).toArray();
      elementAddresses = elements.stream().mapToLong(Page::getAddress).toArray();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Insert a single element into a list.
   */
  private static void insertElement(
    final int key,
    final long[] headAddresses,
    final int headShift,
    final int headMask,
    final long[] elementAddresses,
    final int elementShift,
    final int elementMask,
    final int listIndex,
    final long carryAlongLocation) {
    // get the head corresponding to the key.
    final int headPage = key >>> headShift;
    final long headAddr = headAddresses[headPage] + (key & headMask) * HEAD_SIZE;

    // note, this could be a EMPTY value but that is ok
    // since it is a consistent value across both structures.
    final int oldHead = PlatformDependent.getInt(headAddr);
    // overwrite the head
    PlatformDependent.putInt(headAddr, listIndex);

    final int thisElementPage = listIndex >> elementShift;
    final int thisElementIndex = listIndex & elementMask;
    final long thisElementAddr = elementAddresses[thisElementPage] + thisElementIndex * ELEMENT_SIZE;

    PlatformDependent.putLong(thisElementAddr, carryAlongLocation);
    PlatformDependent.putLong(thisElementAddr + NEXT_OFFSET, ptrs(oldHead, key));
  }

  /**
   * Insert a collection of items for a given batchId. The list of offsets is mapped to a start = 0.correspond to the start
   * @param keys A collection of four byte keys
   * @param maxKeyValue The maximum value of the key
   * @param batchId The incoming batch id
   * @param startRecordIdx The index of the first record to be inserted.
   * @param numRecords The number of records to insert.
   */
  public void insertCollection(ArrowBuf keys, int maxKeyValue, int batchId, int startRecordIdx, int numRecords) {
    Preconditions.checkArgument(state == State.BUILD);
    Preconditions.checkArgument(maxKeyValue < EMPTY);
    // first, let's allocate another page if it is necessary.
    expandIfNecessary(numRecords, maxKeyValue);

    // localize references for performance
    final int headShift = this.headShift;
    final int headMask = this.headMask;
    final int elementShift = this.elementShift;
    final int elementMask = this.elementMask;
    final long[] headAddresses = getHeadAddresses();
    final long[] elementAddresses = getElementAddresses();

    final long lastKeyAddr = keys.memoryAddress() + (startRecordIdx + numRecords) * KEY_SIZE;

    int listIndex = totalListSize;
    int currentRecordIdx = startRecordIdx;
    for (long currentKeyAddr = keys.memoryAddress() + startRecordIdx * KEY_SIZE;
         currentKeyAddr < lastKeyAddr;
         currentKeyAddr += KEY_SIZE, listIndex++, currentRecordIdx++) {
      long carryAlongId = getCarryAlongId(batchId, currentRecordIdx);
      final int key = PlatformDependent.getInt(currentKeyAddr);
      Preconditions.checkArgument(key <= maxKeyValue && key >= 0);
      insertElement(key, headAddresses, headShift, headMask, elementAddresses, elementShift, elementMask, listIndex, carryAlongId);
    }

    maxInsertedKey = Integer.max(maxInsertedKey, maxKeyValue);
    totalListSize = listIndex;
  }

  public void moveToRead() {
    Preconditions.checkArgument(state == State.BUILD, "Must be in BUILD state to start reading. Currently in %s state.", state);
    state = State.READ;
  }

  /**
   * Iterator for all the elements for specified key.
   * @param key
   * @return stream of values, each value is long of batchIdx, recordIndex
   */
  @VisibleForTesting
  public LongStream getStreamForKey(int key) {
    Preconditions.checkArgument(state == State.READ, "Must be in READ state to read. Currently in a %s state.", state);
    Preconditions.checkArgument(key >= 0, "Key must be greater than or equal to zero.");
    if (key > maxInsertedKey) {
      throw new ArrayIndexOutOfBoundsException(String.format("Key requested %d greater than max inserted key %d.", key, maxInsertedKey));
    }
    final int head = PlatformDependent.getInt(headAddresses[key >>> headShift] + HEAD_SIZE * (key & headMask));
    final KeyIterator iterator = new KeyIterator(head);
    return StreamSupport.longStream(Spliterators.spliterator(iterator, 5, 0), false);
  }

  /**
   * Iterator for all the elements in the list.
   * @return stream of values
   */
  @VisibleForTesting
  public Stream<KeyAndCarryAlongId> getKeyAndCarryAlongIdStream() {
    Preconditions.checkArgument(state != State.CLOSED,
      "Must be in BUILD/READ state to iterate. Currently in a %s state.", state);

    return StreamSupport.stream(Spliterators.spliterator(new KeyAndCarryAlongIdIterator(), 5, 0), false);
  }

  private class KeyIterator implements PrimitiveIterator.OfLong {
    private int current;

    private KeyIterator(int current) {
      this.current = current;
    }

    @Override
    public boolean hasNext() {
      return current != EMPTY;
    }

    @Override
    public long nextLong() {
      Preconditions.checkArgument(current != EMPTY);
      final long address = elementAddresses[current >> elementShift] + ELEMENT_SIZE * (current & elementMask);
      final long val = PlatformDependent.getLong(address);
      current = getNextFromPtrs(PlatformDependent.getLong(address + NEXT_OFFSET));
      return val;
    }

  }

  private class KeyAndCarryAlongIdIterator implements Iterator<KeyAndCarryAlongId> {
    private int current = 0;

    @Override
    public boolean hasNext() {
      return current < totalListSize;
    }

    @Override
    public KeyAndCarryAlongId next() {
      Preconditions.checkArgument(current < totalListSize);
      final long address = elementAddresses[current >> elementShift] + ELEMENT_SIZE * (current & elementMask);
      final long val = PlatformDependent.getLong(address);
      final int key = getKeyFromPtrs(PlatformDependent.getLong(address + NEXT_OFFSET));
      ++current;
      return new KeyAndCarryAlongId(key, new CarryAlongId(val));
    }

  }

  @Override
  public void close() throws Exception {
    Preconditions.checkArgument(state != State.CLOSED, "Already closed.");
    List<Page> pages = new ArrayList<>();
    pages.addAll(elements);
    pages.addAll(heads);
    try {
      AutoCloseables.close(pages);
    } finally {
      state = State.CLOSED;
    }
  }

  /**
   * A class that can be used to understand the carry along value stored in this
   * map. Not used internally by the map since it would increase object overhead.
   */
  public static class CarryAlongId {
    private final long id;

    public CarryAlongId(int batchId, int recordIndex) {
      this(getCarryAlongId(batchId, recordIndex));
    }

    public CarryAlongId(long id) {
      this.id = id;
    }

    public int getBatchId() {
      return getBatchIdFromLong(id);
    }

    public int getRecordIndex() {
      return getRecordIndexFromLong(id);
    }

    @Override
    public int hashCode() {
      return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      CarryAlongId other = (CarryAlongId) obj;
      return id == other.id;
    }

    @Override
    public String toString() {
      return getBatchId() + ":" + getRecordIndex();
    }
  }

  /**
   * Pair of key and CarryAlongId
   */
  public static class KeyAndCarryAlongId {
    private final int key;
    private final CarryAlongId carryAlongId;

    KeyAndCarryAlongId(int key, CarryAlongId carryAlongId) {
      this.key = key;
      this.carryAlongId = carryAlongId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, carryAlongId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }

      KeyAndCarryAlongId other = (KeyAndCarryAlongId) obj;
      return key == other.key && this.carryAlongId.equals(other.carryAlongId);
    }

    @Override
    public String toString() {
      return "key " + key + " carryAlongId " + carryAlongId;
    }
  }
}
