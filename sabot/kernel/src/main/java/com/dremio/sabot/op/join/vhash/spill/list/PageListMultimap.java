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
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PageListMultimap.class);

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
  static final int KEY_OFFSET = 12;
  public static final int KEY_SIZE = 4;

  public static final int BATCH_OFFSET_SIZE = 2;
  public static final int BATCH_INDEX_SIZE = 4;

  // Used to indicate end-of-list
  static final int TERMINAL = Integer.MAX_VALUE;

  // skip index 0, we use -ve to indicate an element has been visited.
  static final int BASE_ELEMENT_INDEX = 1;

  private final int headsPerPage;
  private final int headShift;
  private final int headMask;

  private final int elementsPerPage;
  private final int elementShift;
  private final int elementMask;

  private final PagePool pool;

  private final List<Page> heads = new ArrayList<>();
  private final List<Page> elements = new ArrayList<>();
  private ArrowBuf[] headBufs = new ArrowBuf[0];
  private ArrowBuf[] elementBufs = new ArrowBuf[0];
  private long[] headBufAddrs = new long[0];
  private long[] elementBufAddrs = new long[0];

  // the size of elements list
  private int totalListSize = BASE_ELEMENT_INDEX;

  // the max key inserted into the list
  private int maxInsertedKey = 0;

  private State state = State.BUILD;

  // mark all entries that were visited using the find() call.
  private boolean trackVisited = false;

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

  public PageListMultimap withTrackVisited() {
    Preconditions.checkState(state == State.BUILD);

    this.trackVisited = true;
    return this;
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

  static long getCarryAlongId(int batchId, int recordIndex) {
    Preconditions.checkArgument(recordIndex <= 0x0000ffff,
      "batch size should be <= 0x0000ffff");
    return (((long) batchId) << 16) | recordIndex;
  }

  public static int getBatchIdFromLong(long carryAlongId) {
    return ((int) (carryAlongId >> 16));
  }

  public static int getRecordIndexFromLong(long carryAlongId) {
    // truncate the least significant 16 bytes of data.
    return Short.toUnsignedInt((short)(carryAlongId & 0x0000FFFF));
  }

  ArrowBuf[] getHeadBufs() {
    return headBufs;
  }

  ArrowBuf[] getElementBufs() {
    return elementBufs;
  }

  long[] getHeadBufAddrs() {
    return headBufAddrs;
  }

  long[] getElementBufAddrs() {
    return elementBufAddrs;
  }

  int getBufSize() {
    return pool.getPageSize();
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
    Preconditions.checkArgument(key >= 0);
    expandIfNecessary(1, key);
    insertElement(key, getHeadBufs(), headShift, headMask, getElementBufs(), elementShift, elementMask, totalListSize, carryAlongId);
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
        final ArrowBuf buf = p.getBackingBuf();
        for(int bufOffset = 0; bufOffset < headsPerPage * HEAD_SIZE; bufOffset += HEAD_SIZE) {
          buf.setInt(bufOffset, TERMINAL);
        }
      }

      heads.addAll(localHeadPages);
      elements.addAll(localElementPages);
      c.commit();

      headBufs = heads.stream()
        .map(Page::getBackingBuf)
        .toArray(ArrowBuf[]::new);
      elementBufs = elements.stream()
        .map(Page::getBackingBuf)
        .toArray(ArrowBuf[]::new);
      headBufAddrs = heads.stream()
        .mapToLong(Page::getAddress)
        .toArray();
      elementBufAddrs = elements.stream()
        .mapToLong(Page::getAddress)
        .toArray();
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
    final ArrowBuf[] headBufs,
    final int headShift,
    final int headMask,
    final ArrowBuf[] elementBufs,
    final int elementShift,
    final int elementMask,
    final int listIndex,
    final long carryAlongLocation) {
    // get the head corresponding to the key.
    final int headPageIdx = key >>> headShift;
    final long headOffsetInPage = (key & headMask) * HEAD_SIZE;

    // note, this could be a EMPTY value but that is ok
    // since it is a consistent value across both structures.
    final int oldHead = headBufs[headPageIdx].getInt(headOffsetInPage);
    // overwrite the head
    headBufs[headPageIdx].setInt(headOffsetInPage, listIndex);

    final int thisElementPageIdx = listIndex >> elementShift;
    final long thisElementOffsetInPage = (listIndex & elementMask) * ELEMENT_SIZE;
    final ArrowBuf thisElementBuf = elementBufs[thisElementPageIdx];

    thisElementBuf.setLong(thisElementOffsetInPage, carryAlongLocation);
    thisElementBuf.setInt(thisElementOffsetInPage + NEXT_OFFSET, oldHead);
    thisElementBuf.setInt(thisElementOffsetInPage + KEY_OFFSET, key);

    logger.trace("insert key {} index {} next {}", key, listIndex, oldHead);
  }

  /**
   * Insert a collection of items for a given batchId.
   * @param keysIn buffer with collection of four byte keys
   * @param maxKeyValue The maximum value of the key
   * @param batchId The incoming batch id
   * @param numRecords The number of records to insert.
   */
  public void insertCollection(ArrowBuf keysIn, int maxKeyValue, int batchId, int numRecords) {
    Preconditions.checkArgument(state == State.BUILD);
    // first, let's allocate another page if it is necessary.
    expandIfNecessary(numRecords, maxKeyValue);

    // localize references for performance
    final int headShift = this.headShift;
    final int headMask = this.headMask;
    final int elementShift = this.elementShift;
    final int elementMask = this.elementMask;
    final ArrowBuf[] headBufs = getHeadBufs();
    final ArrowBuf[] elementBufs = getElementBufs();

    int listIndex = totalListSize;
    for (int currentRecordIdx = 0; currentRecordIdx < numRecords; listIndex++, currentRecordIdx++) {
      long carryAlongId = getCarryAlongId(batchId, currentRecordIdx);
      final int key = keysIn.getInt(currentRecordIdx * KEY_SIZE);
      Preconditions.checkArgument(key <= maxKeyValue && key >= 0);
      Preconditions.checkState(listIndex < TERMINAL);
      insertElement(key, headBufs, headShift, headMask, elementBufs, elementShift, elementMask, listIndex, carryAlongId);
    }

    maxInsertedKey = Integer.max(maxInsertedKey, maxKeyValue);
    totalListSize = listIndex;
  }

  public void moveToRead() {
    Preconditions.checkArgument(state == State.BUILD, "Must be in BUILD state to start reading. Currently in %s state.", state);
    state = State.READ;
  }

  int getTotalListSize() {
    return totalListSize;
  }

  /**
   * Iterator for all the elements for specified key.
   * @param key
   * @return stream of values, each value is long of batchIdx, recordIndex
   */
  @VisibleForTesting
  public LongStream find(int key) {
    Preconditions.checkArgument(state == State.READ, "Must be in READ state to read. Currently in a %s state.", state);
    Preconditions.checkArgument(key >= 0, "Key must be greater than or equal to zero.");
    if (key > maxInsertedKey) {
      throw new ArrayIndexOutOfBoundsException(String.format("Key requested %d greater than max inserted key %d.", key, maxInsertedKey));
    }
    final int head = headBufs[key >>> headShift].getInt(HEAD_SIZE * (key & headMask));
    final FindIterator iterator = new FindIterator(key, head);
    return StreamSupport.longStream(Spliterators.spliterator(iterator, 5, 0), false);
  }

  /**
   * Iterator for all the unvisited elements in the list.
   * @return stream of values
   */
  @VisibleForTesting
  public Stream<KeyAndCarryAlongId> findUnvisited() {
    Preconditions.checkArgument(state == State.READ, "Must be in READ state to read. Currently in a %s state.", state);
    Preconditions.checkState(trackVisited, "Must have trackVisited set to true");

    return StreamSupport.stream(Spliterators.spliterator(new FindUnvisitedIterator(), 5, 0), false);
  }

  /**
   * Array view for all the elements in the list.
   * @return array view
   */
  @VisibleForTesting
  public ArrayOfEntriesView findAll() {
    Preconditions.checkArgument(state != State.CLOSED,
      "Must be in BUILD/READ state to iterate. Currently in a %s state.", state);

    return new FindAllEntriesView();
  }

  private class FindIterator implements PrimitiveIterator.OfLong {
    private final int key;
    private int current;

    private FindIterator(int key, int current) {
      this.key = key;
      this.current = current;
    }

    @Override
    public boolean hasNext() {
      return current != TERMINAL;
    }

    @Override
    public long nextLong() {
      Preconditions.checkArgument(current >= BASE_ELEMENT_INDEX && current < totalListSize);
      final ArrowBuf currentElementBuf = elementBufs[current >> elementShift];
      final int offsetInPage = ELEMENT_SIZE * (current & elementMask);
      final long val = currentElementBuf.getLong(offsetInPage);
      logger.trace("find key {} index {}", key, current);

      current  = currentElementBuf.getInt(offsetInPage + NEXT_OFFSET);
      if (trackVisited) {
        if (current > 0) {
          // first visit, mark as visited by flipping the sign.
          currentElementBuf.setInt(offsetInPage + NEXT_OFFSET, -current);
        } else {
          // not the first visit, flip the sign of what we just read.
          current = -current;
        }
      }

      return val;
    }

  }

  private class FindAllEntriesView implements ArrayOfEntriesView {
    @Override
    public int getFirstValidIndex() {
      return BASE_ELEMENT_INDEX;
    }

    @Override
    public int size() {
      return totalListSize;
    }

    @Override
    public int getKey(int index) {
      Preconditions.checkArgument(isIndexValid(index));
      final ArrowBuf buf = elementBufs[index >> elementShift];
      return buf.getInt(ELEMENT_SIZE * (index & elementMask) + KEY_OFFSET);
    }

    @Override
    public long getCarryAlongId(int index) {
      Preconditions.checkArgument(isIndexValid(index));
      final ArrowBuf buf = elementBufs[index >> elementShift];
      return buf.getLong(ELEMENT_SIZE * (index & elementMask));
    }

    private boolean isIndexValid(int index) {
      return index >= BASE_ELEMENT_INDEX && index < totalListSize;
    }
  }

  private class FindUnvisitedIterator implements Iterator<KeyAndCarryAlongId> {
    private int current = BASE_ELEMENT_INDEX;
    private boolean initDone;

    @Override
    public boolean hasNext() {
      if (!initDone) {
        // can't do in constructor because the iterator may be created too early (before the find() calls).
        skipVisited();
        initDone = true;
      }
      return current < totalListSize;
    }

    @Override
    public KeyAndCarryAlongId next() {
      Preconditions.checkArgument(current >= BASE_ELEMENT_INDEX && current < totalListSize);
      final ArrowBuf currentElementBuf = elementBufs[current >> elementShift];
      final int offsetInPage = ELEMENT_SIZE * (current & elementMask);
      final long val = currentElementBuf.getLong(offsetInPage);
      final int key = currentElementBuf.getInt(offsetInPage + KEY_OFFSET);
      logger.trace("findUnvisited key {} index {}", key, current);

      ++current;
      skipVisited();
      return new KeyAndCarryAlongId(key, val);
    }

    private void skipVisited() {
      while (current < totalListSize) {
        final ArrowBuf currentElementBuf = elementBufs[current >> elementShift];
        final int offsetInPage = ELEMENT_SIZE * (current & elementMask);
        int next = currentElementBuf.getInt(offsetInPage + NEXT_OFFSET);
        if (next > 0) {
          // all visited entries will have their sign bit flipped to -ve value.
          break;
        }
        ++current;
      }
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

    long getId() {
      return id;
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
    private final long carryAlongId;

    KeyAndCarryAlongId(int key, long carryAlongId) {
      this.key = key;
      this.carryAlongId = carryAlongId;
    }

    public int getKey() {
      return key;
    }

    public long getCarryAlongId() {
      return carryAlongId;
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
      return key == other.key && carryAlongId == other.carryAlongId;
    }

    @Override
    public String toString() {
      return "key " + key + " carryAlongId " + new CarryAlongId(carryAlongId);
    }
  }
}
