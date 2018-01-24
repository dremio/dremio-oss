/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.common.hashtable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.ValueVector;
import com.dremio.common.AutoCloseables;
import com.dremio.exec.compile.sig.RuntimeOverridden;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

public abstract class HashTableTemplate implements HashTable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashTable.class);
  private static final boolean EXTRA_DEBUG = false;

  private static final int EMPTY_SLOT = -1;
  private static final int KEY_MATCHES = -2;

  private BatchAddedListener listener;

  // private final int MISSING_VALUE = 65544;

  // A hash 'bucket' consists of the start index to indicate start of a hash chain

  // Array of start indexes. start index is a global index across all batch holders
  private SimpleIntVector startIndices;

  // Array of batch holders..each batch holder can hold up to BATCH_SIZE entries
  private ArrayList<BatchHolder> batchHolders;

  // Size of the hash table in terms of number of buckets
  private int tableSize = 0;

  // Threshold after which we rehash; It must be the tableSize * loadFactor
  private int threshold;

  // Actual number of entries in the hash table
  private int numEntries = 0;

  // current available (free) slot globally across all batch holders
  private int freeIndex = 0;

  // Placeholder for the current index while probing the hash table
//  private IndexPointer currentIdxHolder;

  private BufferAllocator allocator;

  // The incoming build side record batch
  private VectorAccessible incomingBuild;

  // The incoming probe side record batch (may be null)
  private VectorAccessible incomingProbe;

  // The outgoing record batch
  private VectorAccessible outgoing;

  // Hash table configuration parameters
  private HashTableConfig htConfig;

  // The original container from which others may be cloned
  private VectorContainer htContainerOrig;

  private int numResizing = 0;

  private Stopwatch resizingTime = Stopwatch.createUnstarted();

  private FunctionContext context;

  private final Constructor<BatchHolder> innerConstructor;

  public BatchHolder newBatchHolder(int index) {
    try {
      return innerConstructor.newInstance(this, index);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  public HashTableTemplate(){
    try{
      this.innerConstructor = (Constructor<BatchHolder>) this.getClass().getDeclaredClasses()[0].getConstructor(this.getClass(), int.class);
      innerConstructor.setAccessible(true);
    }catch(Exception ex){
      throw Throwables.propagate(ex);
    }
  }

  // This class encapsulates the links, keys and values for up to BATCH_SIZE
  // *unique* records. Thus, suppose there are N incoming record batches, each
  // of size BATCH_SIZE..but they have M unique keys altogether, the number of
  // BatchHolders will be (M/BATCH_SIZE) + 1
  public class BatchHolder implements AutoCloseable {

    // Container of vectors to hold type-specific keys
    private final VectorContainer htContainer;

    // Array of 'link' values
    private final SimpleIntVector links;

    // Array of hash values - this is useful when resizing the hash table
    private final SimpleIntVector hashValues;

    private int maxOccupiedIdx = -1;
    private int batchIndex = 0;

    public BatchHolder(int idx) {

      this.batchIndex = idx;
      SimpleIntVector links = null;
      htContainer = new VectorContainer();
      boolean success = false;
      try {
        for (VectorWrapper<?> w : htContainerOrig) {
          ValueVector vv = TypeHelper.getNewVector(w.getField(), allocator);

          // Capacity for "hashValues" and "links" vectors is BATCH_SIZE records. It is better to allocate space for
          // "key" vectors to store as close to as BATCH_SIZE records. A new BatchHolder is created when either BATCH_SIZE
          // records are inserted or "key" vectors ran out of space. Allocating too less space for "key" vectors will
          // result in unused space in "hashValues" and "links" vectors in the BatchHolder. Also for each new
          // BatchHolder we create a SV4 vector of BATCH_SIZE in HashJoinHelper.
          if (vv instanceof FixedWidthVector) {
            ((FixedWidthVector) vv).allocateNew(BATCH_SIZE);
          } else if (vv instanceof VariableWidthVector) {
            ((VariableWidthVector) vv).allocateNew(VARIABLE_WIDTH_VECTOR_SIZE, BATCH_SIZE);
          } else {
            vv.allocateNew();
          }

          htContainer.add(vv);
        }

        // cache local links reference to support failed creation
        links = allocMetadataVector(HashTable.BATCH_SIZE, EMPTY_SLOT);
        this.links = links;

        this.hashValues = allocMetadataVector(HashTable.BATCH_SIZE, 0);

        success = true;
      } finally {
        if (!success) {
          htContainer.clear();
          if (links != null) {
            links.clear();
          }
        }
      }
    }

    protected void setup() {
      setupInterior(context, incomingBuild, incomingProbe, outgoing, htContainer);
    }

    // Check if the key at the currentIdx position in hash table matches the key
    // at the incomingRowIdx. if the key does not match, return the the index of the next link
    private int isKeyMatch (
        final int incomingRowIdx,
        final int currentIdx,
        final boolean isProbe) {

      final int currentIdxWithinBatch = currentIdx & BATCH_MASK;

      if(EXTRA_DEBUG){
        if (currentIdxWithinBatch >= HashTable.BATCH_SIZE) {
          logger.debug("Batch size = {}, incomingRowIdx = {}, currentIdxWithinBatch = {}.", HashTable.BATCH_SIZE,
              incomingRowIdx, currentIdxWithinBatch);
        }
      }

      assert (currentIdxWithinBatch < HashTable.BATCH_SIZE);
      assert (incomingRowIdx < HashTable.BATCH_SIZE);

      if (isProbe) {
        if(isKeyMatchInternalProbe(incomingRowIdx, currentIdxWithinBatch)){
          return KEY_MATCHES;
        };
      } else {
        if(isKeyMatchInternalBuild(incomingRowIdx, currentIdxWithinBatch)){
          return KEY_MATCHES;
        };
      }

      return links.get(currentIdxWithinBatch);

    }

    // Insert a new <key1, key2...keyN> entry coming from the incoming batch into the hash table
    // container at the specified index
    private void insertEntry(int incomingRowIdx, int currentIdx, int hashValue, BatchHolder lastEntryBatch, int lastEntryIdxWithinBatch) {
      int currentIdxWithinBatch = currentIdx & BATCH_MASK;

      setValue(incomingRowIdx, currentIdxWithinBatch);

      // the previous entry in this hash chain should now point to the entry in this currentIdx
      if (lastEntryBatch != null) {
        lastEntryBatch.links.setSafe(lastEntryIdxWithinBatch, currentIdx);
      }

      // since this is the last entry in the hash chain, the links array at position currentIdx
      // will point to a null (empty) slot
      links.setSafe(currentIdxWithinBatch, EMPTY_SLOT);
      hashValues.setSafe(currentIdxWithinBatch, hashValue);

      maxOccupiedIdx = Math.max(maxOccupiedIdx, currentIdxWithinBatch);

      if (EXTRA_DEBUG) {
        logger.debug("BatchHolder: inserted key at incomingRowIdx = {}, currentIdx = {}, hash value = {}.",
            incomingRowIdx, currentIdx, hashValue);
      }
    }

    private void rehash(final int numbuckets, final SimpleIntVector newStartIndices, final int batchStartIdx) {

      logger.debug("Rehashing entries within the batch: {}; batchStartIdx = {}, total numBuckets in hash table = {}.", batchIndex, batchStartIdx, numbuckets);

      int size = links.getValueCount();
      final SimpleIntVector newLinks = allocMetadataVector(size, EMPTY_SLOT);
      final SimpleIntVector newHashValues = allocMetadataVector(size, 0);

      for (int i = 0; i <= maxOccupiedIdx; i++) {
        int entryIdxWithinBatch = i;
        int entryIdx = entryIdxWithinBatch + batchStartIdx;
        int hash = hashValues.get(entryIdxWithinBatch); // get the already saved hash value
        int bucketIdx = getBucketIndex(hash, numbuckets);
        int newStartIdx = newStartIndices.get(bucketIdx);

        if (newStartIdx == EMPTY_SLOT) { // new bucket was empty
          newStartIndices.setSafe(bucketIdx, entryIdx); // update the start index to point to entry
          newLinks.setSafe(entryIdxWithinBatch, EMPTY_SLOT);
          newHashValues.setSafe(entryIdxWithinBatch, hash);

          if (EXTRA_DEBUG) {
            logger.debug("New bucket was empty. bucketIdx = {}, newStartIndices[ {} ] = {}, newLinks[ {} ] = {}, " +
                "hash value = {}.", bucketIdx, bucketIdx, newStartIndices.get(bucketIdx),
                entryIdxWithinBatch, newLinks.get(entryIdxWithinBatch),
                newHashValues.get(entryIdxWithinBatch));
          }

        } else {
          // follow the new table's hash chain until we encounter empty slot. Note that the hash chain could
          // traverse multiple batch holders, so make sure we are accessing the right batch holder.
          int idx = newStartIdx;
          int idxWithinBatch = 0;

          @SuppressWarnings("resource")
          BatchHolder bh = this;

          while (true) {
            if (idx != EMPTY_SLOT) {
              idxWithinBatch = idx & BATCH_MASK;
              int batchIdx = ((idx >>> 16) & BATCH_MASK);
              bh = batchHolders.get(batchIdx);
            }

            if (bh == this && newLinks.get(idxWithinBatch) == EMPTY_SLOT) {
              newLinks.setSafe(idxWithinBatch, entryIdx);
              newLinks.setSafe(entryIdxWithinBatch, EMPTY_SLOT);
              newHashValues.setSafe(entryIdxWithinBatch, hash);

              if (EXTRA_DEBUG) {
                logger.debug("Followed hash chain in new bucket. bucketIdx = {}, newLinks[ {} ] = {}, " +
                    "newLinks[ {} ] = {}, hash value = {}.", bucketIdx, idxWithinBatch,
                    newLinks.get(idxWithinBatch), entryIdxWithinBatch,
                    newLinks.get(entryIdxWithinBatch), newHashValues.get(entryIdxWithinBatch));
              }

              break;
            } else if (bh != this && bh.links.get(idxWithinBatch) == EMPTY_SLOT) {
              bh.links.setSafe(idxWithinBatch, entryIdx); // update the link in the other batch
              newLinks.setSafe(entryIdxWithinBatch, EMPTY_SLOT); // update the newLink entry in this
              // batch to mark end of the hash chain
              newHashValues.setSafe(entryIdxWithinBatch, hash);

              if (EXTRA_DEBUG) {
                logger.debug("Followed hash chain in new bucket. bucketIdx = {}, newLinks[ {} ] = {}, " +
                    "newLinks[ {} ] = {}, hash value = {}.", bucketIdx, idxWithinBatch,
                    newLinks.get(idxWithinBatch), entryIdxWithinBatch,
                    newLinks.get(entryIdxWithinBatch),
                    newHashValues.get(entryIdxWithinBatch));
              }

              break;
            }
            if (bh == this) {
              idx = newLinks.get(idxWithinBatch);
            } else {
              idx = bh.links.get(idxWithinBatch);
            }
          }

        }

      }

      links.clear();
      hashValues.clear();

      newLinks.transferTo(links);
      newHashValues.transferTo(hashValues);
    }

    private void outputKeys(VectorContainer outContainer) {

      /** for debugging
      BigIntVector vv0 = getValueVector(0);
      BigIntHolder holder = new BigIntHolder();
      */

      // set the value count for htContainer's value vectors before the transfer ..
      setValueCount();

      Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();

      for (VectorWrapper<?> sourceWrapper : htContainer) {
        ValueVector sourceVV = sourceWrapper.getValueVector();
        ValueVector targetVV = outgoingIter.next().getValueVector();
        TransferPair tp = sourceVV.makeTransferPair(targetVV);
        tp.transfer();
      }

/*
      logger.debug("Attempting to output keys for batch index: {} from index {} to maxOccupiedIndex {}.",
      this.batchIndex, 0, maxOccupiedIdx);
      for (int i = batchOutputCount; i <= maxOccupiedIdx; i++) {
        if (outputRecordKeys(i, batchOutputCount) ) {
          if (EXTRA_DEBUG) logger.debug("Outputting keys to output index: {}", batchOutputCount) ;

          // debugging
          // holder.value = vv0.getAccessor().get(i);
          // if (holder.value == 100018 || holder.value == 100021) {
          //  logger.debug("Outputting key = {} at index - {} to outgoing index = {}.", holder.value, i,
          //      batchOutputCount);
          // }

          batchOutputCount++;
        } else {
          return false;
        }
      }
 */
    }

    private void setValueCount() {
      for (VectorWrapper<?> vw : htContainer) {
        ValueVector vv = vw.getValueVector();
        vv.setValueCount(maxOccupiedIdx + 1);
      }
    }

    private void dump(int idx) {
      while (true) {
        int idxWithinBatch = idx & BATCH_MASK;
        if (idxWithinBatch == EMPTY_SLOT) {
          break;
        } else {
          logger.debug("links[ {} ] = {}, hashValues[ {} ] = {}.", idxWithinBatch,
              links.get(idxWithinBatch), idxWithinBatch, hashValues.get(idxWithinBatch));
          idx = links.get(idxWithinBatch);
        }
      }
    }

    public void close() throws Exception {
      AutoCloseables.close(Arrays.asList(htContainer, links, hashValues));
    }

    // Only used for internal debugging. Get the value vector at a particular index from the htContainer.
    // By default this assumes the VV is a BigIntVector.
    private ValueVector getValueVector(int index) {
      Object tmp = (htContainer).getValueAccessorById(BigIntVector.class, index).getValueVector();
      if (tmp != null) {
        BigIntVector vv0 = ((BigIntVector) tmp);
        return vv0;
      }
      return null;
    }

    // These methods will be code-generated

    @RuntimeOverridden
    protected void setupInterior(
        @Named("context") FunctionContext context,
        @Named("incomingBuild") VectorAccessible incomingBuild,
        @Named("incomingProbe") VectorAccessible incomingProbe,
        @Named("outgoing") VectorAccessible outgoing,
        @Named("htContainer") VectorContainer htContainer) {
    }

    @RuntimeOverridden
    protected boolean isKeyMatchInternalBuild(
        @Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {
      return false;
    }

    @RuntimeOverridden
    protected boolean isKeyMatchInternalProbe(
        @Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {
      return false;
    }

    @RuntimeOverridden
    protected void setValue(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {
    }

    @RuntimeOverridden
    protected void outputRecordKeys(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) {
    }

  } // class BatchHolder


  @Override
  public void setup(HashTableConfig htConfig, FunctionContext context, BufferAllocator allocator,
      VectorAccessible incomingBuild, VectorAccessible incomingProbe,
      VectorAccessible outgoing, VectorContainer htContainerOrig, BatchAddedListener listener) {
    this.listener = listener;
    float loadf = htConfig.getLoadFactor();
    int initialCap = htConfig.getInitialCapacity();

    if (loadf <= 0 || Float.isNaN(loadf)) {
      throw new IllegalArgumentException("Load factor must be a valid number greater than 0");
    }
    if (initialCap <= 0) {
      throw new IllegalArgumentException("The initial capacity must be greater than 0");
    }
    if (initialCap > MAXIMUM_CAPACITY) {
      throw new IllegalArgumentException("The initial capacity must be less than maximum capacity allowed");
    }

    if (htConfig.getKeyExprsBuild() == null || htConfig.getKeyExprsBuild().size() == 0) {
      throw new IllegalArgumentException("Hash table must have at least 1 key expression");
    }

    this.htConfig = htConfig;
    this.allocator = allocator;
    this.incomingBuild = incomingBuild;
    this.incomingProbe = incomingProbe;
    this.outgoing = outgoing;
    this.htContainerOrig = htContainerOrig;
    this.context = context;

    // round up the initial capacity to nearest highest power of 2
    tableSize = roundUpToPowerOf2(initialCap);
    if (tableSize > MAXIMUM_CAPACITY) {
      tableSize = MAXIMUM_CAPACITY;
    }

    threshold = (int) Math.ceil(tableSize * loadf);

    startIndices = allocMetadataVector(tableSize, EMPTY_SLOT);

    // Create the first batch holder
    batchHolders = new ArrayList<>();
    // First BatchHolder is created when the first put request is received.
    this.context = context;
    doSetup(context, incomingBuild, incomingProbe);

    addBatchHolder();
    freeIndex = 0;


  }

  private int numBuckets() {
    return startIndices.getValueCount();
  }

  public int size() {
    return numEntries;
  }

  public void getStats(HashTableStats stats) {
    assert stats != null;
    stats.numBuckets = numBuckets();
    stats.numEntries = numEntries;
    stats.numResizing = numResizing;
    stats.resizingTime = resizingTime.elapsed(TimeUnit.NANOSECONDS);
  }

  public boolean isEmpty() {
    return numEntries == 0;
  }

  public void close() throws Exception {
    List<AutoCloseable> closeables = new ArrayList<>();
    if (batchHolders != null) {
      for (BatchHolder bh : batchHolders) {
        closeables.add(bh);
      }
    }
    closeables.add(startIndices);
    AutoCloseables.close(closeables);
  }

  private static int getBucketIndex(int hash, int numBuckets) {
    return hash & (numBuckets - 1);
  }

  private static int roundUpToPowerOf2(int number) {
    int rounded = number >= MAXIMUM_CAPACITY
        ? MAXIMUM_CAPACITY
        : (rounded = Integer.highestOneBit(number)) != 0
        ? (Integer.bitCount(number) > 1) ? rounded << 1 : rounded
        : 1;

    return rounded;
  }

  public int put(int incomingRowIdx) {
    final int hash = getHashBuild(incomingRowIdx);
    final int i = getBucketIndex(hash, numBuckets());
    final int startIdx = startIndices.get(i);

    int currentIdx;
    int currentIdxWithinBatch;
    BatchHolder bh;
    BatchHolder lastEntryBatch = null;
    int lastEntryIdxWithinBatch = EMPTY_SLOT;

    if (startIdx == EMPTY_SLOT) {


      // this is the first entry in this bucket; find the first available slot in the
      // container of keys and values
      currentIdx = freeIndex++;
      addBatchIfNeeded(currentIdx);

      if (EXTRA_DEBUG) {
        logger.debug("Empty bucket index = {}. incomingRowIdx = {}; inserting new entry at currentIdx = {}.", i,
            incomingRowIdx, currentIdx);
      }

      insertEntry(incomingRowIdx, currentIdx, hash, lastEntryBatch, lastEntryIdxWithinBatch);
      // update the start index array
      startIndices.setSafe(getBucketIndex(hash, numBuckets()), currentIdx);

      return currentIdx;
    }

    currentIdx = startIdx;

    bh = batchHolders.get((currentIdx >>> 16) & BATCH_MASK);

    // if startIdx is non-empty, follow the hash chain links until we find a matching
    // key or reach the end of the chain
    while (true) {
      currentIdxWithinBatch = currentIdx & BATCH_MASK;

      final int keyMatch = bh.isKeyMatch(incomingRowIdx, currentIdx, false);

      switch(keyMatch){
      case KEY_MATCHES:
        return currentIdx;
      case EMPTY_SLOT:
        lastEntryBatch = bh;
        lastEntryIdxWithinBatch = currentIdxWithinBatch;

        // no match was found, so insert a new entry
        currentIdx = freeIndex++;
        addBatchIfNeeded(currentIdx);

        if (EXTRA_DEBUG) {
          logger.debug("No match was found for incomingRowIdx = {}; inserting new entry at currentIdx = {}.", incomingRowIdx, currentIdx);
        }

        insertEntry(incomingRowIdx, currentIdx, hash, lastEntryBatch, lastEntryIdxWithinBatch);
        return currentIdx;
      default: // link case
        currentIdx = keyMatch;
        // we didn't match but there is a link.
        bh = batchHolders.get((currentIdx >>> 16) & HashTable.BATCH_MASK);
        lastEntryBatch = bh;
      }
    }

  }

  private void insertEntry(int incomingRowIdx, int currentIdx, int hashValue, BatchHolder lastEntryBatch, int lastEntryIdx) {

    addBatchIfNeeded(currentIdx);

    BatchHolder bh = batchHolders.get((currentIdx >>> 16) & BATCH_MASK);

    bh.insertEntry(incomingRowIdx, currentIdx, hashValue, lastEntryBatch, lastEntryIdx);
    numEntries++;

      /* Resize hash table if needed and transfer the metadata
       * Resize only after inserting the current entry into the hash table
       * Otherwise our calculated lastEntryBatch and lastEntryIdx
       * becomes invalid after resize.
       */
    resizeAndRehashIfNeeded();
  }

  // Return -1 if key is not found in the hash table. Otherwise, return the global index of the key
  @SuppressWarnings("resource")
  @Override
  public int containsKey(int incomingRowIdx, boolean isProbe) {
    final int hash = isProbe ? getHashProbe(incomingRowIdx) : getHashBuild(incomingRowIdx);
    final int i = getBucketIndex(hash, numBuckets());

    int currentIdx = startIndices.get(i);

    if (currentIdx == EMPTY_SLOT) {
      return -1;
    }

    BatchHolder bh = batchHolders.get((currentIdx >>> 16) & BATCH_MASK);

    while (true) {
      final int keyMatch = bh.isKeyMatch(incomingRowIdx, currentIdx, isProbe);
      switch(keyMatch){
      case KEY_MATCHES:
        return currentIdx;

      case EMPTY_SLOT:
        return -1;

      default:
        currentIdx = keyMatch;
        bh = batchHolders.get((currentIdx >>> 16) & BATCH_MASK);
      }
    }
  }

  // Add a new BatchHolder to the list of batch holders if needed. This is based on the supplied
  // currentIdx; since each BatchHolder can hold up to BATCH_SIZE entries, if the currentIdx exceeds
  // the capacity, we will add a new BatchHolder.
  private void addBatchIfNeeded(int currentIdx) {
    int totalBatchSize = batchHolders.size() * BATCH_SIZE;

    if (currentIdx >= totalBatchSize) {
      addBatchHolder();
    }
  }

  private void addBatchHolder(){
    BatchHolder bh = newBatchHolder(batchHolders.size());
    final int batchCount = batchHolders.size();
    batchHolders.add(bh);
    bh.setup();
    freeIndex = batchCount * BATCH_SIZE + 1;
    listener.batchAdded();
    if (EXTRA_DEBUG) {
      logger.debug("HashTable: Added new batch. Num batches = {}.", batchHolders.size());
    }
  }

  // Resize the hash table if needed by creating a new one with double the number of buckets.
  // For each entry in the old hash table, re-hash it to the new table and update the metadata
  // in the new table.. the metadata consists of the startIndices, links and hashValues.
  // Note that the keys stored in the BatchHolders are not moved around.
  private void resizeAndRehashIfNeeded() {
    if (numEntries < threshold) {
      return;
    }

    resizingTime.start();

    if (EXTRA_DEBUG) {
      logger.debug("Hash table numEntries = {}, threshold = {}; resizing the table...", numEntries, threshold);
    }

    // If the table size is already MAXIMUM_CAPACITY, don't resize
    // the table, but set the threshold to Integer.MAX_VALUE such that
    // future attempts to resize will return immediately.
    if (tableSize == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    int newSize = 2 * tableSize;

    tableSize = roundUpToPowerOf2(newSize);
    if (tableSize > MAXIMUM_CAPACITY) {
      tableSize = MAXIMUM_CAPACITY;
    }

    // set the new threshold based on the new table size and load factor
    threshold = (int) Math.ceil(tableSize * htConfig.getLoadFactor());

    SimpleIntVector newStartIndices = allocMetadataVector(tableSize, EMPTY_SLOT);

    for (int i = 0; i < batchHolders.size(); i++) {
      BatchHolder bh = batchHolders.get(i);
      int batchStartIdx = i * BATCH_SIZE;
      bh.rehash(tableSize, newStartIndices, batchStartIdx);
    }

    startIndices.clear();
    startIndices = newStartIndices;

    if (EXTRA_DEBUG) {
      logger.debug("After resizing and rehashing, dumping the hash table...");
      logger.debug("Number of buckets = {}.", startIndices.getValueCount());
      for (int i = 0; i < startIndices.getValueCount(); i++) {
        logger.debug("Bucket: {}, startIdx[ {} ] = {}.", i, i, startIndices.get(i));
        int idx = startIndices.get(i);
        BatchHolder bh = batchHolders.get((idx >>> 16) & BATCH_MASK);
        bh.dump(idx);
      }
    }
    resizingTime.stop();
    numResizing++;
  }

  public void outputKeys(int batchIdx, VectorContainer outContainer) {
    assert batchIdx < batchHolders.size();
    batchHolders.get(batchIdx).outputKeys(outContainer);
  }

  private SimpleIntVector allocMetadataVector(int size, int initialValue) {
    SimpleIntVector vector = new SimpleIntVector("dummy", allocator);
    vector.allocateNew(size);
    for (int i = 0; i < size; i++) {
      vector.setSafe(i, initialValue);
    }
    vector.setValueCount(size);
    return vector;
  }

  // These methods will be code-generated in the context of the outer class
  protected abstract void doSetup(@Named("context") FunctionContext context, @Named("incomingBuild") VectorAccessible incomingBuild, @Named("incomingProbe") VectorAccessible incomingProbe);

  protected abstract int getHashBuild(@Named("incomingRowIdx") int incomingRowIdx);

  protected abstract int getHashProbe(@Named("incomingRowIdx") int incomingRowIdx);

}
