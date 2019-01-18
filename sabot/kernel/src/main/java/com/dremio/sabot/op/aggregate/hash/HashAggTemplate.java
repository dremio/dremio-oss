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
package com.dremio.sabot.op.aggregate.hash;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ObjectVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.sig.RuntimeOverridden;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats;
import com.dremio.sabot.op.common.hashtable.ChainedHashTable;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.hashtable.HashTableConfig;
import com.dremio.sabot.op.common.hashtable.HashTableStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

public abstract class HashAggTemplate implements HashAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private static final int VARIABLE_WIDTH_VALUE_SIZE = 50;

  private static final boolean EXTRA_DEBUG_1 = false;

  private VectorAccessible incoming;
  private VectorContainer outContainer;
  private BufferAllocator allocator;

  private HashTable htable;
  private ArrayList<BatchHolder> batchHolders;
  private int numGroupByOutFields = 0; // Note: this should be <= number of group-by fields

  private Field[] materializedValueFields;

  private OperatorStats stats;
  private HashTableStats hashTableStats;
  private FunctionContext context;
  private final Constructor<BatchHolder> innerConstructor;

  public BatchHolder newBatchHolder() {
    try {
      return innerConstructor.newInstance(this);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  public HashAggTemplate(){
    try{
      Class<?> clazz = null;
      for(Class<?> c : getClass().getDeclaredClasses()){
        if(c.getSimpleName().equals("BatchHolder")){
          clazz = c;
          break;
        }
      }

      Preconditions.checkNotNull(clazz);
      this.innerConstructor = (Constructor<BatchHolder>) clazz.getConstructor(this.getClass());
      innerConstructor.setAccessible(true);
    }catch(Exception ex){
      throw Throwables.propagate(ex);
    }
  }

  public class BatchHolder implements AutoCloseable {

    private VectorContainer aggrValuesContainer; // container for aggr values (workspace variables)
    private int maxOccupiedIdx = -1;

    private int capacity = Character.MAX_VALUE;

    public BatchHolder() {

      this.aggrValuesContainer = new VectorContainer();
      boolean success = false;
      try {
        ValueVector vector;

        for (int i = 0; i < materializedValueFields.length; i++) {
          Field outputField = materializedValueFields[i];
          // Create a type-specific ValueVector for this value
          vector = TypeHelper.getNewVector(outputField, allocator);

          // Try to allocate space to store BATCH_SIZE records. Key stored at index i in HashTable has its workspace
          // variables (such as count, sum etc) stored at index i in HashAgg. HashTable and HashAgg both have
          // BatchHolders. Whenever a BatchHolder in HashAgg reaches its capacity, a new BatchHolder is added to
          // HashTable. If HashAgg can't store BATCH_SIZE records in a BatchHolder, it leaves empty slots in current
          // BatchHolder in HashTable, causing the HashTable to be space inefficient. So it is better to allocate space
          // to fit as close to as BATCH_SIZE records.
          if (vector instanceof FixedWidthVector) {
            ((FixedWidthVector) vector).allocateNew(HashTable.BATCH_SIZE);
          } else if (vector instanceof VariableWidthVector) {
            ((VariableWidthVector) vector).allocateNew(HashTable.VARIABLE_WIDTH_VECTOR_SIZE * HashTable.BATCH_SIZE,
                HashTable.BATCH_SIZE);
          } else if (vector instanceof ObjectVector) {
            ((ObjectVector) vector).allocateNew(HashTable.BATCH_SIZE);
          } else {
            vector.allocateNew();
          }

          capacity = Math.min(capacity, vector.getValueCapacity());

          aggrValuesContainer.add(vector);
        }
        success = true;
      } finally {
        if (!success) {
          aggrValuesContainer.close();
        }
      }
    }

    private void updateAggrValues(int incomingRowIdx, int idxWithinBatch) {
      updateAggrValuesInternal(incomingRowIdx, idxWithinBatch);
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
    }

    private void setup() {
      setupInterior(context, incoming, outContainer, aggrValuesContainer);
    }

    public int getRecordCount(){
      return maxOccupiedIdx + 1;
    }

    private void outputValues() {
      final int recordCount = maxOccupiedIdx + 1;
      for(int i = 0; i < recordCount; i++){
        outputRecordValues(i, i);
      }
    }

    public void close() {
      aggrValuesContainer.close();
    }

    // Code-generated methods (implemented in HashAggBatch)

    @RuntimeOverridden
    public void setupInterior(
        @Named("context") FunctionContext context,
        @Named("incoming") VectorAccessible incoming,
        @Named("outgoing") VectorAccessible outgoing,
        @Named("aggrValuesContainer") VectorContainer aggrValuesContainer) {
    }

    @RuntimeOverridden
    public void updateAggrValuesInternal(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) {
    }

    @RuntimeOverridden
    public void outputRecordValues(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) {
    }
  }

  @Override
  public int batchCount() {
    return batchHolders.size();
  }

  @Override
  public void setup(HashAggregate hashAggrConfig, HashTableConfig htConfig, ClassProducer producer,
      OperatorStats stats, BufferAllocator allocator, VectorAccessible incoming,
      LogicalExpression[] valueExprs, List<TypedFieldId> valueFieldIds, TypedFieldId[] groupByOutFieldIds,
      VectorContainer outContainer) throws SchemaChangeException, ClassTransformationException, IOException {

    if (valueExprs == null || valueFieldIds == null) {
      throw new IllegalArgumentException("Invalid aggr value exprs or workspace variables.");
    }
    if (valueFieldIds.size() < valueExprs.length) {
      throw new IllegalArgumentException("Wrong number of workspace variables.");
    }

    // currently, hash aggregation is only applicable if there are group-by expressions.
    // For non-grouped (a.k.a Plain) aggregations that don't involve DISTINCT, there is no
    // need to create hash table.  However, for plain aggregations with DISTINCT ..
    //      e.g SELECT COUNT(DISTINCT a1) FROM t1 ;
    // we need to build a hash table on the aggregation column a1.
    // TODO:  This functionality will be added later.
    if (hashAggrConfig.getGroupByExprs().size() == 0) {
      throw new IllegalArgumentException("Currently, hash aggregation is only applicable if there are group-by " +
          "expressions.");
    }

    this.hashTableStats = new HashTableStats();
    this.stats = stats;
    this.allocator = allocator;
    this.incoming = incoming;
    this.outContainer = outContainer;
    this.materializedValueFields = new Field[valueFieldIds.size()];

    if (valueFieldIds.size() > 0) {
      int i = 0;
      FieldReference ref =
          new FieldReference("dummy", valueFieldIds.get(0).getIntermediateType());
      for (TypedFieldId id : valueFieldIds) {
        if (id.getIntermediateType() == CompleteType.OBJECT) {
          materializedValueFields[i++] = new Field(ref.getAsNamePart().getName(), true, id.getIntermediateType().getType(), null);
        } else {
          materializedValueFields[i++] = new Field(ref.getAsNamePart().getName(), true, id.getIntermediateType().getType(), null);
        }
      }
    }

    // Get informed when the hash table adds a new batch.
    final HashTable.BatchAddedListener listener = new HashTable.BatchAddedListener(){
      public void batchAdded() {
        @SuppressWarnings("resource")
        final BatchHolder bh = newBatchHolder();
        batchHolders.add(bh);

        if (EXTRA_DEBUG_1) {
          logger.debug("HashAggregate: Added new batch; num batches = {}.", batchHolders.size());
        }

        bh.setup();
      }
    };

    context = producer.getFunctionContext();

    final ChainedHashTable ht =
        new ChainedHashTable(htConfig, producer, allocator, incoming, null /* no incoming probe */, outContainer, listener);
    this.batchHolders = new ArrayList<BatchHolder>();
    this.numGroupByOutFields = groupByOutFieldIds.length;
    this.htable = ht.createAndSetupHashTable(groupByOutFieldIds);

    doSetup(producer.getFunctionContext(), incoming);
  }



  @Override
  public void addBatch(int records) {

    // Check if a group is present in the hash table; if not, insert it in the hash table.
    // The htIdxHolder contains the index of the group in the hash table container; this same
    // index is also used for the aggregation values maintained by the hash aggregate.

    for(int i = 0; i < records; i++){

      int vectorIndex = getVectorIndex(i);

      // insert value into hash table.
      int currentIdx = htable.put(vectorIndex);

      // now add aggregations to batch holder.
      final BatchHolder bh = batchHolders.get((currentIdx >>> 16) & HashTable.BATCH_MASK);
      int idxWithinBatch = currentIdx & HashTable.BATCH_MASK;
      bh.updateAggrValues(vectorIndex, idxWithinBatch);
    }

    updateStats();
  }

  private void updateStats() {
    if (htable == null) {
      return;
    }
    final OperatorStats stats = this.stats;
    htable.getStats(hashTableStats);
    stats.setLongStat(HashAggStats.Metric.NUM_BUCKETS, hashTableStats.numBuckets);
    stats.setLongStat(HashAggStats.Metric.NUM_ENTRIES, hashTableStats.numEntries);
    stats.setLongStat(HashAggStats.Metric.NUM_RESIZING, hashTableStats.numResizing);
    stats.setLongStat(HashAggStats.Metric.RESIZING_TIME, hashTableStats.resizingTime);
  }

  private void allocateOutgoing(int records) {
    // Skip the keys and only allocate for outputting the workspace values
    // (keys will be output through splitAndTransfer)
    Iterator<VectorWrapper<?>> outgoingIter = outContainer.iterator();
    for (int i = 0; i < numGroupByOutFields; i++) {
      outgoingIter.next();
    }
    while (outgoingIter.hasNext()) {
      final ValueVector vv = outgoingIter.next().getValueVector();
      /*
       * In build schema we use the allocation model that specifies exact record count
       * so we need to stick with that allocation model until DRILL-2211 is resolved. Using
       * 50 as the average bytes per value as is used in HashTable.
       */
      AllocationHelper.allocatePrecomputedChildCount(vv, records, VARIABLE_WIDTH_VALUE_SIZE, 0);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Iterables.concat(Collections.singleton(htable), batchHolders));
  }

  @Override
  public int outputBatch(int batchIndex) {
    assert batchIndex <= batchHolders.size();

    final BatchHolder valueHolder = batchHolders.get(batchIndex);
    final int recordCount = valueHolder.getRecordCount();
    allocateOutgoing(recordCount);
    valueHolder.outputValues();
    htable.outputKeys(batchIndex, outContainer);

    // set the value count for outgoing batch value vectors
    for (VectorWrapper<?> v : outContainer) {
      v.getValueVector().setValueCount(recordCount);
    }

    return recordCount;
  }

  @Override
  public long numHashTableEntries() {
    return htable.size();
  }


  // Code-generated methods (implemented in HashAggBatch)
  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming);

  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);

  public abstract boolean resetValues();

}
