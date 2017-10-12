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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;

import com.dremio.exec.ExecConstants;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * Primary algorithm:
 *
 * Multiple allocators:
 * - incoming data
 * - one allocator per batch
 * - treeVector
 * - outputCopier (always holds reservation at least as much as x) -
 *
 * Each record batch group that comes in is: - sorted independently - added to a
 * Splay tree of sv4 values (sv4) - The tree vector is resized as new batches
 * come in.
 *
 * We ensure that we always have a reservation of at least maxBatchSize. This
 * guarantees that we can spill.
 *
 * We spill to disk if any of the following conditions occurs: - OutOfMemory -
 * Can't add max batch size reservation - Hit 64k batches
 *
 * When we spill, we do batch copies (guaranteed successful since we previously
 * reserved memory to do the spills) to generate a new spill run. A DiskRun is
 * written to disk.
 *
 * Once spilled to disk, we keep track of each spilled run (including the
 * maximum batch size of that run).
 *
 * Once we complete a number of runs, we determine a merge plan to complete the
 * data merges. (For now, the merge plan is always a simple priority queue n-way
 * merge where n is the final number of disk runs.)
 *
 */
public class ExternalSortOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortOperator.class);

  private final int targetBatchSize;
  private final OperatorContext context;
  private final BufferAllocator allocator;
  private final ClassProducer producer;
  private final ExternalSort config;

  private VectorContainer output;
  private DiskRunManager diskRuns;
  private VectorAccessible incoming;
  private MemoryRun memoryRun;
  private MovingCopier copier;

  private int maxBatchesInMemory = 0;

  private State state = State.NEEDS_SETUP;

  private SortState sortState = SortState.CONSUME;

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    MERGE_COUNT,            // number of times spills were merged
    PEAK_BATCHES_IN_MEMORY, // maximum number of batches kept in memory
    MAX_BATCH_SIZE,
    AVG_BATCH_SIZE,
    SPILL_TIME_NANOS,       // time spent spilling to diskRuns while sorting
    MERGE_TIME_NANOS;       // time spent merging disk runs and spilling

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  private enum SortState {
    CONSUME,
    CONSOLIDATE,
    COPY_FROM_MEMORY,
    COPY_FROM_DISK
  }

  @Override
  public State getState(){
    return state;
  }

  public ExternalSortOperator(OperatorContext context, ExternalSort popConfig) throws OutOfMemoryException {
    this.context = context;
    this.targetBatchSize = context.getTargetBatchSize();
    this.config = popConfig;
    this.producer = context.getClassProducer();
    this.allocator = context.getAllocator();
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) {
    this.output =  VectorContainer.create(context.getAllocator(), incoming.getSchema());
    this.memoryRun = new MemoryRun(config, producer, context.getAllocator(), incoming.getSchema());
    this.incoming = incoming;
    state = State.CAN_CONSUME;


    // estimate how much memory the outgoing batch will take in memory
    final OptionManager options = context.getOptions();
    final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize = incoming.getSchema().estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);
    final int targetBatchSizeInBytes = targetBatchSize * estimatedRecordSize;
    final boolean compressSpilledBatch = context.getOptions().getOption(ExecConstants.EXTERNAL_SORT_COMPRESS_SPILL_FILES);

    this.diskRuns = new DiskRunManager(context.getConfig(), context.getOptions(), targetBatchSize, targetBatchSizeInBytes,
        context.getFragmentHandle(), config.getOperatorId(), context.getClassProducer(), allocator,
        config.getOrderings(), incoming.getSchema(), compressSpilledBatch);
    return output;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(copier, output, memoryRun, diskRuns);
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    while(true){
      boolean added = memoryRun.addBatch(incoming);
      if(!added){
        rotateRuns();
      }else{
        break;
      }
    }
    updateStats();
  }

  @Override
  public void noMoreToConsume() {
    state = State.CAN_PRODUCE;

    if(diskRuns.isEmpty()){ // no spills

      // we can return the existing (already sorted) data
      copier = memoryRun.closeToCopier(output, targetBatchSize);

      sortState = SortState.COPY_FROM_MEMORY;

    } else { // some spills

      sortState = SortState.CONSOLIDATE;

      // spill remainders to only deal with disk runs
      if (!memoryRun.isEmpty()) {
        try {
          memoryRun.closeToDisk(diskRuns);
        } catch (Exception ex) {
          throw UserException.dataWriteError(ex).message("Failure while attempting to spill sort data to disk.")
              .build(logger);
        }
      }

      // only need to deal with disk runs.
      consolidateIfNecessary();
    }
    updateStats();
  }

  /**
   * Attempt to consolidate disk runs if necessary. If the diskRunManager indicates consolidation is complete, create
   * the copier and update the sort state to COPY_FROM_DISK
   */
  private void consolidateIfNecessary() {
    try {
      if (diskRuns.consolidateAsNecessary()) {
        copier = diskRuns.createCopier();
        sortState = SortState.COPY_FROM_DISK;
      }
    } catch (IOException ex) {
      throw UserException.dataReadError(ex).message("Failure while attempting to read spill data from disk.")
        .build(logger);
    }
  }

  private boolean canCopy() {
    return sortState == SortState.COPY_FROM_MEMORY || sortState == SortState.COPY_FROM_DISK;
  }

  @Override
  public int outputData() {
    state.is(State.CAN_PRODUCE);
    if (!canCopy()) {
      consolidateIfNecessary();
      updateStats();
      return 0;
    }
    int copied = copier.copy(targetBatchSize);
    if (copied == 0) {
      state = State.DONE;
      return 0;
    }

    if (sortState == SortState.COPY_FROM_DISK) {
      // need to use the copierAllocator for the copy, because the copierAllocator is the one that reserves enough
      // memory to copy the data. This requires using an intermedate VectorContainer. Now, we need to transfer the
      // the output data to the output VectorContainer
      diskRuns.transferOut(output, copied);
    }

    for (VectorWrapper<?> w : output) {
      w.getValueVector().getMutator().setValueCount(copied);
    }
    output.setRecordCount(copied);
    return copied;
  }

  private void updateStats() {
    if (memoryRun != null) {
      maxBatchesInMemory = Math.max(maxBatchesInMemory, memoryRun.getNumberOfBatches());
    }

    OperatorStats stats = context.getStats();

    stats.setLongStat(Metric.PEAK_BATCHES_IN_MEMORY, maxBatchesInMemory);
    stats.setLongStat(Metric.SPILL_COUNT, diskRuns.spillCount());
    stats.setLongStat(Metric.MERGE_COUNT, diskRuns.mergeCount());
    stats.setLongStat(Metric.MAX_BATCH_SIZE, diskRuns.getMaxBatchSize());
    stats.setLongStat(Metric.AVG_BATCH_SIZE, diskRuns.getAvgMaxBatchSize());
    stats.setLongStat(Metric.SPILL_TIME_NANOS, diskRuns.spillTimeNanos());
    stats.setLongStat(Metric.MERGE_TIME_NANOS, diskRuns.mergeTimeNanos());
  }

  private void rotateRuns() {
    if(memoryRun.isEmpty()){
      throw UserException
        .memoryError()
        .message("Memory failed due to not enough memory to sort even one batch of records.")
        .build(logger);
    }

    try {
      memoryRun.closeToDisk(diskRuns);
      memoryRun = new MemoryRun(config, producer, allocator, incoming.getSchema());
    } catch (Exception e) {
      throw UserException.dataWriteError(e)
        .message("Failure while attempting to spill sort data to disk.")
        .build(logger);
    }
  }

//  @Override
//  public void reduceMemoryConsumption(long target) {
//    if(!memoryRun.isEmpty()){
//      rotateRuns();
//    }
//  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  static void generateComparisons(ClassGenerator<?> g, VectorAccessible batch, Iterable<Ordering> orderings, ClassProducer producer) throws SchemaChangeException {

    final MappingSet mainMappingSet = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet leftMappingSet = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet rightMappingSet = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    g.setMappingSet(mainMappingSet);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      final LogicalExpression expr = producer.materialize(od.getExpr(), batch);
      g.setMappingSet(leftMappingSet);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(rightMappingSet);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(mainMappingSet);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh = FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right, producer);
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));
  }

}
