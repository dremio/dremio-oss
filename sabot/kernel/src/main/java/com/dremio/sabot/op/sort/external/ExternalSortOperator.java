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
package com.dremio.sabot.op.sort.external;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.ExtSortSpillNotificationMessage;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Throwables;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * Primary algorithm:
 *
 * Multiple allocators:
 * - incoming data
 * - one allocator per batch
 * - simpleIntVector or treeVector
 * - outputCopier (always holds reservation at least as much as x) -
 *
 * Each record batch group that comes in is:
 * - tracked in a QuickSorter and the whole set is sorted at the time a final list is requested
 *   (default mode)
 * - each batch is locally sorter, then added to a SplayTreeSorter of sv4 values (sv4), the
 *   SplayTree is traversed when the final list is requested (if SplaySort is enabled)
 * - (in either sort method case, the data-buffers used to track the row-indices in the batches
 *   are resized as new batches come in.)
 *
 * We ensure that we always have a reservation of at least maxBatchSize. This guarantees that we
 * can spill.
 *
 * We spill to disk if any of the following conditions occurs: - OutOfMemory or Can't add max
 * batch size reservation or Hit 64k batches
 *
 * When we spill, we do batch copies (guaranteed successful since we previously reserved memory
 * to do the spills) to generate a new spill run. A DiskRun is written to disk.
 *
 * Once spilled to disk, we keep track of each spilled run (including the maximum batch size of
 * that run).
 *
 * Once we complete a number of runs, we determine a merge plan to complete the data merges.
 * (For now, the merge plan is always a simple priority queue n-way merge where n is the final
 * number of disk runs.)
 *
 */
@Options
public class ExternalSortOperator implements SingleInputOperator {
  public static final BooleanValidator OOB_SORT_TRIGGER_ENABLED = new BooleanValidator("exec.operator.sort.oob_trigger_enabled", true);
  public static final DoubleValidator OOB_SORT_SPILL_TRIGGER_FACTOR = new RangeDoubleValidator("exec.operator.sort.oob_trigger_factor", 0.0d, 10.0d, .75d);
  public static final DoubleValidator OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR = new RangeDoubleValidator("exec.operator.sort.oob_trigger_headroom_factor", 0.0d, 10.0d, .2d);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortOperator.class);

  public static final int MAX_BATCHES_PER_HYPERBATCH = 65535;
  public static final int MAX_BATCHES_PER_MEMORY_RUN = 32768;

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
  private ExternalSortTracer tracer;

  private int maxBatchesInMemory = 0;
  private int batchsizeMultiplier;
  private boolean enableSplaySort;

  private int oobSends;
  private int oobReceives;
  private int oobDropLocal;
  private int oobDropWrongState;
  private int oobDropUnderThreshold;
  private int oobSpills;

  private State state = State.NEEDS_SETUP;

  private SortState sortState = SortState.CONSUME;

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    MERGE_COUNT,            // number of times spills were merged
    PEAK_BATCHES_IN_MEMORY, // maximum number of batches kept in memory
    MAX_BATCH_SIZE,         // maximum size of batch spilled amongst all batches spilled from all disk runs
    AVG_BATCH_SIZE,         // average size of batch spilled amongst all batches spilled from all disk runs
    SPILL_TIME_NANOS,       // time spent spilling to diskRuns while sorting
    MERGE_TIME_NANOS,       // time spent merging disk runs and spilling
    TOTAL_SPILLED_DATA_SIZE,  // total data spilled by sort operator

    // OOB related metrics
    OOB_SENDS, // Number of times operator informed others of spilling
    OOB_RECEIVES, // Number of times operator received a notification of spilling.
    OOB_DROP_LOCAL, // Number of times operator dropped self-referencing spilling notification
    OOB_DROP_WRONG_STATE, // Number of times operator dropped spilling notification as it was in wrong state to spill
    OOB_DROP_UNDER_THRESHOLD, // Number of times OOB dropped spilling notification as it was under the threshold.
    OOB_SPILL; // Spill was done due to oob.

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
    try(RollbackCloseable rollback = new RollbackCloseable()) {
      this.tracer = new ExternalSortTracer();
      this.output = context.createOutputVectorContainer(incoming.getSchema());

      final OptionManager options = context.getOptions();
      this.batchsizeMultiplier = (int) options.getOption(ExecConstants.EXTERNAL_SORT_BATCHSIZE_MULTIPLIER);
      final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
      final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
      final boolean compressSpilledBatch = options.getOption(ExecConstants.EXTERNAL_SORT_COMPRESS_SPILL_FILES);
      this.enableSplaySort = options.getOption(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT);

      this.memoryRun = new MemoryRun(config, producer, context.getAllocator(), incoming.getSchema(), tracer,
        batchsizeMultiplier, enableSplaySort);
      rollback.add(this.memoryRun);

      this.incoming = incoming;
      state = State.CAN_CONSUME;

      // estimate how much memory the outgoing batch will take in memory
      final int estimatedRecordSize = incoming.getSchema().estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);
      final int targetBatchSizeInBytes = targetBatchSize * estimatedRecordSize;

      this.diskRuns = new DiskRunManager(context.getConfig(), context.getOptions(), targetBatchSize, targetBatchSizeInBytes,
                                         context.getFragmentHandle(), config.getProps().getLocalOperatorId(), context.getClassProducer(), allocator,
                                         config.getOrderings(), incoming.getSchema(), compressSpilledBatch, tracer,
                                         context.getSpillService());
      rollback.add(this.diskRuns);

      tracer.setTargetBatchSize(targetBatchSize);
      tracer.setTargetBatchSizeInBytes(targetBatchSizeInBytes);

      rollback.commit();
    } catch(Exception e) {
      Throwables.propagate(e);
    }

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
        notifyOthersOfSpill();
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
    } catch (Exception ex) {
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
      w.getValueVector().setValueCount(copied);
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
    stats.setLongStat(Metric.TOTAL_SPILLED_DATA_SIZE, diskRuns.getTotalDataSpilled());

    stats.setLongStat(Metric.OOB_SENDS, oobSends);
    stats.setLongStat(Metric.OOB_RECEIVES, oobReceives);
    stats.setLongStat(Metric.OOB_DROP_LOCAL, oobDropLocal);
    stats.setLongStat(Metric.OOB_DROP_WRONG_STATE, oobDropWrongState);
    stats.setLongStat(Metric.OOB_DROP_UNDER_THRESHOLD, oobDropUnderThreshold);
    stats.setLongStat(Metric.OOB_SPILL, oobSpills);
  }

  private void rotateRuns() {
    if(memoryRun.isEmpty()){
      final String message = "Memory failed due to not enough memory to sort even one batch of records.";
      tracer.setExternalSortAllocatorState(allocator);
      throw tracer.prepareAndThrowException(new OutOfMemoryException(message), null);
    }

    try {
      memoryRun.closeToDisk(diskRuns);
      memoryRun = new MemoryRun(config, producer, allocator, incoming.getSchema(), tracer,
        batchsizeMultiplier, enableSplaySort);
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

  /**
   * When this operator starts spilling, notify others if the triggering is enabled.
   */
  private void notifyOthersOfSpill() {
    if(!context.getOptions().getOption(OOB_SORT_TRIGGER_ENABLED)) {
      return;
    }

    try {
      OutOfBandMessage.Payload payload = new OutOfBandMessage.Payload(ExtSortSpillNotificationMessage.newBuilder().setMemoryUse(allocator.getAllocatedMemory()).build());
      for(CoordExecRPC.FragmentAssignment a : context.getAssignments()) {
        OutOfBandMessage message = new OutOfBandMessage(
          context.getFragmentHandle().getQueryId(),
          context.getFragmentHandle().getMajorFragmentId(),
          a.getMinorFragmentIdList(),
          config.getProps().getOperatorId(),
          context.getFragmentHandle().getMinorFragmentId(),
          payload, true);

        NodeEndpoint endpoint = context.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      }
      oobSends++;
      updateStats();
      logger.debug("notifyOthersOfSpill allocated memory {}. headroom {} oobsends {} oobreceives {}", allocator.getAllocatedMemory(), allocator.getHeadroom(), oobSends, oobReceives);
    } catch(Exception ex) {
      logger.warn("Failure while attempting to notify others of spilling.", ex);
    }
  }

  /**
    * When a out of band message arrives, spill if we're within a factor of the other operator that is spilling.
    */
  @Override
  public void workOnOOB(OutOfBandMessage message) {

    ++oobReceives;

    //ignore self notification.
    if(message.getSendingMinorFragmentId() == context.getFragmentHandle().getMinorFragmentId()) {
      oobDropLocal++;
      return;
    }

    if (state != State.CAN_CONSUME) {
      oobDropWrongState++;
      return;
    }

    logger.debug("workOnOOB allocated memory {}. headroom {} oobsends {} oobreceives {}", allocator.getAllocatedMemory(), allocator.getHeadroom(), oobSends, oobReceives);

    // check to see if we're at the point where we want to spill.
    final ExtSortSpillNotificationMessage spill = message.getPayload(ExtSortSpillNotificationMessage.PARSER);
    final long allocatedMemoryBeforeSpilling = allocator.getAllocatedMemory();
    final double triggerFactor = context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_FACTOR);
    final double headroomRemaining = allocator.getHeadroom() * 1.0d / (allocator.getHeadroom() + allocator.getAllocatedMemory());
    if(allocatedMemoryBeforeSpilling < (spill.getMemoryUse() * triggerFactor) && headroomRemaining > context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR)) {
      logger.debug("Skipping OOB spill trigger, current allocation is {}, which is not within the current factor of the spilling operator ({}) which has memory use of {}. Headroom is at {} which is greater than trigger headroom of {}",
        allocatedMemoryBeforeSpilling, triggerFactor, spill.getMemoryUse(), headroomRemaining, context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR));
      oobDropUnderThreshold++;
      return;
    }

    rotateRuns();
    ++oobSpills;
    updateStats();
  }

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
