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
package com.dremio.sabot.op.sort.external;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.OutOfMemoryOrResourceExceptionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.common.memory.MemoryDebugInfo;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.dremio.sabot.op.spi.SingleInputOperator.State;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.calcite.rel.RelFieldCollation.Direction;

public class VectorSorter implements AutoCloseable {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VectorSorter.class);

  public static final int MAX_BATCHES_PER_HYPERBATCH = 65535;
  public static final int MAX_BATCHES_PER_MEMORY_RUN = 32768;

  private static final String PEAK_BATCHES_IN_MEMORY = "PEAK_BATCHES_IN_MEMORY";
  private static final String SPILL_COUNT = "SPILL_COUNT";
  private static final String MERGE_COUNT = "MERGE_COUNT";
  private static final String MAX_BATCH_SIZE = "MAX_BATCH_SIZE";
  private static final String AVG_BATCH_SIZE = "AVG_BATCH_SIZE";
  private static final String SPILL_TIME_NANOS = "SPILL_TIME_NANOS";
  private static final String MERGE_TIME_NANOS = "MERGE_TIME_NANOS";
  private static final String BATCHES_SPILLED = "BATCHES_SPILLED";
  private static final String UNCOMPRESSED_BYTES_READ = "UNCOMPRESSED_BYTES_READ";
  private static final String UNCOMPRESSED_BYTES_WRITTEN = "UNCOMPRESSED_BYTES_WRITTEN";
  private static final String IO_BYTES_READ = "IO_BYTES_READ";
  private static final String TOTAL_SPILLED_DATA_SIZE = "TOTAL_SPILLED_DATA_SIZE";
  private static final String IO_BYTES_WRITTEN = "IO_BYTES_WRITTEN";
  private static final String COMPRESSION_NANOS = "COMPRESSION_NANOS";
  private static final String DECOMPRESSION_NANOS = "DECOMPRESSION_NANOS";
  private static final String IO_READ_WAIT_NANOS = "IO_READ_WAIT_NANOS";
  private static final String IO_WRITE_WAIT_NANOS = "IO_WRITE_WAIT_NANOS";
  private static final String OOM_ALLOCATE_COUNT = "OOM_ALLOCATE_COUNT";
  private static final String OOM_COPY_COUNT = "OOM_COPY_COUNT";
  private static final String SPILL_COPY_NANOS = "SPILL_COPY_NANOS";
  private static final String SETUP_MILLIS = "SETUP_MILLIS";

  private final int targetBatchSize;
  private final OperatorContext context;
  private final BufferAllocator allocator;
  private final ClassProducer producer;
  private int operatorLocalId;

  private List<Ordering> sortOrderings;

  private VectorContainer output;
  private DiskRunManager diskRuns;
  private VectorAccessible incoming;
  private VectorContainer unconsumedRef;
  private MemoryRun memoryRun;
  private MovingCopier copier;
  private VectorSortTracer tracer;

  private int maxBatchesInMemory = 0;
  private int batchsizeMultiplier;
  private boolean enableSplaySort;
  private boolean enableMicroSpill;
  private State prevState;
  private SortState prevSortState;

  /**
   * Useful when micro-spilling is enabled. This flag indicates that spill was done due to
   * consumeData (and not due to an OOB Message).
   */
  private boolean consumePendingIncomingBatch;

  private boolean isSpillAllowed;

  private State state = State.NEEDS_SETUP;

  private SortState sortState = SortState.CONSUME;

  private Runnable spillNotifier;
  private boolean memoryStatsAvailable;

  private boolean diskStatsAvailable;
  private final Stopwatch setUpWatch = Stopwatch.createUnstarted();

  public enum SortState {
    CONSUME,
    CONSOLIDATE,
    COPY_FROM_MEMORY,

    COPY_FROM_DISK,
    SPILL_IN_PROGRESS, // spill from memoryRun to disk
    COPIER_SPILL_IN_PROGRESS // spill from copier to disk
  }

  public State getState() {
    return state;
  }

  public VectorSorter(
      OperatorContext context,
      Runnable spillNotifier,
      List<Ordering> sortOrderings,
      int operatorLocalId) {
    this(context);
    this.spillNotifier = spillNotifier;
    this.sortOrderings = sortOrderings;
    this.operatorLocalId = operatorLocalId;
  }

  public VectorSorter(OperatorContext context, List<Ordering> orderings) {
    this(context);
    this.sortOrderings = orderings;
    this.operatorLocalId = -1;
  }

  public VectorSorter(OperatorContext context) throws OutOfMemoryException {
    this.context = context;
    this.targetBatchSize = context.getTargetBatchSize();
    this.producer = context.getClassProducer();
    this.allocator = context.getAllocator();
  }

  public VectorAccessible setup(VectorAccessible incoming, boolean isSpillAllowed) {
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      setUpWatch.start();
      this.isSpillAllowed = isSpillAllowed;

      this.tracer = new VectorSortTracer();
      this.output = context.createOutputVectorContainer(incoming.getSchema());

      final OptionManager options = context.getOptions();
      this.batchsizeMultiplier =
          (int) options.getOption(ExecConstants.EXTERNAL_SORT_BATCHSIZE_MULTIPLIER);
      final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
      final int varFieldSizeEstimate =
          (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
      final boolean compressSpilledBatch =
          options.getOption(ExecConstants.EXTERNAL_SORT_COMPRESS_SPILL_FILES);
      this.enableSplaySort = options.getOption(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT);
      this.unconsumedRef = null;
      this.enableMicroSpill = options.getOption(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL);
      this.consumePendingIncomingBatch = false;
      this.prevState = null;
      this.prevSortState = null;

      this.memoryRun =
          new MemoryRun(
              sortOrderings,
              producer,
              context.getAllocator(),
              incoming.getSchema(),
              tracer,
              batchsizeMultiplier,
              enableSplaySort,
              targetBatchSize,
              context.getExecutionControls());
      rollback.add(this.memoryRun);

      this.incoming = incoming;
      state = State.CAN_CONSUME;

      // estimate how much memory the outgoing batch will take in memory
      final int estimatedRecordSize =
          incoming.getSchema().estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);
      final int targetBatchSizeInBytes = targetBatchSize * estimatedRecordSize;

      if (isSpillAllowed) {
        this.diskRuns =
            new DiskRunManager(
                context.getConfig(),
                context.getOptions(),
                targetBatchSize,
                targetBatchSizeInBytes,
                context.getFragmentHandle(),
                operatorLocalId,
                context.getClassProducer(),
                allocator,
                sortOrderings,
                incoming.getSchema(),
                compressSpilledBatch,
                tracer,
                context.getSpillService(),
                context.getStats(),
                context.getExecutionControls());
        rollback.add(this.diskRuns);
      } else {
        this.diskRuns = null;
      }

      tracer.setTargetBatchSize(targetBatchSize);
      tracer.setTargetBatchSizeInBytes(targetBatchSizeInBytes);

      rollback.commit();
      setUpWatch.stop();
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return output;
  }

  public void setIncoming(VectorAccessible incoming) {
    this.incoming = incoming;
  }

  @Override
  public void close() throws Exception {
    /**
     * 'diskRuns' holds a ref to a VectorContainer, which is created by 'memoryRun'. Thus,'diskRuns'
     * must be closed before 'memoryRun' so that all the buffers referred in the VectorContainer
     * etc. are released first. Otherwise 'memoryRun' close would fail reporting memory leak.
     */
    AutoCloseables.close(copier, output, diskRuns, memoryRun, unconsumedRef);
  }

  public void setStateToCanConsume() {
    this.state = State.CAN_CONSUME;
  }

  public void setStates(State masterState, SortState sortState) {
    this.state = masterState;
    this.sortState = sortState;
  }

  /** consume the incoming data and sort it. on spill-not-allowed case, record count is unused. */
  public void consumeData() throws Exception {
    consumeData(-1);
  }

  /** basic case of consume data. Consume incoming batches. */
  public void consumeData(int records) throws Exception {
    consumeDataCommon(records);
  }

  /**
   * Consumes the incoming record batch
   *
   * @param records the record count of the batch
   */
  private void consumeDataCommon(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    // when micro-spilling is in progress, we never consume any data.
    Preconditions.checkState(
        sortState != SortState.SPILL_IN_PROGRESS
            && sortState != SortState.COPIER_SPILL_IN_PROGRESS);

    boolean attempt = true;
    while (attempt) {
      boolean added = memoryRun.addBatch(incoming);
      if (added) {
        break;
      }
      attempt = handleBatchAdditionFailure(records);
    }
  }

  /**
   * Handles the failure to add a batch and decides if further attempts should be made.
   *
   * @param records the number of records attempted to add.
   * @return true if conditions have been altered in a way that justifies another attempt, false
   *     otherwise.
   */
  private boolean handleBatchAdditionFailure(int records) throws Exception {
    if (this.isSpillAllowed) {
      this.spillNotifier.run();
      if (!this.enableMicroSpill) {
        rotateRuns(); // Adjust conditions to possibly allow adding the batch on a subsequent
        // attempt.
        return true;
      } else {
        consumePendingIncomingBatch = true;
        transferIncomingBatch(records);
        startMicroSpilling(); // May adjust conditions but decides to not retry.
        return false;
      }
    } else {
      // Handling when spilling is not allowed.
      onSpillNotAllowed();
      return false;
    }
  }

  /**
   * handles failure of consumption to memory during Spill-not-allowed case: offload memory
   * immediately in prep for output sorting and writing to file.
   */
  private void onSpillNotAllowed() {
    sortState = SortState.COPY_FROM_MEMORY;
    state = State.CAN_PRODUCE;
  }

  /**
   * Alter the Operator and Sorter to a state where it's prepared to produce. This means closing and
   * consolidating outstanding data in memory to disk or copier.
   */
  public void noMoreToConsume() throws Exception {
    state = State.CAN_PRODUCE;

    if (!isSpillAllowed || diskRuns.isEmpty()) { // no spills

      copier = memoryRun.closeToCopier(output, targetBatchSize);
      sortState = SortState.COPY_FROM_MEMORY;

    } else { // some spills

      sortState = SortState.CONSOLIDATE;

      // spill remainders to only deal with disk runs
      if (!memoryRun.isEmpty()) {
        if (!enableMicroSpill) {
          try {
            memoryRun.closeToDisk(diskRuns);
          } catch (Exception ex) {
            throw UserException.dataWriteError(ex)
                .message("Failure while attempting to spill sort data to disk.")
                .build(logger);
          }
        } else {
          startMicroSpilling();
          return;
        }
      }
      // only need to deal with disk runs.
      consolidateIfNecessary();
    }
  }

  /**
   * @return the output vector (copier), this object which provides the sorted data
   */
  public VectorAccessible getOutputVector() {
    return output;
  }

  public long shrinkableMemory() {
    long shrinkableMemory = 0;

    if (isShrinkable()) {
      shrinkableMemory =
          allocator.getAllocatedMemory() - MemoryRun.INITIAL_COPY_ALLOCATOR_RESERVATION;
    }

    if (shrinkableMemory < 0) {
      return 0;
    }
    return shrinkableMemory;
  }

  public boolean isShrinkable() {
    /*
     * Shrink is currently possible in one of the following case:
     * 1. operator is in consume state with batches in memory (CAN_CONSUME)
     * 2. micro-spilling from memoryRun to disk is in progress. (CAN_PRODUCE + SPILL_IN_PROGRESS)
     * 3. micro-spilling from copier to disk is in progress (CAN_PRODUCE + COPIER_SPILL_IN_PROGRESS)
     * 4. in produce state with batches in copier (CAN_PRODUCE + COPY_FROM_MEMORY)
     */

    return (state == State.CAN_CONSUME
            && !memoryRun
                .isEmpty() // only when having in-memory records, sorter's memory is shrinkable.
        || (state == State.CAN_PRODUCE && sortState == SortState.SPILL_IN_PROGRESS)
        || (state == State.CAN_PRODUCE && sortState == SortState.COPIER_SPILL_IN_PROGRESS)
        || (state == State.CAN_PRODUCE && sortState == SortState.COPY_FROM_MEMORY));
  }

  public boolean shrinkMemory(long size) throws Exception {
    if (!isShrinkable()) {
      // shrinkable memory is reported as 0 in all other cases
      return true;
    }

    if (!enableMicroSpill) {
      rotateRuns();
      return true;
    }

    if ((state == State.CAN_PRODUCE && sortState == SortState.COPY_FROM_MEMORY)
        || (state == State.CAN_PRODUCE && sortState == SortState.COPIER_SPILL_IN_PROGRESS)) {

      if (sortState == SortState.COPY_FROM_MEMORY) {
        // sortState is COPY_FROM_MEMORY, records are moved to copier already.
        startMicroSpillingFromCopier();
      }

      boolean done = spillNextBatchFromCopier();
      if (done) {
        finishMicroSpillingFromCopier();
      }
      return done;
    }

    if (state == State.CAN_CONSUME
        || (state == State.CAN_PRODUCE && sortState == SortState.SPILL_IN_PROGRESS)) {

      if (state == State.CAN_CONSUME) {
        startMicroSpilling();
      }

      final boolean done = memoryRun.spillNextBatch(diskRuns);
      if (done) {
        finishMicroSpilling();
      }
      return done;
    }

    return false;
  }

  private void startMicroSpillingFromCopier() throws IOException {
    Preconditions.checkState(state == State.CAN_PRODUCE && sortState == SortState.COPY_FROM_MEMORY);
    prevState = state;
    prevSortState = sortState;
    state = State.CAN_PRODUCE;
    sortState = SortState.COPIER_SPILL_IN_PROGRESS;

    logger.info(
        "State transition from (state:{}, sortState:{}) to (state:{}, sortState:{})",
        prevState.name(),
        prevSortState.name(),
        state.name(),
        sortState.name());

    // disk manager will re-use output VectorContainer, and spill records from it to disk.
    diskRuns.startMicroSpillingFromCopier(copier, output);
  }

  private boolean spillNextBatchFromCopier() throws Exception {
    return diskRuns.spillNextBatchFromCopier(copier, targetBatchSize);
  }

  private void finishMicroSpillingFromCopier() throws Exception {
    Preconditions.checkState(
        prevState == State.CAN_PRODUCE && prevSortState == SortState.COPY_FROM_MEMORY);
    logger.info(
        "State transition from (state:{}, sortState:{}) to (state:{}, sortState:{})",
        state.name(),
        sortState.name(),
        State.CAN_PRODUCE,
        SortState.CONSOLIDATE);

    // all records are on disk, restore sortState to CONSOLIDATE
    state = State.CAN_PRODUCE;
    sortState = SortState.CONSOLIDATE;
    copier.close();
  }

  /**
   * Printing operator state for debug logs
   *
   * @return
   */
  public String getOperatorStateToPrint() {
    return this.state.name() + " " + this.sortState.name();
  }

  /**
   * Attempt to consolidate disk runs if necessary. If the diskRunManager indicates consolidation is
   * complete, create the copier and update the sort state to COPY_FROM_DISK
   */
  private void consolidateIfNecessary() {
    try {
      if (diskRuns.consolidateAsNecessary()) {
        copier = diskRuns.createCopier();
        sortState = SortState.COPY_FROM_DISK;
      }
    } catch (Exception ex) {
      throw UserException.dataReadError(ex)
          .message("Failure while attempting to read spill data from disk.")
          .build(logger);
    }
  }

  public boolean canCopy() {
    return (sortState == SortState.COPY_FROM_MEMORY || sortState == SortState.COPY_FROM_DISK);
  }

  /**
   * check sorter's state to verify if it's ready to copy. If not, consolidate the disk runs if
   * necessary.
   *
   * @return true if copy was unsuccessful, false otherwise
   */
  public boolean handleIfCannotCopy() {
    if (!canCopy()) {
      consolidateIfNecessary();
      return true;
    }
    return false;
  }

  /**
   * @return verdict of spill's state.
   */
  public boolean handleIfSpillInProgress() throws Exception {
    if (sortState == SortState.SPILL_IN_PROGRESS) {
      final boolean done = memoryRun.spillNextBatch(diskRuns);
      if (done) { // all batches spilled...
        finishMicroSpilling();
      }
      return true;
    }

    if (sortState == SortState.COPIER_SPILL_IN_PROGRESS) {
      final boolean done = diskRuns.spillNextBatchFromCopier(copier, targetBatchSize);
      if (done) {
        finishMicroSpillingFromCopier();
      }
      return true;
    }
    return false;
  }

  /**
   * send the data generated from memory or disk to the copier for sort.
   *
   * @return the total records inside the copier
   * @throws Exception
   */
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    int copied = copier.copy(targetBatchSize);
    if (copied == 0) {
      state = State.DONE;
      return 0;
    }

    if (sortState == SortState.COPY_FROM_DISK) {
      // need to use the copierAllocator for the copy, because the copierAllocator is the one that
      // reserves enough
      // memory to copy the data. This requires using an intermediate VectorContainer. Now, we need
      // to transfer the
      // the output data to the output VectorContainer
      diskRuns.transferOut(output, copied);
    }

    for (VectorWrapper<?> w : output) {
      w.getValueVector().setValueCount(copied);
    }
    output.setRecordCount(copied);
    return copied;
  }

  /**
   * build the vector sorter's metrics.
   *
   * @param closed
   * @return
   */
  public Map<String, Long> getStats(boolean closed) {
    Map<String, Long> stats = new HashMap<>();

    memoryStatsAvailable = memoryRun != null;
    diskStatsAvailable = diskRuns != null;
    if (memoryStatsAvailable && !closed) {
      stats.put(PEAK_BATCHES_IN_MEMORY, (long) maxBatchesInMemory);
    }

    if (diskStatsAvailable) {
      stats.put(SPILL_COUNT, (long) diskRuns.spillCount());
      stats.put(MERGE_COUNT, (long) diskRuns.mergeCount());
      stats.put(MAX_BATCH_SIZE, (long) diskRuns.getMaxBatchSize());
      stats.put(AVG_BATCH_SIZE, (long) diskRuns.getAvgMaxBatchSize());
      stats.put(SPILL_TIME_NANOS, diskRuns.spillTimeNanos());
      stats.put(MERGE_TIME_NANOS, diskRuns.mergeTimeNanos());
      stats.put(BATCHES_SPILLED, diskRuns.getBatchesSpilled());
      stats.put(UNCOMPRESSED_BYTES_READ, diskRuns.getAppReadBytes());
      stats.put(UNCOMPRESSED_BYTES_WRITTEN, diskRuns.getAppWriteBytes());
      stats.put(IO_BYTES_READ, diskRuns.getIOReadBytes());
      stats.put(TOTAL_SPILLED_DATA_SIZE, diskRuns.getIOWriteBytes());
      stats.put(
          IO_BYTES_WRITTEN,
          diskRuns.getIOWriteBytes() == 0
              ? diskRuns.getTotalDataSpilled()
              : diskRuns.getIOWriteBytes());
      stats.put(COMPRESSION_NANOS, diskRuns.getCompressionNanos());
      stats.put(DECOMPRESSION_NANOS, diskRuns.getDecompressionNanos());
      stats.put(IO_READ_WAIT_NANOS, diskRuns.getIOReadWait());
      stats.put(IO_WRITE_WAIT_NANOS, diskRuns.getIOWriteWait());
      stats.put(OOM_ALLOCATE_COUNT, diskRuns.getOOMAllocateCount());
      stats.put(OOM_COPY_COUNT, diskRuns.getOOMCopyCount());
      stats.put(SPILL_COPY_NANOS, diskRuns.getSpillCopyNanos());
    }
    stats.put(SETUP_MILLIS, setUpWatch.elapsed(TimeUnit.MILLISECONDS));
    return stats;
  }

  /**
   * A VectorSorter-exclusive stat updater.
   *
   * @param closed
   */
  public void updateStats(boolean closed) {
    if (!closed) {
      if (memoryRun != null) {
        maxBatchesInMemory = Math.max(maxBatchesInMemory, memoryRun.getNumberOfBatches());
      }
    }
  }

  /** checks if memory exists, thus if stats related to memory exist */
  public boolean memoryStatsAvailable() {
    return memoryStatsAvailable;
  }

  /** checks if disk exists, thus if stats related to disk exist */
  public boolean diskStatsAvailable() {
    return diskStatsAvailable;
  }

  public void rotateRuns() {
    if (memoryRun.isEmpty()) {
      final String message =
          "Memory failed due to not enough memory to sort even one batch of records.";
      tracer.setVectorSorterAllocatorState(allocator);
      throw tracer.prepareAndThrowException(new OutOfMemoryException(message), allocator, null);
    }

    try {
      memoryRun.closeToDisk(diskRuns);
      memoryRun =
          new MemoryRun(
              sortOrderings,
              producer,
              allocator,
              incoming.getSchema(),
              tracer,
              batchsizeMultiplier,
              enableSplaySort,
              targetBatchSize,
              context.getExecutionControls());
    } catch (Exception e) {
      throw UserException.dataWriteError(e)
          .message("Failure while attempting to spill sort data to disk.")
          .build(logger);
    }
  }

  public void startMicroSpilling() {
    if (memoryRun.isEmpty()) {
      final String message =
          "Memory failed due to not enough memory to sort even one batch of records.";
      tracer.setVectorSorterAllocatorState(allocator);
      throw tracer.prepareAndThrowException(new OutOfMemoryException(message), allocator, null);
    }

    try {
      // sorts the records & prepares the hypercontainer
      memoryRun.startMicroSpilling(diskRuns);
    } catch (Exception e) {
      UserException.Builder builder = UserException.memoryError(e);
      context.getNodeDebugContextProvider().addErrorOrigin(builder);
      if (ErrorHelper.isDirectMemoryException(e)) {
        String memoryDetails = null;
        if (e instanceof OutOfMemoryException) {
          throw builder
              .setAdditionalExceptionContext(
                  new OutOfMemoryOrResourceExceptionContext(
                      OutOfMemoryOrResourceExceptionContext.MemoryType.DIRECT_MEMORY,
                      "Failure while attempting to spill sort data to disk. "
                          + MemoryDebugInfo.getDetailsOnAllocationFailure(
                              (OutOfMemoryException) e, allocator)))
              .build(logger);
        } else {
          throw builder
              .setAdditionalExceptionContext(
                  new OutOfMemoryOrResourceExceptionContext(
                      OutOfMemoryOrResourceExceptionContext.MemoryType.HEAP_MEMORY, memoryDetails))
              .build(logger);
        }
      } else {
        throw UserException.resourceError(e).build(logger);
      }
    }

    transitionToMicroSpillState();
  }

  private void finishMicroSpilling() throws Exception {
    memoryRun =
        new MemoryRun(
            sortOrderings,
            producer,
            allocator,
            incoming.getSchema(),
            tracer,
            batchsizeMultiplier,
            enableSplaySort,
            targetBatchSize,
            context.getExecutionControls());

    if (consumePendingIncomingBatch) {
      Preconditions.checkState(this.unconsumedRef != null);
      // add the previous pending batch, it must not fail now.
      final boolean added = memoryRun.addBatch(unconsumedRef);
      if (!added) {
        final String message = "VectorSort: Failure adding single batch for sorter";
        throw tracer.prepareAndThrowException(
            new OutOfMemoryException(message), allocator, message);
      }
      consumePendingIncomingBatch = false;
      this.unconsumedRef.close();
      this.unconsumedRef = null;
    }

    restorePreviousState();
  }

  private void transitionToMicroSpillState() {
    prevState = state;
    prevSortState = sortState;

    Preconditions.checkState(sortState != SortState.SPILL_IN_PROGRESS);
    state = State.CAN_PRODUCE;
    sortState = SortState.SPILL_IN_PROGRESS;

    logger.debug(
        "State transition from (state:{}, sortState:{}) to (state:{}, sortState:{})",
        prevState.name(),
        prevSortState.name(),
        state.name(),
        sortState.name());
  }

  private void restorePreviousState() {
    logger.debug(
        "State transition from (state:{}, sortState:{}) to (state:{}, sortState:{})",
        state.name(),
        sortState.name(),
        prevState.name(),
        prevSortState.name());

    state = prevState;
    sortState = prevSortState;
  }

  private void transferIncomingBatch(final int records) {
    Preconditions.checkState(this.unconsumedRef == null);

    if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      unconsumedRef = new VectorContainerWithSV(null, incoming.getSelectionVector2().clone());
    } else {
      unconsumedRef = new VectorContainer();
    }
    final List<ValueVector> vectors = Lists.newArrayList();

    for (VectorWrapper<?> v : incoming) {
      TransferPair tp = v.getValueVector().getTransferPair(allocator);
      tp.transfer();
      vectors.add(tp.getTo());
    }

    unconsumedRef.addCollection(vectors);
    unconsumedRef.setRecordCount(records);
    unconsumedRef.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }

  public static void generateComparisons(
      ClassGenerator<?> g,
      VectorAccessible batch,
      Iterable<Ordering> orderings,
      ClassProducer producer)
      throws SchemaChangeException {

    final MappingSet mainMappingSet =
        new MappingSet(
            (String) null,
            null,
            ClassGenerator.DEFAULT_SCALAR_MAP,
            ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet leftMappingSet =
        new MappingSet(
            "leftIndex",
            null,
            ClassGenerator.DEFAULT_SCALAR_MAP,
            ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet rightMappingSet =
        new MappingSet(
            "rightIndex",
            null,
            ClassGenerator.DEFAULT_SCALAR_MAP,
            ClassGenerator.DEFAULT_SCALAR_MAP);
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
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right, producer);
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));
  }

  public boolean isMicroSpillEnabled() {
    return this.enableMicroSpill;
  }

  public int getRecordCountInMemory() {
    return memoryRun.getRecordLength();
  }
}
