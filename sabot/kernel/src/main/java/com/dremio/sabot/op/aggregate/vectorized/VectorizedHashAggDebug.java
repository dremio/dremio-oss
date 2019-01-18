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
package com.dremio.sabot.op.aggregate.vectorized;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Joiner;

class VectorizedHashAggDebug {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashAggDebug.class);

  private final boolean detailedEventTracing;

  /* information collected upfront */
  private long aggAllocatorInitReservation;
  private long aggAllocatorLimit;
  private int hashTableBatchSize;
  private int maxVarBlockLength;
  private int averageVarColumnFieldLength;
  private int numVarColumns;
  private int blockWidth;
  private int minHashTableSize;
  private int numPartitions;
  private int minHashTableSizePerPartition;
  private long allocatedMemoryBeforeInit;

  /* information collected after doing setup() in operator */
  private long preallocatedMemoryForPartitions;
  private long preallocatedMemoryForReadingSpilledData;
  private long preallocatedMemoryForAuxStructures;
  private long totalPreallocatedMemory;
  private String aggSchema;

  /* Out Of Memory Events */
  private int eventIndex = 0;
  private int eventCount = 0;
  private final Object[] events;
  /*
   * keeping an extremely large number of events will anyway be
   * less useful for debugging. So just impose a limit on the
   * number of OOM events we will capture.
   */
  private final int maxEvents;

  VectorizedHashAggDebug(final boolean detailedEventTracing,
                         final int maxEvents) {
    this.detailedEventTracing = detailedEventTracing;
    this.maxEvents = maxEvents;
    this.events = new Object[maxEvents];
  }

  UserException prepareAndThrowException(final Exception ex, final String message,
                                         final HashAggErrorType errorType) {
    final UserException.Builder exBuilder = getExceptionBuilder(ex, errorType);
    if (errorType == HashAggErrorType.OOM) {
      exBuilder.addContext(VectorizedHashAggOperator.OUT_OF_MEMORY_MSG);
    }
    if (message != null) {
      exBuilder.addContext(message);
    }
    /* include bare minimum information we collected upfront */
    exBuilder.addContext("AggAllocatorInitReservation", aggAllocatorInitReservation);
    exBuilder.addContext("AggAllocatorLimit", aggAllocatorLimit);
    exBuilder.addContext("AllocatedMemoryBeforeInit", allocatedMemoryBeforeInit);
    exBuilder.addContext("PreallocatedForPartitions", preallocatedMemoryForPartitions);
    exBuilder.addContext("PreallocatedForReadingSpilledData", preallocatedMemoryForReadingSpilledData);
    exBuilder.addContext("PreallocatedForAux", preallocatedMemoryForAuxStructures);
    exBuilder.addContext("TotalPreallocated", totalPreallocatedMemory);
    exBuilder.addContext("HashTable BatchSize", hashTableBatchSize);
    exBuilder.addContext("MaxVarBlockLength", maxVarBlockLength);
    exBuilder.addContext("AverageVarWidthFieldLength", averageVarColumnFieldLength);
    exBuilder.addContext("NumVarColumns", numVarColumns);
    exBuilder.addContext("BlockWidth", blockWidth);
    exBuilder.addContext("MinHashTableSize", minHashTableSize);
    exBuilder.addContext("NumPartitions", numPartitions);
    exBuilder.addContext("MinHashTableSizePerPartition", minHashTableSizePerPartition);
    exBuilder.addContext("AggSchema", aggSchema);
    if (detailedEventTracing) {
      exBuilder.addContext("OOM Events", Joiner.on("\n").join(events));
    }

    return exBuilder.build(logger);
  }

  public long getTotalPreallocatedMemory() {
    return totalPreallocatedMemory;
  }

  enum HashAggErrorType {
    SPILL_READ,
    SPILL_WRITE,
    OOM
  }

  private UserException.Builder getExceptionBuilder(final Exception ex, final HashAggErrorType errorType) {
    switch (errorType) {
      case SPILL_READ:
        return (ex == null) ? (UserException.dataReadError()) : (UserException.dataReadError(ex));
      case SPILL_WRITE:
        return (ex == null) ? (UserException.dataWriteError()) : (UserException.dataWriteError(ex));
      case OOM:
        return (ex == null) ? (UserException.memoryError()) : (UserException.memoryError(ex));
      default:
        /* should never be hit since VectorizedHashAggOperator controls the operator type */
        return null;
    }
  }

  void setInfoBeforeInit(final long aggAllocatorInitReservation,
                         final long aggAllocatorLimit,
                         final int batchSize,
                         final int maxVarBlockLength,
                         final int averageVarWidthFieldSize,
                         final int numVarColumns,
                         final int blockWidth,
                         final int minHashTableSize,
                         final int numPartitions,
                         final int minHashTableSizePerPartition) {
    this.aggAllocatorInitReservation = aggAllocatorInitReservation;
    this.aggAllocatorLimit = aggAllocatorLimit;
    this.hashTableBatchSize = batchSize;
    this.maxVarBlockLength = maxVarBlockLength;
    this.averageVarColumnFieldLength = averageVarWidthFieldSize;
    this.numVarColumns = numVarColumns;
    this.blockWidth = blockWidth;
    this.minHashTableSize = minHashTableSize;
    this.numPartitions = numPartitions;
    this.minHashTableSizePerPartition = minHashTableSizePerPartition;
  }

  void setAllocatedMemoryBeforeInit(final long amount) {
    this.allocatedMemoryBeforeInit = amount;
  }

  void setPreallocatedMemoryForPartitions(final long amount) {
    this.preallocatedMemoryForPartitions = amount;
  }

  void setPreallocatedMemoryForReadingSpilledData(final long amount) {
    this.preallocatedMemoryForReadingSpilledData = amount;
  }

  void setPreallocatedMemoryForAuxStructures(final long amount) {
    this.preallocatedMemoryForAuxStructures = amount;
  }

  void setInfoAfterInit(final long totalPreallocatedMemory,
                        final BatchSchema outgoing) {
    this.totalPreallocatedMemory = totalPreallocatedMemory;
    /* BatchSchema.toString() is an expensive operation so don't do it by default */
    this.aggSchema = (detailedEventTracing ? outgoing.toString() : "enable tracing to record schema");
  }

  void recordOOMEvent(final int iterations,
                      final int ooms,
                      final long currentAllocatedMemory,
                      final VectorizedHashAggPartition[] partitions,
                      final VectorizedHashAggPartitionSpillHandler spillHandler) {
    logger.debug("Attempting to capture OOM event, current total events:{}, iteration:{}, ooms:{}, spills:{}, currentAllocatedMemory:{}",
                 eventCount, iterations, ooms, spillHandler.getNumberOfSpills(), currentAllocatedMemory);
    /* recording OOM event is likely to be an expensive operation and comes on the critical
     * path of operator as it is processing data and hits OOM (and then subsequently handles
     * via spill). So detailed event recording is not turned on by default.
     */
    if (!detailedEventTracing) {
      return;
    }
    /*
     * We have seen some cases in large datasets when we run out of memory
     * plenty of times (e.g sample dataset of 100million rows and we spilled 15000 times),
     * the heap memory associated with data structures to capture OOM events becomes
     * significant. The heap usage keeps increasing with each OOM event. The impact on JVM's
     * GC process is that in each GC cycle, it is not able to get rid of significant amount of memory
     * and thus spends a lot of time doing GC activity without recovering much memory in
     * GC cycle. When this happens often, JVM detects it and throws OutOfMemoryError: GC overhead
     * limit exceeded error.
     *
     * We have tried to run such queries by completely disabling collection of OOM events
     * and the queries complete successfully even if they spilled a large number of times (10K, 15K etc)
     * simply because there is absolutely no overhead for recording OOM events. Such queries are
     * 3x faster when compared to the execution with full recording of OOM events.
     */
    try {
      final OOMEvent event = new OOMEvent(iterations, ooms, currentAllocatedMemory, partitions, spillHandler);
      events[eventIndex] = event;
      eventIndex = (eventIndex + 1)%maxEvents;
      eventCount++; /* total absolute count of events */
    } catch (OutOfMemoryError oe) {
      /* ran out of heap memory in JVM, don't record the event.
       * remove() and clear() don't free memory since Java doesn't allow to explicitly free
       * heap memory. The reference to event object is no longer used by us
       * and is therefore candidate for garbage collection. So hopefully in the next
       * GC cycle, JVM will be happy with the amount of heap memory recovered.
       */
      for (int i = 0; i < maxEvents; i++) {
        events[i] = null;
      }
      eventIndex = 0;
    }
  }

  private static class SpilledPartitionState {
    private final String partitionIdentifier;
    private final int batchesSpilled;
    private final String spillFilePath;
    private final boolean activeSpilled;

    SpilledPartitionState(final String id,
                          final int batches,
                          final String spillFile,
                          final boolean activeSpilled) {
      this.partitionIdentifier = id;
      this.batchesSpilled = batches;
      this.spillFilePath = spillFile;
      this.activeSpilled =  activeSpilled;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(" SpilledPartition: ").append(partitionIdentifier);
      sb.append(" BatchesSpilled: ").append(batchesSpilled);
      sb.append(" SpillFile: ").append(spillFilePath);
      sb.append(" IsActive: ").append(String.valueOf(activeSpilled)).append("\n");
      return sb.toString();
    }
  }

  private static class OOMEvent {
    private final int iterations;
    private final int spills;
    private final int ooms;
    private final long allocatedMemory;
    private final int maxBatchesSpilled;
    private final int totalBatchesSpilled;
    private final int maxRecordsSpilled;
    private final int totalRecordsSpilled;
    private final SpilledPartitionState[] activeSpilled;
    private final SpilledPartitionState[] onDiskOnly;

    OOMEvent(final int iterations,
             final int ooms,
             final long currentAllocatedMemory, /* allocated memory as of the time this OOM happened */
             final VectorizedHashAggPartition[] partitions,
             final VectorizedHashAggPartitionSpillHandler spillHandler) {
      this.iterations = iterations;
      this.spills = spillHandler.getNumberOfSpills();
      this.ooms = ooms;
      this.allocatedMemory = currentAllocatedMemory;
      this.maxBatchesSpilled = spillHandler.getMaxBatchesSpilled();
      this.totalBatchesSpilled = spillHandler.getTotalBatchesSpilled();
      this.maxRecordsSpilled = spillHandler.getMaxRecordsSpilled();
      this.totalRecordsSpilled = spillHandler.getTotalRecordsSpilled();
      this.activeSpilled = new SpilledPartitionState[spillHandler.getActiveSpilledPartitionCount()];
      this.onDiskOnly = new SpilledPartitionState[spillHandler.getSpilledPartitionCount()];

      final List<VectorizedHashAggDiskPartition> activeSpilledPartitions = spillHandler.getActiveSpilledPartitions();
      int counter = 0;
      for (VectorizedHashAggDiskPartition activeSpilledPartition : activeSpilledPartitions) {
        final SpilledPartitionState state =
          new SpilledPartitionState(activeSpilledPartition.getIdentifier(),
                                    activeSpilledPartition.getNumberOfBatches(),
                                    activeSpilledPartition.getSpillFile().getPath().toString(), true);
        activeSpilled[counter] = state;
        counter++;
      }

      final Queue<VectorizedHashAggDiskPartition> spillQueue = spillHandler.getSpilledPartitions();
      final Iterator<VectorizedHashAggDiskPartition> iterator = spillQueue.iterator();
      counter = 0;
      while (iterator.hasNext()) {
        final VectorizedHashAggDiskPartition spilledPartition = iterator.next();
        final SpilledPartitionState state =
          new SpilledPartitionState(spilledPartition.getIdentifier(), spilledPartition.getNumberOfBatches(),
                                    spilledPartition.getSpillFile().getPath().toString(), false);
        onDiskOnly[counter] = state;
        counter++;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(" OOMEvent:");
      sb.append(" Iterations: ").append(iterations);
      sb.append(" Spills: ").append(spills);
      sb.append(" OOMs: ").append(ooms);
      sb.append(" AllocatedMemory: ").append(allocatedMemory);
      sb.append(" MaxBatchesSpilled: ").append(maxBatchesSpilled);
      sb.append(" TotalBatchesSpilled: ").append(totalBatchesSpilled);
      sb.append(" MaxRecordsSpilled: ").append(maxRecordsSpilled);
      sb.append(" TotalRecordsSpilled: ").append(totalRecordsSpilled);
      for (SpilledPartitionState state: activeSpilled) {
        sb.append(state.toString());
      }
      for (SpilledPartitionState state: onDiskOnly) {
        sb.append(state.toString());
      }
      return sb.toString();
    }
  }
}
