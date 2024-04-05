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

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;

public class ExternalSortTracer {
  private int targetBatchSizeInBytes;
  private int targetBatchSize;

  private int spilledBatchCount;
  private int totalRecordsSpilled;
  private int recordsToSpillInCurrentIteration;
  private int recordsSpilledInCurrentIteration;
  private String spilledBatchSchema;
  private int initialCapacityForCurrentSpillIteration;
  private int maxBatchSizeSpilledInCurrentIteration;

  private int numDiskRuns;
  private int spillCount;
  private int mergeCount;
  private int maxBatchSizeAllDiskRuns;

  private final List<Object> events = Lists.newArrayList();

  /* Allocator created by MemoryRun to reserve memory for spilling sorted data to disk */
  private SpillCopyAllocatorState spillCopyAllocatorState = new SpillCopyAllocatorState();
  /* Allocator created by DiskRunManager to reserve memory for loading spilled batches from multiple disk runs */
  private DiskRunCopyAllocatorState diskRunCopyAllocatorState = new DiskRunCopyAllocatorState();
  /* Operator's allocator; passed to ExternalSortOperator as part of OperatorContext */
  private ExternalSortAllocatorState sortAllocatorState = new ExternalSortAllocatorState();

  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExternalSortTracer.class);

  public void setTargetBatchSizeInBytes(int targetBatchSizeInBytes) {
    this.targetBatchSizeInBytes = targetBatchSizeInBytes;
  }

  public void setTargetBatchSize(int targetBatchSize) {
    this.targetBatchSize = targetBatchSize;
  }

  public void reserveMemoryForSpillOOMEvent(
      final long initReservation, final long maxAllocation, BufferAllocator oldSpillCopyAllocator) {
    ReserveMemoryForSpillOOMEvent reserveMemoryEvent =
        new ReserveMemoryForSpillOOMEvent(initReservation, maxAllocation, oldSpillCopyAllocator);
    events.add(reserveMemoryEvent);
  }

  public void reserveMemoryForDiskRunCopyOOMEvent(
      final long initReservation, final long maxAllocation, final long maxBatchSizeAllDiskRuns) {
    ReserveMemoryForDiskRunCopyOOMEvent reserveMemoryEvent =
        new ReserveMemoryForDiskRunCopyOOMEvent(
            initReservation, maxAllocation, maxBatchSizeAllDiskRuns);
    events.add(reserveMemoryEvent);
  }

  public void setBatchesSpilled(int batchCount) {
    this.spilledBatchCount = batchCount;
  }

  public void setTotalRecordsSpilled(int recordsSpilled) {
    this.totalRecordsSpilled = recordsSpilled;
  }

  public void setRecordsToSpillInCurrentIteration(int recordsToSpill) {
    this.recordsToSpillInCurrentIteration = recordsToSpill;
  }

  public void setRecordsSpilledInCurrentIteration(int recordsSpilled) {
    this.recordsSpilledInCurrentIteration = recordsSpilled;
  }

  public void setSchemaOfBatchToSpill(String schema) {
    this.spilledBatchSchema = schema;
  }

  public void setInitialCapacityForCurrentSpillIteration(int initialCapacity) {
    this.initialCapacityForCurrentSpillIteration = initialCapacity;
  }

  public void setMaxBatchSizeSpilled(int maxBatchSizeSpilled) {
    this.maxBatchSizeSpilledInCurrentIteration = maxBatchSizeSpilled;
  }

  public void setDiskRunState(
      int numDiskRuns, int spillCount, int mergeCount, int maxBatchSizeAllDiskRuns) {
    this.numDiskRuns = numDiskRuns;
    this.spillCount = spillCount;
    this.mergeCount = mergeCount;
    this.maxBatchSizeAllDiskRuns = maxBatchSizeAllDiskRuns;
  }

  public void setSpillCopyAllocatorState(final BufferAllocator spillCopyAllocator) {
    if (spillCopyAllocator == null) {
      spillCopyAllocatorState.valid = false;
    } else {
      spillCopyAllocatorState.valid = true;
      spillCopyAllocatorState.name = spillCopyAllocator.getName();
      spillCopyAllocatorState.allocatedMemory = spillCopyAllocator.getAllocatedMemory();
      spillCopyAllocatorState.peakAllocation = spillCopyAllocator.getPeakMemoryAllocation();
      spillCopyAllocatorState.maxAllowed = spillCopyAllocator.getLimit();
      spillCopyAllocatorState.headRoom = spillCopyAllocator.getHeadroom();
      spillCopyAllocatorState.initReservation = spillCopyAllocator.getInitReservation();
    }
  }

  public void setDiskRunCopyAllocatorState(final BufferAllocator diskRunCopyAllocator) {
    if (diskRunCopyAllocator == null) {
      diskRunCopyAllocatorState.valid = false;
    } else {
      diskRunCopyAllocatorState.valid = true;
      diskRunCopyAllocatorState.name = diskRunCopyAllocator.getName();
      diskRunCopyAllocatorState.allocatedMemory = diskRunCopyAllocator.getAllocatedMemory();
      diskRunCopyAllocatorState.peakAllocation = diskRunCopyAllocator.getPeakMemoryAllocation();
      diskRunCopyAllocatorState.maxAllowed = diskRunCopyAllocator.getLimit();
      diskRunCopyAllocatorState.headRoom = diskRunCopyAllocator.getHeadroom();
      diskRunCopyAllocatorState.initReservation = diskRunCopyAllocator.getInitReservation();
    }
  }

  public void setExternalSortAllocatorState(final BufferAllocator sortAllocator) {
    /* this is the parent allocator passed to ExternalSortOperator as part of
     * operator context. it is then used by MemoryRun and DiskRunManager to create
     * child allocators.
     */
    if (sortAllocator == null) {
      sortAllocatorState.valid = false;
    } else {
      sortAllocatorState.valid = true;
      sortAllocatorState.name = sortAllocator.getName();
      sortAllocatorState.allocatedMemory = sortAllocator.getAllocatedMemory();
      sortAllocatorState.peakAllocation = sortAllocator.getPeakMemoryAllocation();
      sortAllocatorState.maxAllowed = sortAllocator.getLimit();
      sortAllocatorState.headRoom = sortAllocator.getHeadroom();
      sortAllocatorState.initReservation = sortAllocator.getInitReservation();
    }
  }

  public UserException prepareAndThrowException(final Exception ex, final String message) {
    final UserException.Builder exBuilder = getExceptionBuilder(ex, message);
    if (message != null) {
      exBuilder.addContext(message);
    }
    // exBuilder.addContext("Detailed External Sort Error Tracing follows: \n\n" + toString() +
    // Joiner.on("\n").join(events));
    exBuilder.addContext("Target Batch Size (in bytes)", targetBatchSizeInBytes);
    exBuilder.addContext("Target Batch Size", targetBatchSize);
    exBuilder.addContext("Batches spilled in failed run", spilledBatchCount);
    exBuilder.addContext("Records spilled in failed run", totalRecordsSpilled);
    exBuilder.addContext(
        "Records to spill in current iteration of failed run", recordsToSpillInCurrentIteration);
    exBuilder.addContext(
        "Records spilled in current iteration of failed run", recordsSpilledInCurrentIteration);
    exBuilder.addContext(
        "Max batch size spilled in current iteration of failed run",
        maxBatchSizeSpilledInCurrentIteration);
    exBuilder.addContext("Spilled Batch Schema of failed run", spilledBatchSchema);
    exBuilder.addContext(
        "Initial capacity for current iteration of failed run",
        initialCapacityForCurrentSpillIteration);
    exBuilder.addContext("Spill copy allocator-Name", spillCopyAllocatorState.name);
    exBuilder.addContext(
        "Spill copy allocator-Allocated memory", spillCopyAllocatorState.allocatedMemory);
    exBuilder.addContext("Spill copy allocator-Max allowed", spillCopyAllocatorState.maxAllowed);
    exBuilder.addContext(
        "Spill copy allocator-Init Reservation", spillCopyAllocatorState.initReservation);
    exBuilder.addContext(
        "Spill copy allocator-Peak allocated", spillCopyAllocatorState.peakAllocation);
    exBuilder.addContext("Spill copy allocator-Head room", spillCopyAllocatorState.headRoom);
    exBuilder.addContext("Disk runs", numDiskRuns);
    exBuilder.addContext("Spill count", spillCount);
    exBuilder.addContext("Merge count", mergeCount);
    exBuilder.addContext("Max batch size amongst all disk runs", maxBatchSizeAllDiskRuns);
    exBuilder.addContext("Disk run copy allocator-Name", diskRunCopyAllocatorState.name);
    exBuilder.addContext(
        "Disk run copy allocator-Allocated memory", diskRunCopyAllocatorState.allocatedMemory);
    exBuilder.addContext(
        "Disk run copy allocator-Max allowed", diskRunCopyAllocatorState.maxAllowed);
    exBuilder.addContext(
        "Disk run copy allocator-Init Reservation", diskRunCopyAllocatorState.initReservation);
    exBuilder.addContext(
        "Disk run copy allocator-Peak allocated", diskRunCopyAllocatorState.peakAllocation);
    exBuilder.addContext("Disk run copy allocator-Head room", diskRunCopyAllocatorState.headRoom);
    exBuilder.addContext("Sort allocator-Name", sortAllocatorState.name);
    exBuilder.addContext("Sort allocator-Allocated memory", sortAllocatorState.allocatedMemory);
    exBuilder.addContext("Sort allocator-Max allowed", sortAllocatorState.maxAllowed);
    exBuilder.addContext("Sort allocator-Init Reservation", sortAllocatorState.initReservation);
    exBuilder.addContext("Sort allocator-Peak allocated", sortAllocatorState.peakAllocation);
    exBuilder.addContext("Sort allocator-Head room", sortAllocatorState.headRoom);
    exBuilder.addContext("OOM Events", Joiner.on("\n").join(events));

    return exBuilder.build(logger);
  }

  private UserException.Builder getExceptionBuilder(final Exception ex, final String msg) {
    final UserException.Builder exceptionBuilder = UserException.memoryError(ex);
    return exceptionBuilder;
  }

  private abstract static class AllocatorState {
    protected String name;
    protected long allocatedMemory;
    protected long initReservation;
    protected long peakAllocation;
    protected long maxAllowed;
    protected long headRoom;
    protected boolean valid;
  }

  private static class SpillCopyAllocatorState extends AllocatorState {

    SpillCopyAllocatorState() {}
  }

  private static class DiskRunCopyAllocatorState extends AllocatorState {

    DiskRunCopyAllocatorState() {}
  }

  private static class ExternalSortAllocatorState extends AllocatorState {

    ExternalSortAllocatorState() {}
  }

  private static class ReserveMemoryForSpillOOMEvent {
    /* we failed with OOM when allocating a spill copy allocator with this state */
    private final long failedInitReservation;
    private final long failedMaxAllocation;

    /* state of copy allocator that existed before we attempted to reserve more and ran out of OOM */
    private final long previousAllocatedMemory;
    private final long previousHeadRoom;
    private final long previousMaxAllocation;
    private final long previousInitReservation;

    ReserveMemoryForSpillOOMEvent(
        final long initReservation,
        final long maxAllocation,
        final BufferAllocator oldSpillCopyAllocator) {
      this.failedInitReservation = initReservation;
      this.failedMaxAllocation = maxAllocation;
      if (oldSpillCopyAllocator != null) {
        this.previousAllocatedMemory = oldSpillCopyAllocator.getAllocatedMemory();
        this.previousHeadRoom = oldSpillCopyAllocator.getHeadroom();
        this.previousMaxAllocation = oldSpillCopyAllocator.getLimit();
        this.previousInitReservation = oldSpillCopyAllocator.getInitReservation();
      } else {
        this.previousAllocatedMemory = 0L;
        this.previousHeadRoom = 0L;
        this.previousMaxAllocation = 0L;
        this.previousInitReservation = 0L;
      }
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("Event: Failed to Reserve copy space for spill in MemoryRun");
      sb.append(" failedInitReservation " + failedInitReservation);
      sb.append(" failedMaxAllocation " + failedMaxAllocation);
      sb.append(" previousAllocatedMemory " + previousAllocatedMemory);
      sb.append(" previousMaxAllocation " + previousMaxAllocation);
      sb.append(" previousInitReservation " + previousInitReservation);
      sb.append(" previousHeadRoom " + previousHeadRoom);
      return sb.toString();
    }
  }

  private static class ReserveMemoryForDiskRunCopyOOMEvent {
    /* we failed with OOM when allocating a disk run copy allocator with this state */
    private final long failedInitReservation;
    private final long failedMaxAllocation;
    private final long maxBatchSizeAllDiskRuns;

    ReserveMemoryForDiskRunCopyOOMEvent(
        final long initReservation, final long maxAllocation, final long maxBatchSizeAllDiskRuns) {
      this.failedInitReservation = initReservation;
      this.failedMaxAllocation = maxAllocation;
      this.maxBatchSizeAllDiskRuns = maxBatchSizeAllDiskRuns;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("Event: Failed to Reserve space for copy allocator in DiskRunManager");
      sb.append(" failedInitReservation " + failedInitReservation);
      sb.append(" failedMaxAllocation " + failedMaxAllocation);
      sb.append(" maxBatchSizeAllDiskRuns " + maxBatchSizeAllDiskRuns);
      return sb.toString();
    }
  }
}
