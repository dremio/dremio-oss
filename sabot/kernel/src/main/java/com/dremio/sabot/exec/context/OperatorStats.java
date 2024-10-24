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
package com.dremio.sabot.exec.context;

import static com.dremio.common.perf.StatsCollectionEligibilityRegistrar.isEligible;

import com.carrotsearch.hppc.IntDoubleHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import com.carrotsearch.hppc.procedures.IntDoubleProcedure;
import com.carrotsearch.hppc.procedures.IntLongProcedure;
import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.MetricValue;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile.Builder;
import com.dremio.exec.proto.UserBitShared.OperatorProfileDetails;
import com.dremio.exec.proto.UserBitShared.ParquetDecodingDetailsInfo;
import com.dremio.exec.proto.UserBitShared.RunTimeFilterDetailsInfoInScan;
import com.dremio.exec.proto.UserBitShared.SlowIOInfo;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.io.file.Path;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.Operator;
import com.google.common.base.Joiner;
import de.vandermeer.asciitable.v2.V2_AsciiTable;
import de.vandermeer.asciitable.v2.render.V2_AsciiTableRenderer;
import de.vandermeer.asciitable.v2.render.WidthAbsoluteEven;
import de.vandermeer.asciitable.v2.themes.V2_E_TableThemes;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;

public class OperatorStats {
  private static final String PREFIX = "operator_stats";

  private static final Meter.MeterProvider<Counter> operatorEvalHeapAllocatedTotal =
      Counter.builder(Joiner.on(".").join(PREFIX, "eval_heap_allocated"))
          .description("Tracks total heap allocation during eval by an operator")
          .withRegistry(Metrics.globalRegistry);

  private static final Meter.MeterProvider<Counter> operatorSetupHeapAllocatedTotal =
      Counter.builder(Joiner.on(".").join(PREFIX, "setup_heap_allocated"))
          .description("Tracks total heap allocation during setup by an operator")
          .withRegistry(Metrics.globalRegistry);

  protected final int operatorId;
  protected final int operatorType;
  protected final int operatorSubType;

  private final BufferAllocator allocator;

  private final IntLongHashMap longMetrics = new IntLongHashMap();
  private final IntDoubleHashMap doubleMetrics = new IntDoubleHashMap();

  public long[] recordsReceivedByInput;
  public long[] batchesReceivedByInput;
  public long[] sizeInBytesReceivedByInput;

  private long outputRecords = 0;
  private long numberOfBatches = 0;
  private long outputSizeInBytes = 0;

  // DML specific stats
  private long addedFilesCount = 0;
  private long removedFilesCount = 0;

  private boolean recordOutput = false;

  enum State {
    NONE,
    SETUP,
    PROCESSING,
    WAIT;

    private static final int Size = State.values().length;
  }

  private State currentState = State.NONE;
  private State savedState = State.NONE;

  private final long[] stateNanos = new long[State.Size];
  private final long[] stateMark = new long[State.Size];

  private final int inputCount;

  private final long warnIOTimeThreshold;

  private int totalFieldCount;
  // misc operator details that are saved in the profile.
  private OperatorProfileDetails profileDetails;
  private final List<RunTimeFilterDetailsInfoInScan> runtimeFilterDetailsInScan = new ArrayList<>();
  private final List<SlowIOInfo> slowIoInfos = new ArrayList<>();
  private final List<SlowIOInfo> slowMetadataIoInfos = new ArrayList<>();
  private final List<ParquetDecodingDetailsInfo> parquetDecodingDetailsInfos = new ArrayList<>();
  private Tags operatorTags;
  // This is enum value of Operator.MasterState
  private int masterState;
  private long lastScheduleTime;
  private long avgAllocatedHeap;
  private long totalAllocatedHeap;
  private long numProcessingLoop;
  private long peakAllocatedHeap;
  private long setupAllocatedHeap;
  private long startHeapAllocation = -1;

  // Need this wrapper so that the caller don't have to handle exception from close().
  public interface WaitRecorder extends AutoCloseable {
    @Override
    void close();
  }

  public class MetadataWaitRecorder implements WaitRecorder {

    private final String path;
    private final WaitRecorder wr;

    public MetadataWaitRecorder(String filePath, WaitRecorder recorder) {
      path = filePath;
      wr = recorder;
    }

    @Override
    public void close() {
      updateReadIOStatsMetadata(System.nanoTime() - stateMark[currentState.ordinal()], path);
      wr.close();
    }
  }

  private MetadataWaitRecorder createMetadataWaitRecorder(String path, WaitRecorder wr) {
    return new MetadataWaitRecorder(path, wr);
  }

  // No-op implementation of WaitRecorder.
  static final WaitRecorder NO_OP_RECORDER = () -> {};

  // Recorder that does a stopWait() on close. This can be used in try-with-resources context.
  // Note that the close for this not idempotent.
  final WaitRecorder recorder = this::stopWait;

  public static class IOStats {
    public final AtomicLong minIOTime = new AtomicLong(Long.MAX_VALUE);
    public final AtomicLong maxIOTime = new AtomicLong(0);
    public final AtomicLong totalIOTime = new AtomicLong(0);
    public final AtomicInteger numIO = new AtomicInteger(0);
    public final List<SlowIOInfo> slowIOInfoList = new ArrayList<>();
    private long lastSlowIOLoggingTime = 0;
  }

  private IOStats readIOStats;
  private IOStats writeIOStats;

  private IOStats metadataReadIOStats;

  public void createReadIOStats() {
    if (this.readIOStats != null) {
      return;
    }
    this.readIOStats = new IOStats();
  }

  public void createWriteIOStats() {
    if (this.writeIOStats != null) {
      return;
    }
    this.writeIOStats = new IOStats();
  }

  public void createMetadataReadIOStats() {
    if (this.metadataReadIOStats != null) {
      return;
    }
    this.metadataReadIOStats = new IOStats();
  }

  public IOStats getReadIOStats() {
    return readIOStats;
  }

  public IOStats getMetadataReadIOStats() {
    return metadataReadIOStats;
  }

  public IOStats getWriteIOStats() {
    return writeIOStats;
  }

  public OperatorStats(OpProfileDef def, BufferAllocator allocator) {
    this(
        def.getOperatorId(),
        def.getOperatorType(),
        def.getIncomingCount(),
        allocator,
        Long.MAX_VALUE,
        def.operatorSubType);
  }

  public OperatorStats(OpProfileDef def, BufferAllocator allocator, long warnIOTimeThreshold) {
    this(
        def.getOperatorId(),
        def.getOperatorType(),
        def.getIncomingCount(),
        allocator,
        warnIOTimeThreshold,
        def.operatorSubType);
  }

  /**
   * Copy constructor to be able to create a copy of existing stats object shell and use it
   * independently this is useful if stats have to be updated in different threads, since it is not
   * really possible to update such stats as waitNanos, setupNanos and processingNanos across
   * threads
   *
   * @param original - OperatorStats object to create a copy from
   * @param isClean - flag to indicate whether to start with clean state indicators or inherit those
   *     from original object
   */
  public OperatorStats(OperatorStats original, boolean isClean) {
    this(
        original.operatorId,
        original.operatorType,
        original.inputCount,
        original.allocator,
        original.warnIOTimeThreshold,
        original.operatorSubType);

    if (!isClean) {
      currentState = original.currentState;
      savedState = original.savedState;

      System.arraycopy(original.stateMark, 0, stateMark, 0, State.Size);
    }
  }

  private OperatorStats(
      int operatorId,
      int operatorType,
      int inputCount,
      BufferAllocator allocator,
      long warnIOTimeThreshold,
      int operatorSubType) {
    super();
    this.allocator = allocator;
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.inputCount = inputCount;
    this.recordsReceivedByInput = new long[inputCount];
    this.batchesReceivedByInput = new long[inputCount];
    this.sizeInBytesReceivedByInput = new long[inputCount];
    this.warnIOTimeThreshold = warnIOTimeThreshold;
    this.operatorSubType = operatorSubType;
    this.masterState = 0;
    this.totalFieldCount = -1;
    this.operatorTags = Tags.of(Tag.of("operator_type", getOperatorTypeAsString()));
  }

  private String getOperatorTypeAsString() {
    try {
      return CoreOperatorType.values()[operatorType].name();
    } catch (IndexOutOfBoundsException e) {
      return "UNKNOWN";
    }
  }

  public int getOperatorId() {
    return operatorId;
  }

  public int getOperatorType() {
    return operatorType;
  }

  public long getAverageHeapAllocated() {
    return avgAllocatedHeap / MB;
  }

  public long getPeakHeapAllocated() {
    return peakAllocatedHeap / MB;
  }

  private String assertionError(String msg) {
    return String.format(
        "Failure while %s for operator id %d. Currently have currentState:%s savedState:%s",
        msg, operatorId, currentState.name(), savedState.name());
  }

  /**
   * OperatorStats merger - to merge stats from other OperatorStats this is needed in case some
   * processing is multithreaded that needs to have separate OperatorStats to deal with WARN - this
   * will only work for metrics that can be added
   *
   * @param from - OperatorStats from where to merge to "this"
   * @return OperatorStats - for convenience so one can merge multiple stats in one go
   */
  public OperatorStats mergeMetrics(OperatorStats from) {
    final IntLongHashMap fromMetrics = from.longMetrics;

    final Iterator<IntLongCursor> iter = fromMetrics.iterator();
    while (iter.hasNext()) {
      final IntLongCursor next = iter.next();
      longMetrics.putOrAdd(next.key, next.value, next.value);
    }

    final IntDoubleHashMap fromDMetrics = from.doubleMetrics;
    final Iterator<IntDoubleCursor> iterD = fromDMetrics.iterator();

    while (iterD.hasNext()) {
      final IntDoubleCursor next = iterD.next();
      doubleMetrics.putOrAdd(next.key, next.value, next.value);
    }
    return this;
  }

  /** Clear stats */
  public void clear() {
    Arrays.fill(stateNanos, 01);
    longMetrics.clear();
    doubleMetrics.clear();
  }

  private void startState(State nextState) {
    if (nextState != State.NONE) {
      stateMark[nextState.ordinal()] = System.nanoTime();
    }
    currentState = nextState;
  }

  private void stopState() {
    if (currentState != State.NONE) {
      int idx = currentState.ordinal();
      stateNanos[idx] += System.nanoTime() - stateMark[idx];
      currentState = State.NONE;
    }
  }

  public void startSetup() {
    assert currentState == State.PROCESSING : assertionError("starting setup");
    stopState();
    startState(State.SETUP);
  }

  public void stopSetup() {
    assert currentState == State.SETUP : assertionError("stopping setup");
    if (startHeapAllocation >= 0) {
      final long current = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
      setupAllocatedHeap = current - startHeapAllocation;
      operatorSetupHeapAllocatedTotal.withTags(operatorTags).increment(setupAllocatedHeap);
      startHeapAllocation = current;
    }
    stopState();
    startState(State.PROCESSING);
  }

  public void setSchema(BatchSchema initialSchema) {
    if (initialSchema != null) {
      totalFieldCount = initialSchema.getTotalFieldCount();
    }
  }

  // Use this method to account the processing time in OperatorStats
  public void startProcessing() {
    assert currentState == State.NONE : assertionError("starting processing");
    startHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
    startState(State.PROCESSING);
  }

  // use this method to account the processing time and record the OperatorState of the operator in
  // OperatorStats. Primarily called from SmartOp.
  public void startProcessing(Operator.OperatorState<?> state) {
    assert currentState == State.NONE : assertionError("starting processing");
    this.masterState = state.getMasterState().ordinal();
    lastScheduleTime = System.currentTimeMillis();
    startHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
    startState(State.PROCESSING);
  }

  public void stopProcessing() {
    assert currentState == State.PROCESSING : assertionError("stopping processing");
    if (startHeapAllocation >= 0) {
      long currentHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
      final long lastAllocatedHeap = currentHeapAllocation - startHeapAllocation;
      totalAllocatedHeap += lastAllocatedHeap;
      operatorEvalHeapAllocatedTotal.withTags(operatorTags).increment(lastAllocatedHeap);
      numProcessingLoop++;
      double avg =
          (double) avgAllocatedHeap
              + ((double) (lastAllocatedHeap - avgAllocatedHeap) / (double) numProcessingLoop);
      avgAllocatedHeap = Math.round(avg);
      peakAllocatedHeap = Math.max(lastAllocatedHeap, peakAllocatedHeap);
    }
    stopState();
  }

  public void stopProcessing(Operator.OperatorState<?> state) {
    assert currentState == State.PROCESSING : assertionError("stopping processing");
    this.masterState = state.getMasterState().ordinal();
    lastScheduleTime = System.currentTimeMillis();
    if (startHeapAllocation >= 0) {
      long currentHeapAllocation = HeapAllocatedMXBeanWrapper.getCurrentThreadAllocatedBytes();
      final long lastAllocatedHeap = currentHeapAllocation - startHeapAllocation;
      totalAllocatedHeap += lastAllocatedHeap;
      operatorEvalHeapAllocatedTotal.withTags(operatorTags).increment(lastAllocatedHeap);
      numProcessingLoop++;
      double avg =
          (double) avgAllocatedHeap
              + ((double) (lastAllocatedHeap - avgAllocatedHeap) / (double) numProcessingLoop);
      avgAllocatedHeap = Math.round(avg);
      peakAllocatedHeap = Math.max(lastAllocatedHeap, peakAllocatedHeap);
    }
    stopState();
  }

  public void startWait() {
    assert currentState != State.WAIT : assertionError("starting waiting");
    savedState = currentState;
    stopState();
    startState(State.WAIT);
  }

  public void stopWait() {
    assert currentState == State.WAIT : assertionError("stopping waiting");
    stopState();
    // revert to the saved state
    startState(savedState);
    savedState = State.NONE;
  }

  public void moveProcessingToWait(long nanos) {
    this.stateNanos[State.WAIT.ordinal()] += nanos;
    this.stateNanos[State.PROCESSING.ordinal()] -= nanos;
  }

  /*
   * starts wait only if it's not already in the wait mode.
   *
   * Returns true if this call started the wait mode. In that case, the caller should do a
   * stopWait() too.
   */
  public boolean checkAndStartWait() {
    if (currentState == State.WAIT) {
      return false;
    } else {
      startWait();
      return true;
    }
  }

  public void batchReceived(int inputIndex, long records, long size) {
    recordsReceivedByInput[inputIndex] += records;
    batchesReceivedByInput[inputIndex]++;
    sizeInBytesReceivedByInput[inputIndex] += size;
  }

  public void recordBatchOutput(long records, long size) {
    outputRecords += records;
    numberOfBatches++;
    outputSizeInBytes += size;
  }

  public OperatorProfile getProfile() {
    return getProfile(false);
  }

  public OperatorProfile getProfile(boolean withDetails) {
    final OperatorProfile.Builder b =
        OperatorProfile //
            .newBuilder() //
            .setOperatorType(operatorType) //
            .setOperatorId(operatorId) //
            .setSetupNanos(getSetupNanos()) //
            .setProcessNanos(getProcessingNanos())
            .setWaitNanos(getWaitNanos())
            .setOperatorSubtype(operatorSubType)
            .setOutputRecords(outputRecords)
            .setOutputBytes(outputSizeInBytes)
            .setAddedFiles(addedFilesCount)
            .setRemovedFiles(removedFilesCount)
            .setOperatorState(masterState)
            .setLastScheduleTime(lastScheduleTime);
    if (allocator != null) {
      b.setPeakLocalMemoryAllocated(
          Long.max(allocator.getPeakMemoryAllocation(), allocator.getInitReservation()));
    }
    if (withDetails && (profileDetails != null)) {
      b.setDetails(profileDetails);
    }
    addAllMetrics(b);
    return b.build();
  }

  public void addAllMetrics(OperatorProfile.Builder builder) {
    addStreamProfile(builder);
    addLongMetrics(builder);
    addDoubleMetrics(builder);
  }

  public long getRecordsProcessed() {
    if (recordOutput) {
      return outputRecords;
    } else {
      long recordsProcessed = 0;
      for (int i = 0; i < recordsReceivedByInput.length; i++) {
        recordsProcessed += recordsReceivedByInput[i];
      }
      return recordsProcessed;
    }
  }

  public void addStreamProfile(OperatorProfile.Builder builder) {
    if (recordOutput) {
      builder.addInputProfile(
          StreamProfile.newBuilder()
              .setBatches(numberOfBatches)
              .setRecords(outputRecords)
              .setSize(outputSizeInBytes));
      return;
    }
    for (int i = 0; i < recordsReceivedByInput.length; i++) {
      builder.addInputProfile(
          StreamProfile.newBuilder()
              .setBatches(batchesReceivedByInput[i])
              .setRecords(recordsReceivedByInput[i])
              .setSize(sizeInBytesReceivedByInput[i]));
    }
  }

  public void recordAddedFiles(long addedFilesCount) {
    this.addedFilesCount += addedFilesCount;
  }

  public void recordRemovedFiles(long removedFilesCount) {
    this.removedFilesCount += removedFilesCount;
  }

  private static class LongProc implements IntLongProcedure {

    private final OperatorProfile.Builder builder;

    public LongProc(Builder builder) {
      super();
      this.builder = builder;
    }

    @Override
    public void apply(int key, long value) {
      builder.addMetric(MetricValue.newBuilder().setMetricId(key).setLongValue(value));
    }
  }

  public void addLongMetrics(OperatorProfile.Builder builder) {
    if (longMetrics.size() > 0) {
      longMetrics.forEach(new LongProc(builder));
    }
  }

  private static class DoubleProc implements IntDoubleProcedure {
    private final OperatorProfile.Builder builder;

    public DoubleProc(Builder builder) {
      super();
      this.builder = builder;
    }

    @Override
    public void apply(int key, double value) {
      builder.addMetric(MetricValue.newBuilder().setMetricId(key).setDoubleValue(value));
    }
  }

  public void addDoubleMetrics(OperatorProfile.Builder builder) {
    if (doubleMetrics.size() > 0) {
      doubleMetrics.forEach(new DoubleProc(builder));
    }
  }

  public void addLongStat(MetricDef metric, long value) {
    longMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void addDoubleStat(MetricDef metric, double value) {
    doubleMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void setLongStat(MetricDef metric, long value) {
    longMetrics.put(metric.metricId(), value);
  }

  public long getLongStat(MetricDef metric) {
    return longMetrics.get(metric.metricId());
  }

  public void setDoubleStat(MetricDef metric, double value) {
    doubleMetrics.put(metric.metricId(), value);
  }

  private long getNanos(State state) {
    return stateNanos[state.ordinal()];
  }

  public long getSetupNanos() {
    return getNanos(State.SETUP);
  }

  public long getProcessingNanos() {
    return getNanos(State.PROCESSING);
  }

  public long getWaitNanos() {
    return getNanos(State.WAIT);
  }

  /**
   * Adjust waitNanos based on client calculations
   *
   * @param waitNanosOffset - could be negative as well as positive
   */
  public void adjustWaitNanos(long waitNanosOffset) {
    this.stateNanos[State.WAIT.ordinal()] += waitNanosOffset;
  }

  public void setProfileDetails(OperatorProfileDetails details) {
    this.profileDetails = details;
  }

  public OperatorProfileDetails getProfileDetails() {
    return this.profileDetails;
  }

  public void addRuntimeFilterDetailsInScan(
      List<RunTimeFilterDetailsInfoInScan> runtimeFilterDetails) {
    runtimeFilterDetailsInScan.addAll(runtimeFilterDetails);
  }

  public void addSlowIoInfos(List<SlowIOInfo> slowIoInfos) {
    this.slowIoInfos.addAll(slowIoInfos);
  }

  public void addSlowMetadataIoInfos(List<SlowIOInfo> slowMetadataIoInfos) {
    this.slowMetadataIoInfos.addAll(slowMetadataIoInfos);
  }

  public void addParquetDecodingDetailsInfos(
      List<ParquetDecodingDetailsInfo> parquetDecodingDetailsInfos) {
    this.parquetDecodingDetailsInfos.addAll(parquetDecodingDetailsInfos);
  }

  @Override
  public String toString() {
    String[] names = OperatorMetricRegistry.getMetricNames(operatorType);
    StringBuilder sb = new StringBuilder();

    final V2_AsciiTable outputTable = new V2_AsciiTable();
    outputTable.addRule();
    outputTable.addRow(
        String.format("Metrics for operator %s", CoreOperatorType.values()[operatorType]),
        String.format("id: %d.", operatorId));
    outputTable.addRule();
    outputTable.addRow("metric", "value");
    outputTable.addRow("Setup time", NumberFormat.getInstance().format(getSetupNanos()) + " ns");
    outputTable.addRow(
        "Processing time", NumberFormat.getInstance().format(getProcessingNanos()) + " ns");

    for (int i = 0; i < inputCount; i++) {
      outputTable.addRow(
          String.format("Input[%d] Records", i),
          NumberFormat.getInstance().format(recordsReceivedByInput[i]) + " records");
    }

    if (!longMetrics.isEmpty() || !doubleMetrics.isEmpty()) {
      outputTable.addRule();
      outputTable.addRow("Custom Metrics", "");
      outputTable.addRule();

      for (int i = 0; i < names.length; i++) {
        if (longMetrics.containsKey(i)) {
          long value = longMetrics.get(i);
          if (value != 0) {
            outputTable.addRow(names[i], NumberFormat.getInstance().format(value));
          }
        } else if (doubleMetrics.containsKey(i)) {
          double value = doubleMetrics.get(i);
          if (value != 0) {
            outputTable.addRow(names[i], NumberFormat.getInstance().format(value));
          }
        }
      }
    }

    outputTable.addRule();

    V2_AsciiTableRenderer rend = new V2_AsciiTableRenderer();
    rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
    rend.setWidth(new WidthAbsoluteEven(76));
    sb.append(rend.render(outputTable));
    sb.append("\n");

    return sb.toString();
  }

  private static final int KB = 1024;
  private static final int MB = 1024 * 1024;
  private static final String COL_DELIMITER = ",";

  public void fillLogBuffer(StringBuilder sb, String fragmentId, boolean dumpHeapUsage) {
    if (isLightOperator(dumpHeapUsage)) {
      // do not log details of light operators
      return;
    }
    final String id =
        fragmentId + ":" + operatorId + "[" + CoreOperatorType.values()[operatorType] + "]";
    sb.append(id)
        .append(COL_DELIMITER.repeat(3))
        .append(numProcessingLoop)
        .append(COL_DELIMITER.repeat(3))
        .append(TimeUnit.NANOSECONDS.toMillis(getSetupNanos()))
        .append(COL_DELIMITER)
        .append(TimeUnit.NANOSECONDS.toMillis(stateNanos[State.PROCESSING.ordinal()]))
        .append(COL_DELIMITER);
    if (inputCount == 0) {
      sb.append(COL_DELIMITER.repeat(6));
    } else {
      sb.append(recordsReceivedByInput[0]).append(COL_DELIMITER);
      sb.append(batchesReceivedByInput[0]).append(COL_DELIMITER);
      sb.append(sizeInBytesReceivedByInput[0]).append(COL_DELIMITER);
      if (inputCount == 2) {
        sb.append(recordsReceivedByInput[1]).append(COL_DELIMITER);
        sb.append(batchesReceivedByInput[1]).append(COL_DELIMITER);
        sb.append(sizeInBytesReceivedByInput[1]).append(COL_DELIMITER);
      } else {
        sb.append(COL_DELIMITER.repeat(3));
      }
    }
    if (outputRecords > 0) {
      sb.append(outputRecords).append(COL_DELIMITER);
      sb.append(numberOfBatches).append(COL_DELIMITER);
      sb.append(outputSizeInBytes).append(COL_DELIMITER);
    } else {
      sb.append(COL_DELIMITER.repeat(3));
    }
    if (totalFieldCount >= 0) {
      sb.append(totalFieldCount);
    }
    sb.append(COL_DELIMITER);
    if (allocator != null) {
      final long allocated =
          Long.max(allocator.getPeakMemoryAllocation(), allocator.getInitReservation()) / KB;
      sb.append(allocated);
    }
    sb.append(COL_DELIMITER);
    insertOtherOptionals(sb);
    if (startHeapAllocation >= 0 && dumpHeapUsage) {
      sb.append(COL_DELIMITER);
      sb.append(setupAllocatedHeap / KB).append(COL_DELIMITER);
      sb.append(avgAllocatedHeap / KB).append(COL_DELIMITER);
      sb.append(peakAllocatedHeap / KB).append(COL_DELIMITER);
      sb.append(totalAllocatedHeap / KB);
    } else {
      if (dumpHeapUsage) {
        sb.append(COL_DELIMITER.repeat(4));
      }
    }
    sb.append(System.lineSeparator());
  }

  private void insertOtherOptionals(StringBuilder sb) {
    boolean delimit = false;
    if (this.profileDetails != null && this.profileDetails.getSplitInfosCount() > 0) {
      sb.append("Num Expression Splits = ").append(this.profileDetails.getSplitInfosCount());
      delimit = true;
    }
    if (addedFilesCount > 0) {
      if (delimit) {
        sb.append("::");
      }
      sb.append("Added Files = ").append(addedFilesCount);
      delimit = true;
    }
    if (removedFilesCount > 0) {
      if (delimit) {
        sb.append("::");
      }
      sb.append("Removed Files = ").append(removedFilesCount);
    }
  }

  private boolean isLightOperator(boolean dumpHeapUsage) {
    if (inputCount == 1 && batchesReceivedByInput[0] > 10) {
      return false;
    }
    if (inputCount == 2 && (batchesReceivedByInput[0] > 10 || batchesReceivedByInput[1] > 10)) {
      return false;
    }
    if (numberOfBatches > 10) {
      return false;
    }
    if (dumpHeapUsage && startHeapAllocation >= 0) {
      if (totalAllocatedHeap > 512 * KB
          || peakAllocatedHeap > 64 * KB
          || avgAllocatedHeap > 32 * KB) {
        return false;
      }
      if (setupAllocatedHeap > 64 * KB) {
        return false;
      }
    }
    return totalFieldCount <= 100;
  }

  public static WaitRecorder getWaitRecorder(OperatorStats operatorStats) {
    if (operatorStats == null || !isEligible() || !operatorStats.checkAndStartWait()) {
      /*
       Return a NO_OP_RECORDER if any of the conditions are met:
       1. If the operatorStats is missing
       2. If the thread this method is invoked from is not eligible for stats collection
       3. If operatorStats is already in WAIT state
      */
      return NO_OP_RECORDER;
    } else {
      return operatorStats.recorder;
    }
  }

  public static WaitRecorder getMetadataWaitRecorder(OperatorStats operatorStats, Path path) {
    if (operatorStats == null || path == null) {
      return NO_OP_RECORDER;
    } else {
      return operatorStats.createMetadataWaitRecorder(
          path.toString(), OperatorStats.getWaitRecorder(operatorStats));
    }
  }

  private void updateIOStats(IOStats ioStats, long elapsed, String filePath, long n, long offset) {
    if (ioStats == null) {
      return;
    }

    ioStats.minIOTime.getAndAccumulate(elapsed, Math::min);
    ioStats.maxIOTime.getAndAccumulate(elapsed, Math::max);
    ioStats.totalIOTime.addAndGet(elapsed);
    ioStats.numIO.incrementAndGet();

    if (elapsed >= TimeUnit.MILLISECONDS.toNanos(warnIOTimeThreshold)) {
      synchronized (ioStats.slowIOInfoList) {
        ioStats.slowIOInfoList.add(
            SlowIOInfo.newBuilder()
                .setFilePath(filePath)
                .setIoTime(elapsed)
                .setIoSize(n)
                .setIoOffset(offset)
                .build());
      }
    }
  }

  public void updateReadIOStats(long elapsed, String filePath, long n, long offset) {
    updateIOStats(readIOStats, elapsed, filePath, n, offset);
  }

  public void updateWriteIOStats(long elapsed, String filePath, long n, long offset) {
    updateIOStats(writeIOStats, elapsed, filePath, n, offset);
  }

  public void updateReadIOStatsMetadata(long elapsed, String path) {
    updateIOStats(metadataReadIOStats, elapsed, path, 0, 0);
  }

  public void setReadIOStats() {
    IOStats ioStats = getReadIOStats();
    IOStats ioStatsMetadata = getMetadataReadIOStats();

    OperatorProfileDetails.Builder profileDetailsBuilder = OperatorProfileDetails.newBuilder();

    if (ioStats != null) {
      setLongStat(ScanOperator.Metric.MIN_IO_READ_TIME_NS, ioStats.minIOTime.longValue());
      setLongStat(ScanOperator.Metric.MAX_IO_READ_TIME_NS, ioStats.maxIOTime.longValue());
      setLongStat(
          ScanOperator.Metric.AVG_IO_READ_TIME_NS,
          ioStats.numIO.get() == 0 ? 0 : ioStats.totalIOTime.longValue() / ioStats.numIO.get());
      setLongStat(ScanOperator.Metric.NUM_IO_READ, ioStats.numIO.longValue());
      profileDetailsBuilder.addAllSlowIoInfos(ioStats.slowIOInfoList);
    }

    if (ioStatsMetadata != null) {
      setLongStat(
          ScanOperator.Metric.MIN_METADATA_IO_READ_TIME_NS, ioStatsMetadata.minIOTime.longValue());
      setLongStat(
          ScanOperator.Metric.MAX_METADATA_IO_READ_TIME_NS, ioStatsMetadata.maxIOTime.longValue());
      setLongStat(
          ScanOperator.Metric.AVG_METADATA_IO_READ_TIME_NS,
          ioStatsMetadata.numIO.get() == 0
              ? 0
              : ioStatsMetadata.totalIOTime.longValue() / ioStatsMetadata.numIO.get());
      setLongStat(ScanOperator.Metric.NUM_METADATA_IO_READ, ioStatsMetadata.numIO.longValue());
      profileDetailsBuilder.addAllSlowMetadataIoInfos(ioStatsMetadata.slowIOInfoList);
    }

    setProfileDetails(profileDetailsBuilder.build());
  }

  public void setScanRuntimeFilterDetailsInProfile() {
    OperatorProfileDetails.Builder profileDetailsBuilder = getProfileDetails().toBuilder();
    profileDetailsBuilder.clearRuntimefilterDetailsInfosInScan();
    profileDetailsBuilder.addAllRuntimefilterDetailsInfosInScan(runtimeFilterDetailsInScan);
    setProfileDetails(profileDetailsBuilder.build());
  }

  public void setSlowIoInfosInProfile() {
    OperatorProfileDetails.Builder profileDetailsBuilder = getProfileDetails().toBuilder();
    profileDetailsBuilder.clearSlowIoInfos();
    profileDetailsBuilder.addAllSlowIoInfos(slowIoInfos);
    profileDetailsBuilder.clearSlowMetadataIoInfos();
    profileDetailsBuilder.addAllSlowMetadataIoInfos(slowMetadataIoInfos);
    setProfileDetails(profileDetailsBuilder.build());
  }

  public void setParquetDecodingDetailsInfosInProfile() {
    setProfileDetails(
        getProfileDetails().toBuilder()
            .clearParquetDecodingDetailsInfo()
            .addAllParquetDecodingDetailsInfo(parquetDecodingDetailsInfos)
            .build());
  }

  public void setRecordOutput(boolean recordOutput) {
    this.recordOutput = recordOutput;
  }
}
