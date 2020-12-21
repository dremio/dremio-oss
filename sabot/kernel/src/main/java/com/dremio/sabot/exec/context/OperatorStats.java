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

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;

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
import com.dremio.exec.proto.UserBitShared.SlowIOInfo;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.io.file.Path;

import de.vandermeer.asciitable.v2.V2_AsciiTable;
import de.vandermeer.asciitable.v2.render.V2_AsciiTableRenderer;
import de.vandermeer.asciitable.v2.render.WidthAbsoluteEven;
import de.vandermeer.asciitable.v2.themes.V2_E_TableThemes;

public class OperatorStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorStats.class);

  protected final int operatorId;
  protected final int operatorType;
  private final BufferAllocator allocator;

  private IntLongHashMap longMetrics = new IntLongHashMap();
  private IntDoubleHashMap doubleMetrics = new IntDoubleHashMap();

  public long[] recordsReceivedByInput;
  public long[] batchesReceivedByInput;
  public long[] sizeInBytesReceivedByInput;

  enum State {
    NONE,
    SETUP,
    PROCESSING,
    WAIT;

    private static final int Size = State.values().length;
  }
  private State currentState = State.NONE;
  private State savedState = State.NONE;

  private long[] stateNanos = new long[State.Size];
  private long[] stateMark = new long[State.Size];

  private int inputCount;

  private long warnIOTimeThreshold;

  // misc operator details that are saved in the profile.
  private OperatorProfileDetails profileDetails;


  // Need this wrapper so that the caller don't have to handle exception from close().
  public interface WaitRecorder extends AutoCloseable {
    @Override
    void close();
  }

  public class MetadataWaitRecorder implements WaitRecorder {

    private String path;
    private WaitRecorder wr;

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

  public class IOStats {
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

  public OperatorStats(OpProfileDef def, BufferAllocator allocator){
    this(def.getOperatorId(), def.getOperatorType(), def.getIncomingCount(), allocator, Long.MAX_VALUE);
  }

  public OperatorStats(OpProfileDef def, BufferAllocator allocator, long warnIOTimeThreshold){
    this(def.getOperatorId(), def.getOperatorType(), def.getIncomingCount(), allocator, warnIOTimeThreshold);
  }

  /**
   * Copy constructor to be able to create a copy of existing stats object shell and use it independently
   * this is useful if stats have to be updated in different threads, since it is not really
   * possible to update such stats as waitNanos, setupNanos and processingNanos across threads
   * @param original - OperatorStats object to create a copy from
   * @param isClean - flag to indicate whether to start with clean state indicators or inherit those from original object
   */
  public OperatorStats(OperatorStats original, boolean isClean) {
    this(original.operatorId, original.operatorType, original.inputCount, original.allocator, original.warnIOTimeThreshold);

    if ( !isClean ) {
      currentState = original.currentState;
      savedState = original.savedState;

      System.arraycopy(original.stateMark, 0, stateMark, 0, State.Size);
    }
  }

  private OperatorStats(int operatorId, int operatorType, int inputCount, BufferAllocator allocator, long warnIOTimeThreshold) {
    super();
    this.allocator = allocator;
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.inputCount = inputCount;
    this.recordsReceivedByInput = new long[inputCount];
    this.batchesReceivedByInput = new long[inputCount];
    this.sizeInBytesReceivedByInput = new long[inputCount];
    this.warnIOTimeThreshold = warnIOTimeThreshold;
  }

  public int getOperatorId(){
    return operatorId;
  }

  public int getOperatorType(){
    return operatorType;
  }

  private String assertionError(String msg){
    return String.format("Failure while %s for operator id %d. Currently have currentState:%s savedState:%s", msg, operatorId, currentState.name(), savedState.name());
  }
  /**
   * OperatorStats merger - to merge stats from other OperatorStats
   * this is needed in case some processing is multithreaded that needs to have
   * separate OperatorStats to deal with
   * WARN - this will only work for metrics that can be added
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

  /**
   * Clear stats
   */
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
    assert currentState == State.SETUP :  assertionError("stopping setup");
    stopState();
    startState(State.PROCESSING);
  }

  public void startProcessing() {
    assert currentState == State.NONE :  assertionError("starting processing");
    startState(State.PROCESSING);
  }

  public void stopProcessing() {
    assert currentState == State.PROCESSING : assertionError("stopping processing");
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
    this.stateNanos[State.WAIT.ordinal()]+=nanos;
    this.stateNanos[State.PROCESSING.ordinal()]-=nanos;
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

  public OperatorProfile getProfile() {
    return getProfile(false);
  }

  public OperatorProfile getProfile(boolean withDetails) {
    final OperatorProfile.Builder b = OperatorProfile //
        .newBuilder() //
        .setOperatorType(operatorType) //
        .setOperatorId(operatorId) //
        .setSetupNanos(getSetupNanos()) //
        .setProcessNanos(getProcessingNanos())
        .setWaitNanos(getWaitNanos());

    if (allocator != null) {
      b.setPeakLocalMemoryAllocated(allocator.getPeakMemoryAllocation());
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

  public void addStreamProfile(OperatorProfile.Builder builder) {
    for(int i = 0; i < recordsReceivedByInput.length; i++){
      builder.addInputProfile(
          StreamProfile.newBuilder()
              .setBatches(batchesReceivedByInput[i])
              .setRecords(recordsReceivedByInput[i])
              .setSize(sizeInBytesReceivedByInput[i])
      );
    }
  }

  private class LongProc implements IntLongProcedure {

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

  private class DoubleProc implements IntDoubleProcedure {
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

  public void addLongStat(MetricDef metric, long value){
    longMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void addDoubleStat(MetricDef metric, double value){
    doubleMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void setLongStat(MetricDef metric, long value){
    longMetrics.put(metric.metricId(), value);
  }

  public long getLongStat(MetricDef metric){
    return longMetrics.get(metric.metricId());
  }

  public void setDoubleStat(MetricDef metric, double value){
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

  @Override
  public String toString(){
    String[] names = OperatorMetricRegistry.getMetricNames(operatorType);
    StringBuilder sb = new StringBuilder();

    final V2_AsciiTable outputTable = new V2_AsciiTable();
    outputTable.addRule();
    outputTable.addRow(String.format("Metrics for operator %s", CoreOperatorType.values()[operatorType]), String.format("id: %d.", operatorId));
    outputTable.addRule();
    outputTable.addRow("metric", "value");
    outputTable.addRow("Setup time", NumberFormat.getInstance().format(getSetupNanos()) + " ns");
    outputTable.addRow("Processing time", NumberFormat.getInstance().format(getProcessingNanos()) + " ns");

    for(int i =0; i < inputCount; i++){
      outputTable.addRow(String.format("Input[%d] Records", i) , NumberFormat.getInstance().format(recordsReceivedByInput[i]) + " records");
    }


    if(!longMetrics.isEmpty() || !doubleMetrics.isEmpty()){
      outputTable.addRule();
      outputTable.addRow("Custom Metrics","");
      outputTable.addRule();

      for(int i =0; i < names.length; i++){
        if(longMetrics.containsKey(i)){
          long value = longMetrics.get(i);
          if(value != 0){
            outputTable.addRow(names[i], NumberFormat.getInstance().format(value));
          }
        }else if(doubleMetrics.containsKey(i)){
          double value = doubleMetrics.get(i);
          if(value != 0){
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
      if(operatorStats == null || path == null) {
        return NO_OP_RECORDER;
      }
      else {
        return operatorStats.createMetadataWaitRecorder(path.toString(), OperatorStats.getWaitRecorder(operatorStats));
      }
  }

  private void updateIOStats(IOStats ioStats, long elapsed, String filePath, long n, long offset){
    if (ioStats == null) {
      return;
    }

    ioStats.minIOTime.getAndAccumulate(elapsed, Math::min);
    ioStats.maxIOTime.getAndAccumulate(elapsed, Math::max);
    ioStats.totalIOTime.addAndGet(elapsed);
    ioStats.numIO.incrementAndGet();

    if (elapsed >= TimeUnit.MILLISECONDS.toNanos(warnIOTimeThreshold)) {
      synchronized (ioStats.slowIOInfoList) {
        ioStats.slowIOInfoList.add(SlowIOInfo.newBuilder()
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
    updateIOStats(metadataReadIOStats, elapsed, path, 0, 0 );
  }

}
