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
package com.dremio.sabot.op.join.vhash.spill;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.AssertionUtil;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.copier.CopierFactory;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.dremio.sabot.op.join.vhash.spill.list.ProbeBuffers;
import com.dremio.sabot.op.join.vhash.spill.partition.CanSwitchToSpilling;
import com.dremio.sabot.op.join.vhash.spill.partition.MultiPartition;
import com.dremio.sabot.op.join.vhash.spill.partition.Partition;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.replay.JoinRecursiveReplayer;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class VectorizedSpillingHashJoinOperator implements DualInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedSpillingHashJoinOperator.class);
  /*
   * The computation is as follows :
   * 1. spillPagePool (5 pages) = 5*256K = 1.2M
   * 2. 8 partitions (each use 2.5M) = 8*2.5M = 20M
   *    2a. hash table 1M
   *    2b. linked list & slicer (2 pages), 512K
   *    2c. misc buffers 1M
   * 3. hasher (dummy hash table) = 1M
   * 4. ProbeBuffers = 1.25M
   */
  public static final int MIN_RESERVE = 27 * 1024 * 1024;

  private final OperatorContext context;
  private final HashJoinPOP config;

  private final List<FieldVector> buildVectorsToValidate = new ArrayList<>();
  private final List<FieldVector> probeVectorsToValidate = new ArrayList<>();

  private final RuntimeFilterManager filterManager;

  private final VectorContainer outgoing;
  private final int targetOutputBatchSize;

  private Partition partition;
  private JoinSetupParams joinSetupParams;
  private final Stopwatch pivotBuildWatch = Stopwatch.createUnstarted();
  private final Stopwatch pivotProbeWatch = Stopwatch.createUnstarted();

  private State state = State.NEEDS_SETUP;
  private boolean debugInsertion = false;
  private long outputRecords = 0;
  private int runtimeValFilterCap;
  private FixedBlockVector pivotFixedBlock;
  private VariableBlockVector pivotVarBlock;
  private JoinRecursiveReplayer joinReplayer;
  private enum InternalState {
    BUILD,
    PROBE_IN,
    PROBE_OUT,
    PROJECT_NON_MATCHES,
    REPLAY,
    DONE
  };
  private InternalState internalState = InternalState.BUILD;
  private static final boolean DEBUG = AssertionUtil.isAssertionsEnabled();

  public VectorizedSpillingHashJoinOperator(OperatorContext context, HashJoinPOP popConfig) throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.outgoing = new VectorContainer(context.getFragmentOutputAllocator());
    final Set<Integer> allMinorFragments = context.getAssignments().stream().flatMap(a -> a.getMinorFragmentIdList().stream())
              .collect(Collectors.toSet()); // all minor fragments across all assignments

    targetOutputBatchSize = context.getTargetBatchSize();
    // TODO ravindra: re-enable runtime filters
    runtimeValFilterCap = (int) context.getOptions().getOption(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE);
    filterManager = new RuntimeFilterManager(context.getAllocator(), runtimeValFilterCap, allMinorFragments);
  }

  @Override
  public State getState() {
    return state;
  }

  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
    state.is(State.NEEDS_SETUP);

    outgoing.addSchema(right.getSchema());
    outgoing.addSchema(left.getSchema());
    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(targetOutputBatchSize);

    final List<FieldVectorPair> buildFields = new ArrayList<>();
    final List<FieldVectorPair> probeFields = new ArrayList<>();
    /* The build side key fields in output, the pivoted keys will be unpivoted to the vectors of key fields for non matched records
     * It's different from buildFields because buildFields used to pivot for incoming batch, but buildOutputFields
     * is used to unpivot for output
     */
    final List<FieldVectorPair> buildOutputFields = new ArrayList<>();
    final BitSet requiredBits = new BitSet();
    // Used to indicate which field is key and will not be added to hyper container
    final BitSet isKeyBits = new BitSet(right.getSchema().getFieldCount());
    /* The probe side key fields, which will be used to copied to the build side key fields for output
     * The key fields are not maintained in hyper container, so the probe side key field vectors
     * will be copied to build side key field vectors for matched records.
     */
    final List<FieldVector> probeKeyFieldVectorList = new ArrayList<>();
    for (int i = 0;i < right.getSchema().getFieldCount(); i++) {
      probeKeyFieldVectorList.add(null);
    }

    ImmutableBitSet buildNonKeyFieldsBitset = ImmutableBitSet.range(0, right.getSchema().getFieldCount());

    int fieldIndex = 0;
    for(JoinCondition c : config.getConditions()){
      final FieldVector build = getField(right, c.getRight());
      buildFields.add(new FieldVectorPair(build, build));
      final FieldVector probe = getField(left, c.getLeft());
      probeFields.add(new FieldVectorPair(probe, probe));

      /* Collect the corresponding probe side field vectors for build side keys
       */
      int fieldId = getFieldId(outgoing, c.getRight());
      probeKeyFieldVectorList.set(fieldId, probe);
      // The field is key in build side and its vectors will not be added to hyper container
      isKeyBits.set(fieldId);
      buildNonKeyFieldsBitset = buildNonKeyFieldsBitset.clear(fieldId);

      // Collect the build side keys in output, which will be used to create PivotDef for unpivot in projectBuildNonMatches
      final FieldVector buildOutput = outgoing.getValueAccessorById(FieldVector.class, fieldId).getValueVector();
      buildOutputFields.add(new FieldVectorPair(buildOutput, buildOutput));

      final Comparator joinComparator = JoinUtils.checkAndReturnSupportedJoinComparator(c);
      switch(joinComparator){
      case EQUALS:
        requiredBits.set(fieldIndex);
        break;
      case IS_NOT_DISTINCT_FROM:
        // null keys are equal
        break;
      case NONE:
        throw new UnsupportedOperationException();
      default:
        break;

      }

      fieldIndex++;
    }

    for(VectorWrapper<?> w : right){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(v instanceof VarBinaryVector || v instanceof VarCharVector){
        buildVectorsToValidate.add(v);
      }
    }

    for(VectorWrapper<?> w : left){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(v instanceof VarBinaryVector || v instanceof VarCharVector){
        probeVectorsToValidate.add(v);
      }
    }

    int i = 0;
    final List<FieldVector> probeOutputs = new ArrayList<>();
    final List<FieldVector> probeIncomingKeys = new ArrayList<>();
    final List<FieldVector> buildOutputKeys = new ArrayList<>();
    final List<FieldVector> buildOutputCarryOvers = new ArrayList<>();
    for(VectorWrapper<?> w : outgoing){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(i < right.getSchema().getFieldCount()){
        if (isKeyBits.get(i)) {
          /* The corresponding field is key, so the fields in build side and probe side will
           * be added to probeIncomingKeys and buildOutputKeys. They will be used to create
           * copier to copy the keys from probe side to build side for output.
           * The field in build side will not be added to buildOutputs because we will unpivot them to output.
           */
          probeIncomingKeys.add(probeKeyFieldVectorList.get(i));
          buildOutputKeys.add(v);
        } else {
          buildOutputCarryOvers.add(v);
        }
      } else {
        probeOutputs.add(v);
      }
      i++;
    }

    PivotDef probePivot = PivotBuilder.getBlockDefinition(probeFields);
    PivotDef buildKeyPivot = PivotBuilder.getBlockDefinition(buildFields);

    NullComparator comparator = new NullComparator(requiredBits, probePivot.getBitCount());

    Preconditions.checkArgument(probePivot.getBlockWidth() == buildKeyPivot.getBlockWidth(),
      "Block width of build [%s] and probe pivots are not equal [%s].",
      buildKeyPivot.getBlockWidth(), probePivot.getBlockWidth());
    Preconditions.checkArgument(probePivot.getVariableCount() == buildKeyPivot.getVariableCount(),
      "Variable column count of build [%s] and probe pivots are not equal [%s].",
      buildKeyPivot.getVariableCount(), probePivot.getVariableCount());
    Preconditions.checkArgument(probePivot.getBitCount() == buildKeyPivot.getBitCount(),
      "Bit width of build [%s] and probe pivots are not equal [%s].",
      buildKeyPivot.getBitCount(), probePivot.getBitCount());

    // Create the PivotDef for unpivot in projectBuildNonMatches
    PivotDef buildKeyUnpivot = PivotBuilder.getBlockDefinition(buildOutputFields);

    // Create the hyper container with only the carry-over columns.
    final BatchSchema rightMaskedSchema = new BatchSchema(
      buildOutputCarryOvers.stream()
        .map(ValueVector::getField)
        .collect(Collectors.toList()));
    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

    final BufferAllocator allocator = context.getAllocator();
    try (AutoCloseables.RollbackCloseable rc = new AutoCloseables.RollbackCloseable()) {
      pivotFixedBlock = rc.add(new FixedBlockVector(allocator, buildKeyPivot.getBlockWidth()));
      pivotVarBlock = rc.add(new VariableBlockVector(allocator, buildKeyPivot.getVariableCount()));

      int maxInputBatchSize = (int)context.getOptions().getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
      ProbeBuffers probeBuffers = rc.add(new ProbeBuffers(maxInputBatchSize, context.getAllocator()));

      final ExecProtos.FragmentHandle fragmentHandle = context.getFragmentHandle();
      final String id = String.format("joinspill-%s.%s.%s.%s",
        QueryIdHelper.getQueryId(fragmentHandle.getQueryId()), fragmentHandle.getMajorFragmentId(), fragmentHandle.getMinorFragmentId(),
        config.getProps().getOperatorId());


      final SpillManager spillManager = rc.add(new SpillManager(context.getConfig(), context.getOptions(), id, null,
        context.getSpillService(), "join spilling", context.getStats()));

      // This pool is shared by all partitions, can be used only for spilling (to release memory).
      // 3 pages required by the replayer, and 2 pages required if any partition spills while replay is in-progress.
      PagePool spillPool = rc.add(new PagePool(allocator, PagePool.DEFAULT_PAGE_SIZE, 5));

      joinSetupParams = new JoinSetupParams(
        context.getOptions(),
        context.getConfig(),
        context.getAllocator(),
        context.getFragmentOutputAllocator(),
        pivotFixedBlock,
        pivotVarBlock,
        config.getJoinType(),
        right,
        left,
        buildKeyPivot,
        buildKeyUnpivot,
        buildOutputKeys,
        buildOutputCarryOvers,
        rightMaskedSchema,
        buildNonKeyFieldsBitset,
        comparator,
        probePivot,
        probeIncomingKeys,
        probeOutputs,
        probeBuffers,
        spillManager,
        spillPool);

      partition = rc.add(new MultiPartition(joinSetupParams, CopierFactory.getInstance(context.getConfig(), context.getOptions())));

      joinReplayer = rc.add(new JoinRecursiveReplayer(joinSetupParams, partition, outgoing, targetOutputBatchSize));

      rc.commit();
    }

    Preconditions.checkState(allocator.getAllocatedMemory() <= MIN_RESERVE);
    computeExternalState(InternalState.BUILD);
    return outgoing;
  }

  // Get ids for a field
  private int[] getFieldIds(VectorAccessible accessible, LogicalExpression expr){
    final LogicalExpression materialized = context.getClassProducer().materialize(expr, accessible);
    if(!(materialized instanceof ValueVectorReadExpression)){
      throw new IllegalStateException("Only direct references allowed.");
    }
    return ((ValueVectorReadExpression) materialized).getFieldId().getFieldIds();
  }

  // Get the field vector of a field
  private FieldVector getField(VectorAccessible accessible, LogicalExpression expr){
    return accessible.getValueAccessorById(FieldVector.class, getFieldIds(accessible, expr)).getValueVector();
  }

  // Get the id of a field
  private int getFieldId(VectorAccessible accessible, LogicalExpression expr){
    return getFieldIds(accessible, expr)[0];
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for (FieldVector v : buildVectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }

    pivotBuildWatch.start();
    pivotFixedBlock.reset();
    pivotVarBlock.reset();
    Pivots.pivot(joinSetupParams.getBuildKeyPivot(), records, pivotFixedBlock, pivotVarBlock);
    pivotBuildWatch.stop();

    consumePivotedDataRight(records);
    updateStats();

    computeExternalState();
  }

  private void consumePivotedDataRight(int records) throws Exception {
    int ret = partition.buildPivoted(records);
    Preconditions.checkState(ret == records);
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (partition.isBuildSideEmpty() &&
      !(joinSetupParams.getJoinType() == JoinRelType.LEFT || joinSetupParams.getJoinType() == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      computeExternalState(InternalState.DONE);
      return;
    }

    computeExternalState(InternalState.PROBE_IN);
    if (DEBUG && joinSetupParams.getOptions().getOption(HashJoinOperator.TEST_SPILL_MODE).equals("buildAndReplay")) {
      switchToSpilling(true);
      computeExternalState();
    }
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for (FieldVector v : probeVectorsToValidate) {
      VariableLengthValidator.validateVariable(v, records);
    }

    pivotProbeWatch.start();
    pivotFixedBlock.reset();
    pivotVarBlock.reset();
    Pivots.pivot(joinSetupParams.getProbeKeyPivot(), records, pivotFixedBlock, pivotVarBlock);
    pivotProbeWatch.stop();

    computeExternalState(InternalState.PROBE_OUT);
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    updateStats();

    // highest priority to memory releasers.
    MultiMemoryReleaser releaser = joinSetupParams.getMultiMemoryReleaser();
    if (!releaser.isFinished()) {
      releaser.run();
      computeExternalState();
      return 0;
    }

    if (internalState == InternalState.REPLAY) {
      // if all incoming data has been processed, do spill replay.
      int ret = joinReplayer.run();
      Preconditions.checkState(ret >= 0);

      outgoing.setAllCount(ret);
      outputRecords += ret;
      computeExternalState(joinReplayer.isFinished() ? InternalState.DONE : InternalState.REPLAY);
      return ret;
    } else if (internalState == InternalState.PROBE_OUT) {
      final int probedRecords = partition.probePivoted(joinSetupParams.getLeft().getRecordCount(), 0, targetOutputBatchSize - 1);
      outputRecords += Math.abs(probedRecords);
      if (probedRecords > -1) {
        computeExternalState(InternalState.PROBE_IN);
        return outgoing.setAllCount(probedRecords);
      } else {
        // we didn't finish everything, will produce again.
        computeExternalState(InternalState.PROBE_OUT);
        return outgoing.setAllCount(-probedRecords);
      }
    } else {
      Preconditions.checkState(internalState == InternalState.PROJECT_NON_MATCHES);
      final int unmatched = partition.projectBuildNonMatches(0, targetOutputBatchSize - 1);
      outputRecords += Math.abs(unmatched);
      if (unmatched > -1) {
        checkAndSwitchToReplayMode();
        return outgoing.setAllCount(unmatched);
      } else {
        // remainder, need to output again.
        computeExternalState();
        return outgoing.setAllCount(-unmatched);
      }
    }
  }

  @Override
  public void noMoreToConsumeLeft() throws Exception {
    state.is(State.CAN_CONSUME_L);

    if (joinSetupParams.getJoinType() == JoinRelType.FULL || joinSetupParams.getJoinType() == JoinRelType.RIGHT){
      // if we need to project build records that didn't match, make sure we do so.
      computeExternalState(InternalState.PROJECT_NON_MATCHES);
    } else {
      checkAndSwitchToReplayMode();
    }
  }

  private void checkAndSwitchToReplayMode() {
    Preconditions.checkState(internalState != InternalState.REPLAY);
    // all incoming batches have been processed, including projection of non-matches. check and switch to
    // replay mode if needed.
    partition.reset();
    computeExternalState(joinReplayer.isFinished() ? InternalState.DONE : InternalState.REPLAY);
  }

  private void switchToSpilling(boolean switchAll) {
    if (partition instanceof CanSwitchToSpilling) {
      ((CanSwitchToSpilling) partition).switchToSpilling(switchAll);
    }
  }

  // switch to the new internal state & then, compute the external state.
  private void computeExternalState(InternalState newState) {
    if (internalState != newState) {
      switch (internalState) {
        case BUILD:
          Preconditions.checkState(newState == InternalState.PROBE_IN || newState == InternalState.DONE,
            "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROBE_IN:
          Preconditions.checkState(newState == InternalState.PROBE_OUT ||
              newState == InternalState.PROJECT_NON_MATCHES ||
              newState == InternalState.REPLAY ||
              newState == InternalState.DONE,
            "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROBE_OUT:
          Preconditions.checkState(newState == InternalState.PROBE_IN || newState == InternalState.PROJECT_NON_MATCHES || newState == InternalState.DONE,
            "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROJECT_NON_MATCHES:
          Preconditions.checkState(newState == InternalState.REPLAY || newState == InternalState.DONE,
            "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case REPLAY:
          Preconditions.checkState(newState == InternalState.DONE,
            "unexpected transition from state " + internalState + " to new state " + newState);
          break;
      }
      internalState = newState;
    }
    computeExternalState();
  }

  private void computeExternalState() {
    if (!joinSetupParams.getMultiMemoryReleaser().isFinished()) {
      state = State.CAN_PRODUCE;
      return;
    }

    switch (internalState) {
      case BUILD:
        state = State.CAN_CONSUME_R;
        break;

      case PROBE_IN:
        state = State.CAN_CONSUME_L;
        break;

      case PROBE_OUT:
      case REPLAY:
      case PROJECT_NON_MATCHES:
        state = State.CAN_PRODUCE;
        break;

      case DONE:
        state = State.DONE;
        break;

      default:
        Preconditions.checkState(false, "unexpected state " + internalState);
        break;
    }
  }

  private void updateStats(){
    final OperatorStats stats = context.getStats();
    final TimeUnit ns = TimeUnit.NANOSECONDS;

    Partition.Stats partitionStats = null;
    if (partition != null) {
      partitionStats = partition.getStats();
    }
    if (partitionStats != null) {
      stats.setLongStat(Metric.NUM_ENTRIES, partitionStats.getBuildNumEntries());
      stats.setLongStat(Metric.NUM_BUCKETS,  partitionStats.getBuildNumBuckets());
      stats.setLongStat(Metric.NUM_RESIZING, partitionStats.getBuildNumResizing());
      stats.setLongStat(Metric.RESIZING_TIME_NANOS, partitionStats.getBuildResizingTimeNanos());
      stats.setLongStat(Metric.PIVOT_TIME_NANOS, pivotBuildWatch.elapsed(ns));
      stats.setLongStat(Metric.INSERT_TIME_NANOS, partitionStats.getBuildInsertTimeNanos());
      stats.setLongStat(Metric.HASHCOMPUTATION_TIME_NANOS, partitionStats.getBuildHashComputationTimeNanos());
      stats.setLongStat(Metric.RUNTIME_FILTER_DROP_COUNT, filterManager.getFilterDropCount());
      stats.setLongStat(Metric.RUNTIME_COL_FILTER_DROP_COUNT, filterManager.getSubFilterDropCount());
      stats.setLongStat(Metric.LINK_TIME_NANOS, partitionStats.getBuildLinkTimeNanos());
      stats.setLongStat(Metric.BUILD_CARRYOVER_COPY_NANOS, partitionStats.getBuildCarryOverCopyNanos());
      stats.setLongStat(Metric.BUILD_COPY_NANOS, partitionStats.getBuildKeyCopyNanos());
      stats.setLongStat(Metric.BUILD_COPY_NOMATCH_NANOS, partitionStats.getBuildCopyNonMatchNanos());
      stats.setLongStat(Metric.UNMATCHED_BUILD_KEY_COUNT, partitionStats.getBuildUnmatchedKeyCount());

      stats.setLongStat(Metric.PROBE_FIND_NANOS, partitionStats.getProbeFindTimeNanos());
      stats.setLongStat(Metric.PROBE_PIVOT_NANOS, pivotProbeWatch.elapsed(ns));
      stats.setLongStat(Metric.PROBE_LIST_NANOS, partitionStats.getProbeListTimeNanos());
      stats.setLongStat(Metric.PROBE_HASHCOMPUTATION_TIME_NANOS, partitionStats.getProbeHashComputationTime());
      stats.setLongStat(Metric.PROBE_COPY_NANOS, partitionStats.getProbeCopyNanos());
      stats.setLongStat(Metric.UNMATCHED_PROBE_COUNT, partitionStats.getProbeUnmatchedKeyCount());
      stats.setLongStat(Metric.OUTPUT_RECORDS, outputRecords);
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @Override
  public void close() throws Exception {
    updateStats();
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(filterManager);
    autoCloseables.add(joinReplayer);
    autoCloseables.add(outgoing);
    autoCloseables.add(partition);
    autoCloseables.add(joinSetupParams);
    AutoCloseables.close(autoCloseables);
  }
}
