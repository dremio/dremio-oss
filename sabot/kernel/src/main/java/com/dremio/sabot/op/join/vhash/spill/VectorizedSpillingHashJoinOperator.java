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
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.dremio.sabot.op.join.vhash.spill.partition.MultiPartition;
import com.dremio.sabot.op.join.vhash.spill.partition.Partition;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class VectorizedSpillingHashJoinOperator implements DualInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedSpillingHashJoinOperator.class);

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
  private boolean finishedProbe = false;
  private boolean debugInsertion = false;
  private long outputRecords = 0;
  private int runtimeValFilterCap;
  private FixedBlockVector pivotFixedBlock;
  private VariableBlockVector pivotVarBlock;

  public VectorizedSpillingHashJoinOperator(OperatorContext context, HashJoinPOP popConfig) throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.outgoing = new VectorContainer(context.getAllocator());
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

    Preconditions.checkArgument(probePivot.getBlockWidth() == buildKeyPivot.getBlockWidth(), "Block width of build [%s] and probe pivots are not equal [%s].", buildKeyPivot.getBlockWidth(), probePivot.getBlockWidth());
    Preconditions.checkArgument(probePivot.getBitCount() == buildKeyPivot.getBitCount(), "Bit width of build [%s] and probe pivots are not equal [%s].", buildKeyPivot.getBitCount(), probePivot.getBitCount());

    // Create the PivotDef for unpivot in projectBuildNonMatches
    PivotDef buildKeyUnpivot = PivotBuilder.getBlockDefinition(buildOutputFields);

    ImmutableBitSet carryAlongFieldsBitset = ImmutableBitSet.range(0, right.getSchema().getFieldCount());
    for (int field = 0; field < right.getSchema().getFieldCount(); field++) {
      if (isKeyBits.get(field)) {
        carryAlongFieldsBitset = carryAlongFieldsBitset.clear(field);
      }
    }

    // Create the hyper container with only the carry-over columns.
    final BatchSchema rightMaskedSchema = new BatchSchema(
      buildOutputCarryOvers.stream()
        .map(ValueVector::getField)
        .collect(Collectors.toList()));
    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

    final BufferAllocator allocator = context.getAllocator();
    pivotFixedBlock = new FixedBlockVector(allocator, buildKeyPivot.getBlockWidth());
    pivotVarBlock = new VariableBlockVector(allocator, buildKeyPivot.getVariableCount());

    BufferAllocator buildAllocator = context.getAllocator().newChildAllocator("join_build", 0 /*reservation*/,
      Long.MAX_VALUE);
    joinSetupParams = new JoinSetupParams(
      context.getOptions(),
      context.getAllocator(),
      buildAllocator,
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
      carryAlongFieldsBitset,
      comparator,
      probePivot,
      probeIncomingKeys,
      probeOutputs);
    partition = new MultiPartition(joinSetupParams);
    state = State.CAN_CONSUME_R;
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
  }

  private void consumePivotedDataRight(int records) throws Exception {
    partition.buildPivoted(records);
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (partition.isBuildSideEmpty() &&
      !(joinSetupParams.getJoinType() == JoinRelType.LEFT || joinSetupParams.getJoinType() == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      state = State.DONE;
      return;
    }

    state = State.CAN_CONSUME_L;
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

    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    updateStats();

    if(!finishedProbe){
      final int probedRecords = partition.probePivoted(joinSetupParams.getLeft().getRecordCount(), 0, targetOutputBatchSize - 1);
      outputRecords += Math.abs(probedRecords);
      if (probedRecords > -1) {
        state = State.CAN_CONSUME_L;
        return outgoing.setAllCount(probedRecords);
      } else {
        // we didn't finish everything, will produce again.
        state = State.CAN_PRODUCE;
        return outgoing.setAllCount(-probedRecords);
      }
    } else {
      final int unmatched = partition.projectBuildNonMatches(0, targetOutputBatchSize - 1);
      outputRecords += Math.abs(unmatched);
      if (unmatched > -1) {
        state = State.DONE;
        return outgoing.setAllCount(unmatched);
      } else {
        // remainder, need to output again.
        return outgoing.setAllCount(-unmatched);
      }
    }
  }

  @Override
  public void noMoreToConsumeLeft() throws Exception {
    state.is(State.CAN_CONSUME_L);

    finishedProbe = true;
    if (joinSetupParams.getJoinType() == JoinRelType.FULL || joinSetupParams.getJoinType() == JoinRelType.RIGHT){
      // if we need to project build records that didn't match, make sure we do so.
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
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
    autoCloseables.add(outgoing);
    autoCloseables.add(partition);
    if (joinSetupParams != null) {
      autoCloseables.add(joinSetupParams.getPivotedFixedBlock());
      autoCloseables.add(joinSetupParams.getPivotedVariableBlock());
      autoCloseables.addAll(joinSetupParams.getProbeIncomingKeys());
      autoCloseables.addAll(joinSetupParams.getBuildOutputKeys());
      autoCloseables.add(joinSetupParams.getBuildAllocator());
    }
    AutoCloseables.close(autoCloseables);
  }
}
