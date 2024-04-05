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

import static com.dremio.exec.ExecConstants.ENABLE_SPILLABLE_OPERATORS;
import static com.dremio.sabot.op.join.vhash.PartitionColFilters.BLOOMFILTER_MAX_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.ht2.BoundedPivots;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.hash.HashJoinOperator;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;
import com.dremio.sabot.op.join.vhash.RuntimeFilterUtil;
import com.dremio.sabot.op.join.vhash.spill.list.ProbeBuffers;
import com.dremio.sabot.op.join.vhash.spill.partition.CanSwitchToSpilling;
import com.dremio.sabot.op.join.vhash.spill.partition.MultiPartition;
import com.dremio.sabot.op.join.vhash.spill.partition.Partition;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.replay.JoinRecursiveReplayer;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.Operator.ShrinkableOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

@Options
public class VectorizedSpillingHashJoinOperator implements DualInputOperator, ShrinkableOperator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VectorizedSpillingHashJoinOperator.class);

  /*
   * The computation is as follows :
   * 1. spillPagePool (9 pages) = 9*256K = 2.25M
   * 2. pivotFixedBlock (1 page) = 256K
   * 3. pivotVarBlock (1 page) = 256K
   * 4. Probe buffers - 80K
   * 5. 8/4 byte hash values - 48K
   * 6. 8 partitions - 3.2MB
   *    Control Blocks - 128K
   *    Native HT KeyReader buffer - 256K (only with runtime filters present)
   *    Ordinals buffer - 16K
   *    sv2 bufffer - 8k
   * 7. Runtime filters - 1MB+
   */
  public static int MIN_RESERVE = 12 * 1024 * 1024;
  private final String OOM_SPILL = "OOM_SPILL";
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
  // We use BoundedPivots and hence, may be only pivot part of the batch in each iteration.
  private final ProbePivotCursor probePivotCursor = new ProbePivotCursor();

  private final Map<String, String> build2ProbeKeyMap = new HashMap<>();

  private State state = State.NEEDS_SETUP;
  private boolean debugInsertion = false;
  private long outputRecords = 0;
  private FixedBlockVector pivotFixedBlock;
  private VariableBlockVector pivotVarBlock;
  private JoinRecursiveReplayer joinReplayer;

  private enum InternalState {
    BUILD,
    PROBE_IN,
    PROBE_PIVOT_AND_OUT,
    PROBE_OUT,
    PROJECT_NON_MATCHES,
    REPLAY,
    DONE
  }

  private InternalState internalState = InternalState.BUILD;
  private static final boolean DEBUG = VM.areAssertsEnabled();
  private long reservedPreallocation;

  private final boolean runtimeFilterEnabled;
  private PartitionColFilters partitionColFilters = null;
  private NonPartitionColFilters nonPartitionColFilters = null;
  private final Stopwatch filterBuildTime = Stopwatch.createUnstarted();
  private int oobDropUnderThreshold;
  private int oobDropNoVictim;
  private int oobDropLocal;
  private int oobDropWrongState;
  private int oobSpills;

  public static final BooleanValidator OOB_SPILL_TRIGGER_ENABLED =
      new BooleanValidator("exec.op.join.spill.oob_trigger_enabled", true);
  public static final DoubleValidator OOB_SPILL_TRIGGER_FACTOR =
      new RangeDoubleValidator("exec.op.join.spill.oob_trigger_factor", 0.0d, 10.0d, .75d);
  public static final DoubleValidator OOB_SPILL_TRIGGER_HEADROOM_FACTOR =
      new RangeDoubleValidator("exec.op.join.spill.oob_trigger_headroom_factor", 0.0d, 10.0d, .2d);
  public static boolean oobSpillNotificationsEnabled;

  public VectorizedSpillingHashJoinOperator(OperatorContext context, HashJoinPOP popConfig)
      throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.outgoing = new VectorContainer(context.getFragmentOutputAllocator());
    targetOutputBatchSize = context.getTargetBatchSize();

    final Set<Integer> allMinorFragments =
        context.getAssignments().stream()
            .flatMap(a -> a.getMinorFragmentIdList().stream())
            .collect(Collectors.toSet()); // all minor fragments across all assignments

    runtimeFilterEnabled =
        RuntimeFilterUtil.shouldFragBuildRuntimeFilters(
            config.getRuntimeFilterInfo(), context.getFragmentHandle().getMinorFragmentId());
    filterManager =
        new RuntimeFilterManager(
            context.getAllocator(),
            RuntimeFilterUtil.getRuntimeValFilterCap(context),
            allMinorFragments);
    // not sending oob spill notifications to sibling minor fragments when MemoryArbiter is ON
    oobSpillNotificationsEnabled =
        !(context.getOptions().getOption(ENABLE_SPILLABLE_OPERATORS))
            && context.getOptions().getOption(OOB_SPILL_TRIGGER_ENABLED);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
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
    for (int i = 0; i < right.getSchema().getFieldCount(); i++) {
      probeKeyFieldVectorList.add(null);
    }

    ImmutableBitSet buildNonKeyFieldsBitset =
        ImmutableBitSet.range(0, right.getSchema().getFieldCount());

    int fieldIndex = 0;
    for (JoinCondition c : config.getConditions()) {
      final FieldVector build = getField(right, c.getRight());
      buildFields.add(new FieldVectorPair(build, build));
      final FieldVector probe = getField(left, c.getLeft());
      probeFields.add(new FieldVectorPair(probe, probe));
      build2ProbeKeyMap.put(build.getField().getName(), probe.getField().getName());

      /* Collect the corresponding probe side field vectors for build side keys
       */
      int fieldId = getFieldId(outgoing, c.getRight());
      probeKeyFieldVectorList.set(fieldId, probe);
      // The field is key in build side and its vectors will not be added to hyper container
      isKeyBits.set(fieldId);
      buildNonKeyFieldsBitset = buildNonKeyFieldsBitset.clear(fieldId);

      // Collect the build side keys in output, which will be used to create PivotDef for unpivot in
      // projectBuildNonMatches
      final FieldVector buildOutput =
          outgoing.getValueAccessorById(FieldVector.class, fieldId).getValueVector();
      buildOutputFields.add(new FieldVectorPair(buildOutput, buildOutput));

      final Comparator joinComparator = JoinUtils.checkAndReturnSupportedJoinComparator(c);
      switch (joinComparator) {
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

    for (VectorWrapper<?> w : right) {
      final FieldVector v = (FieldVector) w.getValueVector();
      if (v instanceof VarBinaryVector || v instanceof VarCharVector) {
        buildVectorsToValidate.add(v);
      }
    }

    for (VectorWrapper<?> w : left) {
      final FieldVector v = (FieldVector) w.getValueVector();
      if (v instanceof VarBinaryVector || v instanceof VarCharVector) {
        probeVectorsToValidate.add(v);
      }
    }

    int i = 0;
    final List<FieldVector> probeOutputs = new ArrayList<>();
    final List<FieldVector> probeIncomingKeys = new ArrayList<>();
    final List<FieldVector> buildOutputKeys = new ArrayList<>();
    final List<FieldVector> buildOutputCarryOvers = new ArrayList<>();
    for (VectorWrapper<?> w : outgoing) {
      final FieldVector v = (FieldVector) w.getValueVector();
      if (i < right.getSchema().getFieldCount()) {
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

    Preconditions.checkState(probeFields.size() != 0);
    Preconditions.checkState(buildFields.size() != 0);
    PivotDef probePivot = PivotBuilder.getBlockDefinition(probeFields);
    PivotDef buildKeyPivot = PivotBuilder.getBlockDefinition(buildFields);

    NullComparator comparator = new NullComparator(requiredBits, probePivot.getBitCount());

    Preconditions.checkArgument(
        probePivot.getBlockWidth() == buildKeyPivot.getBlockWidth(),
        "Block width of build [%s] and probe pivots are not equal [%s].",
        buildKeyPivot.getBlockWidth(),
        probePivot.getBlockWidth());
    Preconditions.checkArgument(
        probePivot.getVariableCount() == buildKeyPivot.getVariableCount(),
        "Variable column count of build [%s] and probe pivots are not equal [%s].",
        buildKeyPivot.getVariableCount(),
        probePivot.getVariableCount());
    Preconditions.checkArgument(
        probePivot.getBitCount() == buildKeyPivot.getBitCount(),
        "Bit width of build [%s] and probe pivots are not equal [%s].",
        buildKeyPivot.getBitCount(),
        probePivot.getBitCount());

    // Create the PivotDef for unpivot in projectBuildNonMatches
    PivotDef buildKeyUnpivot = PivotBuilder.getBlockDefinition(buildOutputFields);

    // Create the hyper container with only the carry-over columns.
    final BatchSchema rightMaskedSchema =
        new BatchSchema(
            buildOutputCarryOvers.stream().map(ValueVector::getField).collect(Collectors.toList()));
    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

    final BufferAllocator allocator = context.getAllocator();
    try (AutoCloseables.RollbackCloseable rc = new AutoCloseables.RollbackCloseable()) {
      int preAllocBufSz = (int) context.getOptions().getOption(HashJoinOperator.PAGE_SIZE);
      int numBlocks = preAllocBufSz / buildKeyPivot.getBlockWidth();
      pivotFixedBlock =
          rc.add(new FixedBlockVector(allocator, buildKeyPivot.getBlockWidth(), numBlocks, false));
      if (buildKeyPivot.getVariableCount() > 0) {
        pivotVarBlock =
            rc.add(
                new VariableBlockVector(
                    allocator, buildKeyPivot.getVariableCount(), preAllocBufSz, false));
      }

      int maxInputBatchSize =
          (int) context.getOptions().getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
      ProbeBuffers probeBuffers =
          rc.add(new ProbeBuffers(maxInputBatchSize, context.getAllocator()));

      final ExecProtos.FragmentHandle fragmentHandle = context.getFragmentHandle();
      final String id =
          String.format(
              "joinspill-%s.%s.%s.%s",
              QueryIdHelper.getQueryId(fragmentHandle.getQueryId()),
              fragmentHandle.getMajorFragmentId(),
              fragmentHandle.getMinorFragmentId(),
              config.getProps().getOperatorId());

      final SpillManager spillManager =
          rc.add(
              new SpillManager(
                  context.getConfig(),
                  context.getOptions(),
                  id,
                  null,
                  context.getSpillService(),
                  "join spilling",
                  context.getStats()));

      // This pool is shared by all partitions, can be used only for spilling (to release memory).
      // - 3 pages required by the replayer
      // - 2 pages required if any partition spills while replay is in-progress.
      // - 4 pages required for merging
      PagePool spillPool =
          rc.add(
              new PagePool(
                  allocator, (int) context.getOptions().getOption(HashJoinOperator.PAGE_SIZE), 9));

      final OOBInfo oobInfo =
          new OOBInfo(
              context.getAssignments(),
              context.getFragmentHandle().getQueryId(),
              context.getFragmentHandle().getMajorFragmentId(),
              config.getProps().getOperatorId(),
              context.getFragmentHandle().getMinorFragmentId(),
              context.getEndpointsIndex(),
              context.getTunnelProvider(),
              oobSpillNotificationsEnabled,
              OOM_SPILL);

      joinSetupParams =
          new JoinSetupParams(
              context,
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
              config.getExtraCondition(),
              build2ProbeKeyMap,
              spillManager,
              spillPool,
              oobInfo,
              config.getProps().getOperatorId(),
              runtimeFilterEnabled);

      partition = rc.add(new MultiPartition(joinSetupParams));

      joinReplayer =
          rc.add(
              new JoinRecursiveReplayer(
                  joinSetupParams, partition, outgoing, targetOutputBatchSize));

      if (runtimeFilterEnabled) {
        List<RuntimeFilterProbeTarget> probeTargets =
            config.getRuntimeFilterInfo().getRuntimeFilterProbeTargets();

        /* Step 1: Create partitionColFilters, i.e BloomFilters, one for each probe target */
        logger.debug("Creating partitionColFilters...");
        partitionColFilters =
            new PartitionColFilters(
                context.getAllocator(),
                probeTargets,
                buildKeyPivot,
                BLOOMFILTER_MAX_SIZE,
                RuntimeFilterUtil.getRuntimeFilterKeyMaxSize(context));
        rc.add(partitionColFilters);

        if (RuntimeFilterUtil.isRuntimeFilterEnabledForNonPartitionedCols(context)) {
          /* Step 2: Create ValueListFilterBuilders, one list (for multi-key) for each probe target */
          logger.debug("Creating nonPartitionColFilters...");
          nonPartitionColFilters =
              new NonPartitionColFilters(
                  context.getAllocator(),
                  probeTargets,
                  buildKeyPivot,
                  RuntimeFilterUtil.getRuntimeValFilterCap(context));
          rc.add(nonPartitionColFilters);
        }
        partition.setFilters(nonPartitionColFilters, partitionColFilters);
      }

      rc.commit();
    }

    if (allocator.getAllocatedMemory() > MIN_RESERVE) {
      logger.warn(
          "MIN_RESERVE {} lower than actual usage {}; allocator:\n{}",
          MIN_RESERVE,
          allocator.getAllocatedMemory(),
          allocator);
    }

    reservedPreallocation = Math.max(allocator.getAllocatedMemory(), MIN_RESERVE);

    computeExternalState(InternalState.BUILD);
    return outgoing;
  }

  // Get ids for a field
  private int[] getFieldIds(VectorAccessible accessible, LogicalExpression expr) {
    final LogicalExpression materialized = context.getClassProducer().materialize(expr, accessible);
    if (!(materialized instanceof ValueVectorReadExpression)) {
      throw new IllegalStateException("Only direct references allowed.");
    }
    return ((ValueVectorReadExpression) materialized).getFieldId().getFieldIds();
  }

  // Get the field vector of a field
  private FieldVector getField(VectorAccessible accessible, LogicalExpression expr) {
    return accessible
        .getValueAccessorById(FieldVector.class, getFieldIds(accessible, expr))
        .getValueVector();
  }

  // Get the id of a field
  private int getFieldId(VectorAccessible accessible, LogicalExpression expr) {
    return getFieldIds(accessible, expr)[0];
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds
    // checking later.
    for (FieldVector v : buildVectorsToValidate) {
      VariableLengthValidator.validateVariable(v, records);
    }

    int recordsDone = 0;
    while (recordsDone < records) {
      pivotBuildWatch.start();
      int pivoted =
          BoundedPivots.pivot(
              joinSetupParams.getBuildKeyPivot(),
              recordsDone,
              records - recordsDone,
              pivotFixedBlock,
              pivotVarBlock);
      pivotBuildWatch.stop();

      Preconditions.checkState(pivoted > 0);
      int ret = partition.buildPivoted(recordsDone, pivoted);
      Preconditions.checkState(ret == pivoted);
      recordsDone += pivoted;
    }
    updateStats();
    computeExternalState();
  }

  private boolean runtimeFiltersDropped() {
    return partitionColFilters == null && nonPartitionColFilters == null;
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (runtimeFilterEnabled
        && !runtimeFiltersDropped()
        && (!config.getRuntimeFilterInfo().isBroadcastJoin() || !partition.isBuildSideEmpty())) {
      tryPushRuntimeFilters();
    }

    if (partition.isBuildSideEmpty()
        && joinSetupParams.getJoinType() != JoinRelType.LEFT
        && joinSetupParams.getJoinType() != JoinRelType.FULL) {
      // nothing needs to be read on the left side as right side is empty
      computeExternalState(InternalState.DONE);
      return;
    }

    computeExternalState(InternalState.PROBE_IN);
    if (joinSetupParams
        .getOptions()
        .getOption(HashJoinOperator.TEST_SPILL_MODE)
        .equals("buildAndReplay")) {
      switchToSpilling(true);
      computeExternalState();
    }
  }

  private void closeRuntimeFilters() {
    try {
      AutoCloseables.close(partitionColFilters);
      partitionColFilters = null;
      AutoCloseables.closeNoChecked(nonPartitionColFilters);
      nonPartitionColFilters = null;
    } catch (Exception e) {
      logger.warn("Failed to close PartitionColFilters or NonPartitionColFilters", e);
    }
  }

  private void tryPushRuntimeFilters() {
    try {
      filterBuildTime.start();
      // if the partition is spilled, filters were already prepared
      if (!partition.isFiltersPreparedWithException()) {
        /* Step 4: Prepare BloomFilters, i.e preparing partitionColFilters, for last time */
        logger.debug("Preparing partitionColFilters, for last time...");
        partition.prepareBloomFilters(partitionColFilters);

        if (nonPartitionColFilters != null) {
          /* Step 5. Prepare NonPartitionColFilter, for last time */
          logger.debug("Preparing nonPartitionColFilters, for last time...");
          partition.prepareValueListFilters(nonPartitionColFilters);

          /* Step 6. Finalizing NonPartitionColFiltes */
          logger.debug("Building nonPartitionColFilters...");
          nonPartitionColFilters.finalizeValueListFilters();
        }
        logger.debug("Preparing and sending runtimeFilters...");
        RuntimeFilterUtil.prepareAndSendRuntimeFilters(
            filterManager,
            config.getRuntimeFilterInfo(),
            partitionColFilters,
            nonPartitionColFilters,
            context,
            config);
      } else {
        logger.warn(
            "Some error occurred while preparing runtime filters. Hence, not sending them.");
        filterManager.incrementDropCount();
      }
    } catch (Exception e) {
      // This is just an optimisation. Hence, we don't throw the error further.
      logger.warn("Error while processing runtime join filter", e);
    } finally {
      /*
       * DX-54601: partitionColFilters and nonPartitionColFilters will be freed during the close of the op.
       * Closing here can be too soon as the underlying buffers might get extra reference and are waiting to be sent in OOB.
       */
      try {
        filterBuildTime.stop();
        logger.debug(
            "Time taken for preparation of join runtime filter for query {} is {}ms",
            QueryIdHelper.getQueryIdentifier(context.getFragmentHandle()),
            filterBuildTime.elapsed(TimeUnit.MILLISECONDS));
      } catch (RuntimeException e) {
        logger.warn("Error while recording the time for runtime filter preparation", e);
      }
    }
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    /*
     * There are two types of OOB messages that can arrive.
     * 1. RuntimeFilter
     * 2. OOM Spill
     *
     * For RuntimeFilter, the filter itself is directly pushed as payload,
     * with a single additional buffer. The payload type is RuntimeFilter.
     *
     * For OOM Spill, the payload is of type HashJoinSpill, which has the current
     * memory usage. It has no buffers associated.
     *
     * Since the payload type is different, the getPayload() interface cannot be used
     * directly as it checks against the type.
     *
     * For now, the getBuffers() is used to differentiate the message/payload types.
     * We are not anticipating any new OOB types in the near future. If it ever happen,
     * need to revisit the logic however it is foolproof till then.
     */
    if (message.getBuffers() == null) {
      ExecProtos.HashJoinSpill joinSpill = message.getPayload(ExecProtos.HashJoinSpill.parser());
      Preconditions.checkState(joinSpill.getMessageType().equals(OOM_SPILL));

      if (message.getSendingMinorFragmentId() == context.getFragmentHandle().getMinorFragmentId()) {
        oobDropLocal++;
        logger.debug("Ignoring the OOB spill trigger self notification");
        return;
      }

      if (internalState != InternalState.BUILD) {
        oobDropWrongState++;
        logger.debug("Ignoring OOB spill trigger as fragment is outputting data.");
        return;
      }

      // check to see if we're at the point where we want to spill.
      final ExecProtos.HashJoinSpill spill = message.getPayload(ExecProtos.HashJoinSpill.parser());
      final long allocatedMemoryBeforeSpilling = context.getAllocator().getAllocatedMemory();
      final double triggerFactor = context.getOptions().getOption(OOB_SPILL_TRIGGER_FACTOR);
      final double headroomRemaining =
          context.getAllocator().getHeadroom()
              * 1.0d
              / (context.getAllocator().getHeadroom()
                  + context.getAllocator().getAllocatedMemory());
      if (allocatedMemoryBeforeSpilling < (spill.getMemoryUse() * triggerFactor)
          && headroomRemaining
              > context.getOptions().getOption(OOB_SPILL_TRIGGER_HEADROOM_FACTOR)) {
        logger.debug(
            "Skipping OOB spill trigger, current allocation is {}, which is not within the current factor of "
                + "the spilling operator ({}) which has memory use of {}. Headroom is at {} which is greater than trigger headroom of {}",
            allocatedMemoryBeforeSpilling,
            triggerFactor,
            spill.getMemoryUse(),
            headroomRemaining,
            context.getOptions().getOption(OOB_SPILL_TRIGGER_HEADROOM_FACTOR));
        oobDropUnderThreshold++;
        return;
      }

      final CanSwitchToSpilling.SwitchResult switchResult =
          ((CanSwitchToSpilling) partition).switchToSpilling(false);
      if (!switchResult.isSwitchDone()) {
        ++oobDropNoVictim;
        logger.debug("Ignoring OOB spill trigger as no victim partitions found.");
        return;
      }
      ++oobSpills;
    } else if (runtimeFilterEnabled && !runtimeFiltersDropped()) {
      RuntimeFilterUtil.workOnOOB(message, filterManager, context, config);
    }
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds
    // checking later.
    for (FieldVector v : probeVectorsToValidate) {
      VariableLengthValidator.validateVariable(v, records);
    }
    probePivotCursor.init(records);

    computeExternalState(InternalState.PROBE_PIVOT_AND_OUT);
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

    // realloc buffers so that we do not overwrite any previous buffers that may be in-transit.
    outgoing.setInitialCapacity(targetOutputBatchSize);
    outgoing.allocateNew();

    if (internalState == InternalState.REPLAY) {
      // if all incoming data has been processed, do spill replay.
      int ret = joinReplayer.run();
      Preconditions.checkState(ret >= 0);

      outgoing.setAllCount(ret);
      outputRecords += ret;
      computeExternalState(joinReplayer.isFinished() ? InternalState.DONE : InternalState.REPLAY);
      return ret;
    } else if (internalState == InternalState.PROBE_OUT
        || internalState == InternalState.PROBE_PIVOT_AND_OUT) {
      if (internalState == InternalState.PROBE_PIVOT_AND_OUT) {
        // pivot the next set of records in the incoming batch
        Preconditions.checkState(!probePivotCursor.isFinished());
        pivotProbeWatch.start();
        int startIdx = probePivotCursor.startPivotIdx + probePivotCursor.numPivoted;
        int records = probePivotCursor.batchSize;
        int pivoted =
            BoundedPivots.pivot(
                joinSetupParams.getProbeKeyPivot(),
                startIdx,
                records - startIdx,
                pivotFixedBlock,
                pivotVarBlock);
        pivotProbeWatch.stop();

        probePivotCursor.update(startIdx, pivoted);
        partition.probeBatchBegin(startIdx, pivoted);
      }

      final int probedRecords = partition.probePivoted(0, targetOutputBatchSize - 1);
      outputRecords += Math.abs(probedRecords);
      if (probedRecords > -1) {
        computeExternalState(
            probePivotCursor.isFinished()
                ? InternalState.PROBE_IN
                : InternalState.PROBE_PIVOT_AND_OUT);
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

    if (joinSetupParams.getJoinType() == JoinRelType.FULL
        || joinSetupParams.getJoinType() == JoinRelType.RIGHT) {
      // if we need to project build records that didn't match, make sure we do so.
      computeExternalState(InternalState.PROJECT_NON_MATCHES);
    } else {
      checkAndSwitchToReplayMode();
    }
  }

  @Override
  public int getOperatorId() {
    return config.getProps().getLocalOperatorId();
  }

  private boolean isShrinkable() {
    return (state == State.CAN_CONSUME_R
        || state == State.CAN_CONSUME_L
        || (state == State.CAN_PRODUCE && !joinSetupParams.getMultiMemoryReleaser().isFinished()));
  }

  @Override
  public long shrinkableMemory() {
    long shrinkableMemory = 0;
    if (isShrinkable()) {
      shrinkableMemory = context.getAllocator().getAllocatedMemory() - reservedPreallocation;
    }
    return shrinkableMemory;
  }

  @Override
  public boolean shrinkMemory(long size) throws Exception {
    if (!isShrinkable()) {
      // Shrinkable memory would have returned as 0 in all non-shrinkable states, hence we can
      // safely ignore them.
      return true;
    }
    if (state == State.CAN_CONSUME_R || state == State.CAN_CONSUME_L) {
      switchToSpilling(false);
    } else {
      joinSetupParams.getMultiMemoryReleaser().run();
    }
    computeExternalState();
    return joinSetupParams.getMultiMemoryReleaser().isFinished();
  }

  /**
   * Printing operator state for debug logs
   *
   * @return
   */
  @Override
  public String getOperatorStateToPrint() {
    return state.name() + " " + internalState.name();
  }

  private void checkAndSwitchToReplayMode() {
    Preconditions.checkState(internalState != InternalState.REPLAY);
    // all incoming batches have been processed, including projection of non-matches. check and
    // switch to
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
          Preconditions.checkState(
              newState == InternalState.PROBE_IN || newState == InternalState.DONE,
              "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROBE_IN:
          Preconditions.checkState(
              newState == InternalState.PROBE_PIVOT_AND_OUT
                  || newState == InternalState.PROJECT_NON_MATCHES
                  || newState == InternalState.REPLAY
                  || newState == InternalState.DONE,
              "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROBE_OUT:
        case PROBE_PIVOT_AND_OUT:
          Preconditions.checkState(
              newState == InternalState.PROBE_IN
                  || newState == InternalState.PROBE_OUT
                  || newState == InternalState.PROBE_PIVOT_AND_OUT
                  || newState == InternalState.PROJECT_NON_MATCHES
                  || newState == InternalState.DONE,
              "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case PROJECT_NON_MATCHES:
          Preconditions.checkState(
              newState == InternalState.REPLAY || newState == InternalState.DONE,
              "unexpected transition from state " + internalState + " to new state " + newState);
          break;

        case REPLAY:
          Preconditions.checkState(
              newState == InternalState.DONE,
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

      case PROBE_PIVOT_AND_OUT:
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

  private void updateStats() {
    final OperatorStats stats = context.getStats();
    final TimeUnit ns = TimeUnit.NANOSECONDS;

    Partition.Stats partitionStats = null;
    if (partition != null) {
      partitionStats = partition.getStats();
    }
    if (partitionStats != null) {
      stats.setLongStat(Metric.NUM_ENTRIES, partitionStats.getBuildNumEntries());
      stats.setLongStat(Metric.NUM_BUCKETS, partitionStats.getBuildNumBuckets());
      stats.setLongStat(Metric.NUM_RESIZING, partitionStats.getBuildNumResizing());
      stats.setLongStat(Metric.RESIZING_TIME_NANOS, partitionStats.getBuildResizingTimeNanos());
      stats.setLongStat(Metric.PIVOT_TIME_NANOS, pivotBuildWatch.elapsed(ns));
      stats.setLongStat(Metric.INSERT_TIME_NANOS, partitionStats.getBuildInsertTimeNanos());
      stats.setLongStat(
          Metric.HASHCOMPUTATION_TIME_NANOS, partitionStats.getBuildHashComputationTimeNanos());
      stats.setLongStat(Metric.RUNTIME_FILTER_DROP_COUNT, filterManager.getFilterDropCount());
      stats.setLongStat(
          Metric.RUNTIME_COL_FILTER_DROP_COUNT, filterManager.getSubFilterDropCount());
      stats.setLongStat(Metric.LINK_TIME_NANOS, partitionStats.getBuildLinkTimeNanos());
      stats.setLongStat(
          Metric.BUILD_CARRYOVER_COPY_NANOS, partitionStats.getBuildCarryOverCopyNanos());
      stats.setLongStat(Metric.BUILD_COPY_NANOS, partitionStats.getBuildKeyCopyNanos());
      stats.setLongStat(
          Metric.BUILD_COPY_NOMATCH_NANOS, partitionStats.getBuildCopyNonMatchNanos());
      stats.setLongStat(
          Metric.UNMATCHED_BUILD_KEY_COUNT, partitionStats.getBuildUnmatchedKeyCount());

      stats.setLongStat(Metric.PROBE_FIND_NANOS, partitionStats.getProbeFindTimeNanos());
      stats.setLongStat(Metric.PROBE_PIVOT_NANOS, pivotProbeWatch.elapsed(ns));
      stats.setLongStat(Metric.PROBE_LIST_NANOS, partitionStats.getProbeListTimeNanos());
      stats.setLongStat(
          Metric.PROBE_HASHCOMPUTATION_TIME_NANOS, partitionStats.getProbeHashComputationTime());
      stats.setLongStat(Metric.PROBE_COPY_NANOS, partitionStats.getProbeCopyNanos());
      stats.setLongStat(Metric.UNMATCHED_PROBE_COUNT, partitionStats.getProbeUnmatchedKeyCount());
      stats.setLongStat(Metric.OUTPUT_RECORDS, outputRecords);

      stats.setLongStat(
          Metric.EXTRA_CONDITION_EVALUATION_COUNT, partitionStats.getEvaluationCount());
      stats.setLongStat(
          Metric.EXTRA_CONDITION_EVALUATION_MATCHED, partitionStats.getEvaluationMatchedCount());
      stats.setLongStat(Metric.EXTRA_CONDITION_SETUP_NANOS, partitionStats.getSetupNanos());
    }

    SpillStats spillStats = joinSetupParams.getSpillStats();
    // time taken related to spill
    stats.setLongStat(Metric.SPILL_WR_NANOS, spillStats.getWriteNanos());
    stats.setLongStat(Metric.SPILL_RD_NANOS, spillStats.getReadNanos());

    if (spillStats.getSpillCount() != 0) {
      stats.setLongStat(Metric.SPILL_COUNT, spillStats.getSpillCount());
      stats.setLongStat(Metric.HEAP_SPILL_COUNT, spillStats.getHeapSpillCount());
      stats.setLongStat(Metric.SPILL_REPLAY_COUNT, spillStats.getReplayCount());
      stats.setLongStat(Metric.SPILL_WR_BUILD_BYTES, spillStats.getWriteBuildBytes());
      stats.setLongStat(Metric.SPILL_RD_BUILD_BYTES, spillStats.getReadBuildBytes());
      stats.setLongStat(Metric.SPILL_WR_BUILD_RECORDS, spillStats.getWriteBuildRecords());
      stats.setLongStat(Metric.SPILL_RD_BUILD_RECORDS, spillStats.getReadBuildRecords());
      stats.setLongStat(Metric.SPILL_WR_BUILD_BATCHES, spillStats.getWriteBuildBatches());
      stats.setLongStat(Metric.SPILL_RD_BUILD_BATCHES, spillStats.getReadBuildBatches());
      stats.setLongStat(
          Metric.SPILL_RD_BUILD_BATCHES_MERGED, spillStats.getReadBuildBatchesMerged());
      stats.setLongStat(Metric.SPILL_WR_PROBE_BYTES, spillStats.getWriteProbeBytes());
      stats.setLongStat(Metric.SPILL_RD_PROBE_BYTES, spillStats.getReadProbeBytes());
      stats.setLongStat(Metric.SPILL_WR_PROBE_RECORDS, spillStats.getWriteProbeRecords());
      stats.setLongStat(Metric.SPILL_RD_PROBE_RECORDS, spillStats.getReadProbeRecords());
      stats.setLongStat(Metric.SPILL_WR_PROBE_BATCHES, spillStats.getWriteProbeBatches());
      stats.setLongStat(Metric.SPILL_RD_PROBE_BATCHES, spillStats.getReadProbeBatches());
      stats.setLongStat(
          Metric.SPILL_RD_PROBE_BATCHES_MERGED, spillStats.getReadProbeBatchesMerged());
      stats.setLongStat(Metric.OOB_SENDS, spillStats.getOOBSends());
      stats.setLongStat(Metric.OOB_DROP_UNDER_THRESHOLD, oobDropUnderThreshold);
      stats.setLongStat(Metric.OOB_DROP_NO_VICTIM, oobDropNoVictim);
      stats.setLongStat(Metric.OOB_DROP_LOCAL, oobDropLocal);
      stats.setLongStat(Metric.OOB_DROP_LOCAL, oobDropWrongState);
      stats.setLongStat(Metric.OOB_SPILL, oobSpills);
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @Override
  public void close() throws Exception {
    if (state != State.NEEDS_SETUP) {
      updateStats();
    }

    closeRuntimeFilters();
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(filterManager);
    autoCloseables.add(joinReplayer);
    autoCloseables.add(outgoing);
    autoCloseables.add(partition);
    autoCloseables.add(joinSetupParams);
    AutoCloseables.close(autoCloseables);
  }

  private static class ProbePivotCursor {
    private int batchSize;
    private int startPivotIdx;
    private int numPivoted;

    void init(int batchSize) {
      this.batchSize = batchSize;
      this.startPivotIdx = 0;
      this.numPivoted = 0;
    }

    void update(int startPivotIdx, int numPivoted) {
      this.startPivotIdx = startPivotIdx;
      this.numPivoted = numPivoted;
    }

    boolean isFinished() {
      return startPivotIdx + numPivoted == batchSize;
    }
  }
}
