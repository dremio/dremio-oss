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
package com.dremio.sabot.op.join.vhash;

import static com.dremio.sabot.op.join.vhash.PartitionColFilters.BLOOMFILTER_MAX_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.hash.BuildInfo;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.rel.core.JoinRelType;

public class VectorizedHashJoinOperator implements DualInputOperator {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VectorizedHashJoinOperator.class);

  public enum Mode {
    UNKNOWN,
    VECTORIZED_GENERIC,
    VECTORIZED_BIGINT
  }

  public static final int BATCH_MASK = 0x0000FFFF;

  private static final int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

  // Constant to indicate index is empty.
  private static final int INDEX_EMPTY = -1;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;
  private final OperatorContext context;
  private final HashJoinPOP config;

  private final Stopwatch linkWatch = Stopwatch.createUnstarted();

  // A structure that parallels the
  private final List<ArrowBuf> startIndices = new ArrayList<>();
  // Array of bitvectors. Keeps track of keys on the build side that matched any key on the probe
  // side
  private final List<MatchBitSet> keyMatchBitVectors = new ArrayList<>();
  // Max index(ordinal) in hash table and start indices
  private int maxHashTableIndex = -1;

  // List of BuildInfo structures. Used to maintain auxiliary information about the build batches
  // There is one of these for each incoming batch of records
  private final List<BuildInfo> buildInfoList = new ArrayList<>();
  /* The keys of build batch will not be added to hyper container for VECTORIZED_GENERIC mode.
   * probeIncomingKeys and buildOutputKeys are used to maintain all the keys in probe side and build side,
   * And they will be used to build copier in VectorizedProbe, which will be used to copy the key vectors
   * from probe side to build side in output for matched records.
   * For VECTORIZED_BIGINT mode, we keep the key in hyper container, so we don't need to copy key vectors from
   * probe side to build side in output for matched records.
   */
  private final List<FieldVector> probeIncomingKeys = new ArrayList<>();
  private final List<FieldVector> buildOutputKeys = new ArrayList<>();

  private final List<FieldVector> buildOutputs = new ArrayList<>();
  private final List<FieldVector> probeOutputs = new ArrayList<>();

  private final List<FieldVector> buildVectorsToValidate = new ArrayList<>();
  private final List<FieldVector> probeVectorsToValidate = new ArrayList<>();

  private final RuntimeFilterManager filterManager;
  private final boolean runtimeFilterEnabled;

  private final VectorContainer outgoing;
  private final Map<String, String> build2ProbeKeyMap = new HashMap<>();
  private ExpandableHyperContainer hyperContainer;
  private Mode mode = Mode.UNKNOWN;

  private VectorizedProbe probe;
  private JoinTable table;
  // Used to pivot the keys in probe batch
  private PivotDef probePivot;
  // Used to pivot the keys in hash table for build batch
  private PivotDef buildPivot;
  /* Used to unpivot the keys during outputting the non matched records in build side
   * Note that buildUnpivot is for output, but buildPivot is for incomming build batch
   * Only for VECTORIZED_GENERIC
   */
  private PivotDef buildUnpivot;
  private NullComparator comparator;

  private VectorAccessible left;
  private VectorAccessible right;
  private int buildBatchIndex = 0;
  private State state = State.NEEDS_SETUP;
  private boolean finishedProbe = false;
  private boolean debugInsertion = false;
  private long outputRecords = 0;
  private long duplicateBuildRecordCount;
  private PartitionColFilters partitionColFilters = null;
  private NonPartitionColFilters nonPartitionColFilters = null;

  public VectorizedHashJoinOperator(OperatorContext context, HashJoinPOP popConfig)
      throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.joinType = popConfig.getJoinType();
    this.outgoing = new VectorContainer(context.getAllocator());
    final Set<Integer> allMinorFragments =
        context.getAssignments().stream()
            .flatMap(a -> a.getMinorFragmentIdList().stream())
            .collect(Collectors.toSet()); // all minor fragments across all assignments
    this.runtimeFilterEnabled =
        RuntimeFilterUtil.shouldFragBuildRuntimeFilters(
            config.getRuntimeFilterInfo(), context.getFragmentHandle().getMinorFragmentId());
    this.filterManager =
        new RuntimeFilterManager(
            context.getAllocator(),
            RuntimeFilterUtil.getRuntimeValFilterCap(context),
            allMinorFragments);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
    state.is(State.NEEDS_SETUP);

    this.left = left;
    this.right = right;
    outgoing.addSchema(right.getSchema());
    outgoing.addSchema(left.getSchema());
    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());

    final List<FieldVectorPair> buildFields = new ArrayList<>();
    final List<FieldVectorPair> probeFields = new ArrayList<>();
    /* The build side key fields in output, the pivoted keys will be unpivoted to the vectors of key fields for non matched records
     * It's only for VECTORIZED_GENERIC because we don't maintain keys in hyper container.
     * It's not for VECTORIZED_BIGINT because we keep key in hyper container for only one eight byte key case.
     * It's different from buildFields because buildFields used to pivot for incomming batch, but buildOutputFields
     * is used to unpivot for output
     */
    final List<FieldVectorPair> buildOutputFields = new ArrayList<>();
    final BitSet requiredBits = new BitSet();
    // Used to indicate which field is key and will not be added to hyper container
    // Only for VECTORIZED_GENERIC
    final BitSet isKeyBits = new BitSet(right.getSchema().getFieldCount());
    /* The probe side key fields, which will be used to copied to the build side key fields for output
     * The key fields are not maintained in hyper container, so the probe side key field vectors
     * will be copied to build side key field vectors for matched records.
     * Only for VECTORIZED_GENERIC
     */
    final List<FieldVector> probeKeyFieldVectorList = new ArrayList<>();
    for (int i = 0; i < right.getSchema().getFieldCount(); i++) {
      probeKeyFieldVectorList.add(null);
    }

    Mode mode =
        context.getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN_SPECIFIC)
            ? Mode.VECTORIZED_BIGINT
            : Mode.VECTORIZED_GENERIC;
    int fieldIndex = 0;

    if (config.getConditions().size() != 1) {
      mode = Mode.VECTORIZED_GENERIC;
    }

    boolean isEqualForNullKey = false;
    for (JoinCondition c : config.getConditions()) {
      final FieldVector build = getField(right, c.getRight());
      buildFields.add(new FieldVectorPair(build, build));
      final FieldVector probe = getField(left, c.getLeft());
      probeFields.add(new FieldVectorPair(probe, probe));
      if (mode == Mode.VECTORIZED_GENERIC) {
        build2ProbeKeyMap.put(build.getField().getName(), probe.getField().getName());
      }

      /* Collect the corresponding probe side field vectors for build side keys
       * Only for VECTORIZED_GENERIC, we should do it because we don't know the final mode
       */
      int fieldId = getFieldId(outgoing, c.getRight());
      probeKeyFieldVectorList.set(fieldId, probe);
      /* The field is key in build side and its vectors will not be added to hyper container if mode is VECTORIZED_GENERIC
       * Only for VECTORIZED_GENERIC, we should do it because we don't know the final mode
       */
      isKeyBits.set(fieldId);
      /* Collect the build side keys in output, which will be used to create PivotDef for unpivot in projectBuildNonMatches
       * Only for VECTORIZED_GENERIC, we should do it because we don't know the final mode
       */
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
          isEqualForNullKey = true;
          break;
        case NONE:
          throw new UnsupportedOperationException();
        default:
          break;
      }

      switch (CompleteType.fromField(build.getField()).toMinorType()) {
        case BIGINT:
        case DATE:
        case FLOAT8:
        case INTERVALDAY:
        case TIMESTAMP:
          break;
        default:
          mode = Mode.VECTORIZED_GENERIC;
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

    for (VectorWrapper<?> w : outgoing) {
      final FieldVector v = (FieldVector) w.getValueVector();
      if (i < right.getSchema().getFieldCount()) {
        if ((mode == Mode.VECTORIZED_GENERIC) && isKeyBits.get(i)) {
          /* The corresponding field is key, so the fields in build side and probe side will
           * be added to probeIncomingKeys and buildOutputKeys. They will be used to create
           * copier to copy the keys from probe side to build side for output.
           * The field in build side will not be added to buildOutputs because we will unpivot them to output.
           * It's only for VECTORIZED_GENERIC because we don't need to unpivot for VECTORIZED_BIGINT.
           */
          probeIncomingKeys.add(probeKeyFieldVectorList.get(i));
          buildOutputKeys.add(v);
        } else {
          buildOutputs.add(v);
        }
      } else {
        probeOutputs.add(v);
      }
      i++;
    }

    Preconditions.checkState(buildFields.size() != 0);
    Preconditions.checkState(probeFields.size() != 0);
    this.probePivot = PivotBuilder.getBlockDefinition(probeFields);
    this.buildPivot = PivotBuilder.getBlockDefinition(buildFields);

    this.comparator = new NullComparator(requiredBits, probePivot.getBitCount());

    Preconditions.checkArgument(
        probePivot.getBlockWidth() == buildPivot.getBlockWidth(),
        "Block width of build [%s] and probe pivots are not equal [%s].",
        buildPivot.getBlockWidth(),
        probePivot.getBlockWidth());
    Preconditions.checkArgument(
        probePivot.getVariableCount() == buildPivot.getVariableCount(),
        "Variable column count of build [%s] and probe pivots are not equal [%s].",
        buildPivot.getVariableCount(),
        probePivot.getVariableCount());
    Preconditions.checkArgument(
        probePivot.getBitCount() == buildPivot.getBitCount(),
        "Bit width of build [%s] and probe pivots are not equal [%s].",
        buildPivot.getBitCount(),
        probePivot.getBitCount());

    this.mode = mode;
    switch (mode) {
      case VECTORIZED_BIGINT:
        // For only one eight byte key, we keep key in hyper container, so we don't need to unpivot
        // the key
        this.buildUnpivot = null;
        // Create the hyper container that all the fields, including key, will be added
        hyperContainer = new ExpandableHyperContainer(context.getAllocator(), right.getSchema());
        // Create eight byte key hash table to improve the performance for only one eight byte key
        this.table =
            new EightByteInnerLeftProbeOff(
                context.getAllocator(),
                (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
                probePivot,
                buildPivot,
                isEqualForNullKey);
        break;
      case VECTORIZED_GENERIC:
        // Create the PivotDef for unpivot in projectBuildNonMatches
        this.buildUnpivot = PivotBuilder.getBlockDefinition(buildOutputFields);
        // Create the hyper container with isKeyBits that indicates which field is key and will not
        // be added to hyper container
        hyperContainer =
            new ExpandableHyperContainer(context.getAllocator(), right.getSchema(), isKeyBits);
        // Create generic hash table
        this.table =
            new BlockJoinTable(
                buildPivot,
                probePivot,
                context.getAllocator(),
                comparator,
                (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
                INITIAL_VAR_FIELD_AVERAGE_SIZE,
                context.getConfig(),
                context.getOptions(),
                runtimeFilterEnabled);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

    state = State.CAN_CONSUME_R;
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

    final List<ArrowBuf> startIndices = this.startIndices;
    final List<BuildInfo> buildInfoList = this.buildInfoList;

    BuildInfo info = new BuildInfo(newLinksBuffer(records), records);
    buildInfoList.add(info);

    try (ArrowBuf offsets = context.getAllocator().buffer(records * 4);
        AutoCloseable traceBuf =
            debugInsertion ? table.traceStart(records) : AutoCloseables.noop()) {
      table.insert(offsets, records);

      linkWatch.start();
      setLinks(offsets.memoryAddress(), buildBatchIndex, records);
      linkWatch.stop();
    }

    /* Completed hashing all records in this batch. Transfer the batch
     * to the hyper vector container. Will be used when we want to retrieve
     * records that have matching keys on the probe side.
     */
    hyperContainer.addBatch(VectorContainer.getTransferClone(right, context.getAllocator()));
    // completed processing a batch, increment batch index

    buildBatchIndex++;

    if (buildBatchIndex < 0) {
      throw UserException.unsupportedError()
          .message(
              "HashJoin doesn't support more than %d (Integer.MAX_VALUE) number of batches on build side",
              Integer.MAX_VALUE)
          .build(logger);
    }

    updateStats();
  }

  private void setLinks(long indexAddr, final int buildBatch, final int records) {
    for (int incomingRecordIndex = 0;
        incomingRecordIndex < records;
        incomingRecordIndex++, indexAddr += 4) {
      final int hashTableIndex = PlatformDependent.getInt(indexAddr);

      if (hashTableIndex == -1) {
        continue;
      }

      /* Use the global index returned by the hash table, to store
       * the current record index and batch index. This will be used
       * later when we probe and find a match.
       */

      /* set the current record batch index and the index
       * within the batch at the specified keyIndex. The keyIndex
       * denotes the global index where the key for this record is
       * stored in the hash table
       */
      int hashTableBatch = hashTableIndex >>> 16;
      int hashTableOffset = hashTableIndex & BATCH_MASK;

      if (hashTableIndex > maxHashTableIndex) {
        maxHashTableIndex = hashTableIndex;
        // ensure we have enough start indices space.
        while (hashTableBatch >= startIndices.size()) {
          startIndices.add(newLinksBuffer(HashTable.BATCH_SIZE));
          keyMatchBitVectors.add(new MatchBitSet(HashTable.BATCH_SIZE, context.getAllocator()));
        }
      }

      ArrowBuf startIndex = startIndices.get(hashTableBatch);
      final long startIndexMemStart =
          startIndex.memoryAddress() + hashTableOffset * HashTable.BUILD_RECORD_LINK_SIZE;

      // If head of the list is empty, insert current index at this position
      final int linkBatch = PlatformDependent.getInt(startIndexMemStart);
      if (linkBatch == INDEX_EMPTY) {
        PlatformDependent.putInt(startIndexMemStart, buildBatch);
        PlatformDependent.putShort(startIndexMemStart + 4, (short) incomingRecordIndex);
      } else {
        duplicateBuildRecordCount++;
        /* Head of this list is not empty, if the first link
         * is empty insert there
         */
        hashTableBatch = linkBatch;
        hashTableOffset = Short.toUnsignedInt(PlatformDependent.getShort(startIndexMemStart + 4));

        final ArrowBuf firstLink = buildInfoList.get(hashTableBatch).getLinks();
        final long firstLinkMemStart =
            firstLink.memoryAddress() + hashTableOffset * HashTable.BUILD_RECORD_LINK_SIZE;

        final int firstLinkBatch = PlatformDependent.getInt(firstLinkMemStart);

        if (firstLinkBatch == INDEX_EMPTY) {
          PlatformDependent.putInt(firstLinkMemStart, buildBatch);
          PlatformDependent.putShort(firstLinkMemStart + 4, (short) incomingRecordIndex);
        } else {
          /* Insert the current value as the first link and
           * make the current first link as its next
           */
          final int firstLinkOffset =
              Short.toUnsignedInt(PlatformDependent.getShort(firstLinkMemStart + 4));

          final ArrowBuf nextLink = buildInfoList.get(buildBatch).getLinks();
          final long nextLinkMemStart =
              nextLink.memoryAddress() + incomingRecordIndex * HashTable.BUILD_RECORD_LINK_SIZE;

          PlatformDependent.putInt(nextLinkMemStart, firstLinkBatch);
          PlatformDependent.putShort(nextLinkMemStart + 4, (short) firstLinkOffset);

          // As the existing (batch, offset) pair is moved out of firstLink into nextLink,
          // now put the new (batch, offset) in the firstLink
          PlatformDependent.putInt(firstLinkMemStart, buildBatch);
          PlatformDependent.putShort(firstLinkMemStart + 4, (short) incomingRecordIndex);
        }
      }
    }
  }

  @VisibleForTesting
  public void updateStats() {
    final TimeUnit ns = TimeUnit.NANOSECONDS;
    final OperatorStats stats = context.getStats();

    if (table != null) {
      stats.setLongStat(Metric.NUM_ENTRIES, table.size());
      stats.setLongStat(Metric.NUM_BUCKETS, table.capacity());
      stats.setLongStat(Metric.NUM_RESIZING, table.getRehashCount());
      stats.setLongStat(Metric.RESIZING_TIME_NANOS, table.getRehashTime(ns));
      stats.setLongStat(Metric.PIVOT_TIME_NANOS, table.getBuildPivotTime(ns));
      stats.setLongStat(
          Metric.INSERT_TIME_NANOS, table.getInsertTime(ns) - table.getRehashTime(ns));
      stats.setLongStat(Metric.HASHCOMPUTATION_TIME_NANOS, table.getBuildHashComputationTime(ns));
      stats.setLongStat(Metric.RUNTIME_FILTER_DROP_COUNT, filterManager.getFilterDropCount());
      stats.setLongStat(
          Metric.RUNTIME_COL_FILTER_DROP_COUNT, filterManager.getSubFilterDropCount());

      stats.setLongStat(Metric.PROBE_PIVOT_NANOS, table.getProbePivotTime(ns));
      stats.setLongStat(Metric.PROBE_FIND_NANOS, table.getProbeFindTime(ns));
      stats.setLongStat(
          Metric.PROBE_HASHCOMPUTATION_TIME_NANOS, table.getProbeHashComputationTime(ns));
    }

    stats.setLongStat(Metric.VECTORIZED, mode.ordinal());
    stats.setLongStat(Metric.LINK_TIME_NANOS, linkWatch.elapsed(ns));
    stats.setLongStat(Metric.DUPLICATE_BUILD_RECORD_COUNT, duplicateBuildRecordCount);

    if (probe != null) {
      stats.setLongStat(Metric.PROBE_LIST_NANOS, probe.getProbeListTime());
      stats.setLongStat(Metric.PROBE_COPY_NANOS, probe.getProbeCopyTime());
      stats.setLongStat(Metric.BUILD_COPY_NANOS, probe.getBuildCopyTime());
      stats.setLongStat(Metric.BUILD_COPY_NOMATCH_NANOS, probe.getBuildNonMatchCopyTime());

      stats.setLongStat(Metric.UNMATCHED_BUILD_KEY_COUNT, probe.getUnmatchedBuildKeyCount());
      stats.setLongStat(Metric.UNMATCHED_PROBE_COUNT, probe.getUnmatchedProbeCount());
      stats.setLongStat(Metric.OUTPUT_RECORDS, outputRecords);

      stats.setLongStat(Metric.EXTRA_CONDITION_EVALUATION_COUNT, probe.getEvaluationCount());
      stats.setLongStat(
          Metric.EXTRA_CONDITION_EVALUATION_MATCHED, probe.getEvaluationMatchedCount());
      stats.setLongStat(Metric.EXTRA_CONDITION_SETUP_NANOS, probe.getSetupNanos());
    }
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (runtimeFilterEnabled
        && (!config.getRuntimeFilterInfo().isBroadcastJoin() || table.size() > 0)) {
      // for shuffled hash join case, need push runtime filer even though build side
      // size is 0, because merge points are waiting for runtime filter pieces from all
      // siblings.
      tryPushRuntimeFilter();
    }

    if ((table.size() == 0) && !(joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      state = State.DONE;
      return;
    }

    this.probe = new VectorizedProbe();
    this.probe.setup(
        context,
        hyperContainer,
        left,
        probeOutputs,
        buildOutputs,
        probeIncomingKeys,
        buildOutputKeys,
        build2ProbeKeyMap,
        mode,
        config.getJoinType(),
        buildInfoList,
        startIndices,
        keyMatchBitVectors,
        maxHashTableIndex,
        table,
        buildUnpivot,
        config.getExtraCondition());
    state = State.CAN_CONSUME_L;
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds
    // checking later.
    for (FieldVector v : probeVectorsToValidate) {
      VariableLengthValidator.validateVariable(v, records);
    }

    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    updateStats();

    if (!finishedProbe) {
      final int probedRecords = probe.probeBatch(left.getRecordCount());
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
      final int unmatched = probe.projectBuildNonMatches();
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
    if (joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT) {
      // if we need to project build records that didn't match, make sure we do so.
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
    }
  }

  public ArrowBuf newLinksBuffer(int recordCount) {
    // Each link is 6 bytes.
    // First 4 bytes are used to identify the batch and remaining 2 bytes for record within the
    // batch.
    final ArrowBuf linkBuf =
        context.getAllocator().buffer(recordCount * HashTable.BUILD_RECORD_LINK_SIZE);

    // Initialize the buffer. Write -1 (int) in the first four bytes.
    long bufOffset = linkBuf.memoryAddress();
    final long maxBufOffset = bufOffset + recordCount * HashTable.BUILD_RECORD_LINK_SIZE;
    for (; bufOffset < maxBufOffset; bufOffset += HashTable.BUILD_RECORD_LINK_SIZE) {
      PlatformDependent.putInt(bufOffset, INDEX_EMPTY);
    }

    return linkBuf;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @VisibleForTesting
  public PartitionColFilters createPartitionColFilters() {
    final long bloomFilterSize =
        config.getRuntimeFilterInfo().isBroadcastJoin()
            ? Math.min(BloomFilter.getOptimalSize(table.size()), BLOOMFILTER_MAX_SIZE)
            : BLOOMFILTER_MAX_SIZE;
    final int maxKeySize = RuntimeFilterUtil.getRuntimeFilterKeyMaxSize(context);

    return new PartitionColFilters(
        context.getAllocator(),
        config.getRuntimeFilterInfo().getRuntimeFilterProbeTargets(),
        buildPivot,
        bloomFilterSize,
        maxKeySize);
  }

  @VisibleForTesting
  public NonPartitionColFilters createNonPartitionColFilters() {
    final int maxElements = RuntimeFilterUtil.getRuntimeValFilterCap(context);

    return new NonPartitionColFilters(
        context.getAllocator(),
        config.getRuntimeFilterInfo().getRuntimeFilterProbeTargets(),
        buildPivot,
        maxElements);
  }

  protected void tryPushRuntimeFilter() {
    final Stopwatch filterTime = Stopwatch.createStarted();

    /* Step 1: Create partitionColFilters, i.e BloomFilters, one for each probe target */
    logger.debug("Creating partitionColFilters...");
    partitionColFilters = createPartitionColFilters();

    if (RuntimeFilterUtil.isRuntimeFilterEnabledForNonPartitionedCols(context)) {
      /* Step 2: Create nonPartitionColFilters, i.e ValueListFilterBuilders, one list (for multi-key) for each probe target */
      logger.debug("Creating nonPartitionColFilters...");
      nonPartitionColFilters = createNonPartitionColFilters();
    }

    /* Step 3: Prepare BloomFilters, i.e partitionColFilters */
    logger.debug("Preparing partitionColFilters...");
    table.prepareBloomFilters(partitionColFilters, config.getRuntimeFilterInfo().isBroadcastJoin());

    if (nonPartitionColFilters != null) {
      /* Step 3. Prepare NonPartitionColFilter */
      logger.debug("Preparing nonPartitionColFilters...");
      table.prepareValueListFilters(nonPartitionColFilters);

      /* Step 4. Build NonPartitionColFilters */
      logger.debug("Finalizing nonPartitionColFilters...");
      nonPartitionColFilters.finalizeValueListFilters();
    }

    try {
      /* Step 5. Prepare and send runtimeFilters to scan operator */
      logger.debug("Preparing and sending runtimeFilters...");
      RuntimeFilterUtil.prepareAndSendRuntimeFilters(
          filterManager,
          config.getRuntimeFilterInfo(),
          partitionColFilters,
          nonPartitionColFilters,
          context,
          config);
    } catch (Exception e) {
      // This is just an optimisation. Hence, we don't throw the error further.
      logger.warn("Error while processing runtime join filter", e);
    } finally {
      /*
       * DX-54601: partitionColFilters and nonPartitionColFilters will be freed during the close of the op.
       * Closing here can be too soon as the underlying buffers might get extra reference and are waiting to be sent in OOB.
       */
      try {
        filterTime.stop();
        logger.debug(
            "Time taken for preparation of join runtime filter at major fragment {}, minor fragment {} is {}ms",
            context.getFragmentHandle().getMajorFragmentId(),
            context.getFragmentHandle().getMinorFragmentId(),
            filterTime.elapsed(TimeUnit.MILLISECONDS));
      } catch (RuntimeException e) {
        logger.debug("Error while recording the time for runtime filter preparation", e);
      }
    }
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    if (runtimeFilterEnabled) {
      RuntimeFilterUtil.workOnOOB(message, filterManager, context, config);
    }
  }

  @Override
  public void close() throws Exception {
    updateStats();
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(partitionColFilters);
    autoCloseables.add(nonPartitionColFilters);
    autoCloseables.add(filterManager);
    autoCloseables.add(hyperContainer);
    autoCloseables.add(table);
    autoCloseables.add(probe);
    autoCloseables.add(outgoing);
    autoCloseables.addAll(buildInfoList);
    autoCloseables.addAll(probeIncomingKeys);
    autoCloseables.addAll(buildOutputKeys);
    autoCloseables.addAll(startIndices);
    autoCloseables.addAll(keyMatchBitVectors);
    AutoCloseables.close(autoCloseables);
  }

  @VisibleForTesting
  void setTable(JoinTable table) {
    this.table = table;
  }

  @VisibleForTesting
  JoinTable getTable() {
    return table;
  }

  @VisibleForTesting
  public void setBuildPivot(PivotDef buildPivot) {
    /* Only used in API testing */
    Preconditions.checkState(this.buildPivot == null);
    this.buildPivot = buildPivot;
  }
}
