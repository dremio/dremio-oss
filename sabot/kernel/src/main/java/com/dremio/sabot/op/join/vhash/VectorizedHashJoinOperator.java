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

import static com.dremio.exec.ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET;
import static com.dremio.exec.ExecConstants.RUNTIME_FILTER_KEY_MAX_SIZE;
import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.collections.CollectionUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.MajorFragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.RuntimeFilterManager;
import com.dremio.exec.util.RuntimeFilterProbeTarget;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
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

public class VectorizedHashJoinOperator implements DualInputOperator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashJoinOperator.class);

  public static enum Mode {
    UNKNOWN,
    VECTORIZED_GENERIC,
    VECTORIZED_BIGINT
  }

  public static final int BATCH_MASK = 0x0000FFFF;

  private static final int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

  // Constant to indicate index is empty.
  private static final int INDEX_EMPTY = -1;

  // nodes to shift while obtaining batch index from SV4
  private static final int SHIFT_SIZE = 16;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;
  private final OperatorContext context;
  private final HashJoinPOP config;

  private final Stopwatch linkWatch = Stopwatch.createUnstarted();

  // A structure that parallels the
  private final List<ArrowBuf> startIndices = new ArrayList<>();
  // Array of bitvectors. Keeps track of keys on the build side that matched any key on the probe side
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

  private final VectorContainer outgoing;
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
  private int runtimeValFilterCap;

  public VectorizedHashJoinOperator(OperatorContext context, HashJoinPOP popConfig) throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.joinType = popConfig.getJoinType();
    this.outgoing = new VectorContainer(context.getAllocator());
    final Set<Integer> allMinorFragments = context.getAssignments().stream().flatMap(a -> a.getMinorFragmentIdList().stream())
              .collect(Collectors.toSet()); // all minor fragments across all assignments
    runtimeValFilterCap = (int) context.getOptions().getOption(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE);
    this.filterManager = new RuntimeFilterManager(context.getAllocator(), runtimeValFilterCap, allMinorFragments);
  }

  @Override
  public State getState() {
    return state;
  }

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
    for (int i = 0;i < right.getSchema().getFieldCount(); i++) {
      probeKeyFieldVectorList.add(null);
    }

    Mode mode = context.getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN_SPECIFIC) ? Mode.VECTORIZED_BIGINT : Mode.VECTORIZED_GENERIC;
    int fieldIndex = 0;

    if(config.getConditions().size() != 1){
      mode = Mode.VECTORIZED_GENERIC;
    }

    boolean isEqualForNullKey = false;
    for(JoinCondition c : config.getConditions()){
      final FieldVector build = getField(right, c.getRight());
      buildFields.add(new FieldVectorPair(build, build));
      final FieldVector probe = getField(left, c.getLeft());
      probeFields.add(new FieldVectorPair(probe, probe));

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
      final FieldVector buildOutput = outgoing.getValueAccessorById(FieldVector.class, fieldId).getValueVector();
      buildOutputFields.add(new FieldVectorPair(buildOutput, buildOutput));

      final Comparator joinComparator = JoinUtils.checkAndReturnSupportedJoinComparator(c);
      switch(joinComparator){
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

      switch(CompleteType.fromField(build.getField()).toMinorType()){
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

    for(VectorWrapper<?> w : outgoing){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(i < right.getSchema().getFieldCount()){
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

    this.probePivot = PivotBuilder.getBlockDefinition(probeFields);
    this.buildPivot = PivotBuilder.getBlockDefinition(buildFields);

    this.comparator = new NullComparator(requiredBits, probePivot.getBitCount());

    Preconditions.checkArgument(probePivot.getBlockWidth() == buildPivot.getBlockWidth(), "Block width of build [%s] and probe pivots are not equal [%s].", buildPivot.getBlockWidth(), probePivot.getBlockWidth());
    Preconditions.checkArgument(probePivot.getBitCount() == buildPivot.getBitCount(), "Bit width of build [%s] and probe pivots are not equal [%s].", buildPivot.getBitCount(), probePivot.getBitCount());

    this.mode = mode;
    switch(mode){
      case VECTORIZED_BIGINT:
        // For only one eight byte key, we keep key in hyper container, so we don't need to unpivot the key
        this.buildUnpivot = null;
        // Create the hyper container that all the fields, including key, will be added
        hyperContainer = new ExpandableHyperContainer(context.getAllocator(), right.getSchema());
        // Create eight byte key hash table to improve the performance for only one eight byte key
        this.table = new EightByteInnerLeftProbeOff(context.getAllocator(), (int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), probePivot, buildPivot, isEqualForNullKey);
        break;
      case VECTORIZED_GENERIC:
        // Create the PivotDef for unpivot in projectBuildNonMatches
        this.buildUnpivot = PivotBuilder.getBlockDefinition(buildOutputFields);
        // Create the hyper container with isKeyBits that indicates which field is key and will not be added to hyper container
        hyperContainer = new ExpandableHyperContainer(context.getAllocator(), right.getSchema(), isKeyBits);
        // Create generic hash table
        this.table = new BlockJoinTable(buildPivot, probePivot, context.getAllocator(), comparator, (int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), INITIAL_VAR_FIELD_AVERAGE_SIZE);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

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
    for(FieldVector v : buildVectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }

    final List<ArrowBuf> startIndices = this.startIndices;
    final List<BuildInfo> buildInfoList = this.buildInfoList;

    BuildInfo info = new BuildInfo(newLinksBuffer(records), records);
    buildInfoList.add(info);

    // ensure we have enough start indices space.
    while(table.size() + records > startIndices.size() * HashTable.BATCH_SIZE){
      startIndices.add(newLinksBuffer(HashTable.BATCH_SIZE));
      keyMatchBitVectors.add(new MatchBitSet(HashTable.BATCH_SIZE, context.getAllocator()));
    }

    try(ArrowBuf offsets = context.getAllocator().buffer(records * 4);
        AutoCloseable traceBuf = debugInsertion ? table.traceStart(records) : AutoCloseables.noop()) {
      long findAddr = offsets.memoryAddress();
      table.insert(findAddr, records);

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
          .message("HashJoin doesn't support more than %d (Integer.MAX_VALUE) number of batches on build side",
              Integer.MAX_VALUE)
          .build(logger);
    }

    updateStats();
  }

  private void setLinks(long indexAddr, final int buildBatch, final int records){
    for(int incomingRecordIndex = 0; incomingRecordIndex < records; incomingRecordIndex++, indexAddr+=4){

      final int hashTableIndex = PlatformDependent.getInt(indexAddr);

      if(hashTableIndex == -1){
        continue;
      }

      if (hashTableIndex > maxHashTableIndex) {
        maxHashTableIndex = hashTableIndex;
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
      int hashTableBatch  = hashTableIndex >>> 16;
      int hashTableOffset = hashTableIndex & BATCH_MASK;

      ArrowBuf startIndex;
      try {
        startIndex = startIndices.get(hashTableBatch);
      } catch (IndexOutOfBoundsException e){
        UserException.Builder b = UserException.functionError()
          .message("Index out of bounds in VectorizedHashJoin. Index = %d, size = %d", hashTableBatch, startIndices.size())
          .addContext("incomingRecordIndex=%d, hashTableIndex=%d", incomingRecordIndex, hashTableIndex);
        if (debugInsertion) {
          b.addContext(table.traceReport());
        }
        throw b.build(logger);
      }
      final long startIndexMemStart = startIndex.memoryAddress() + hashTableOffset * HashTable.BUILD_RECORD_LINK_SIZE;

      // If head of the list is empty, insert current index at this position
      final int linkBatch = PlatformDependent.getInt(startIndexMemStart);
      if (linkBatch == INDEX_EMPTY) {
        PlatformDependent.putInt(startIndexMemStart, buildBatch);
        PlatformDependent.putShort(startIndexMemStart + 4, (short) incomingRecordIndex);
      } else {
        /* Head of this list is not empty, if the first link
         * is empty insert there
         */
        hashTableBatch = linkBatch;
        hashTableOffset = Short.toUnsignedInt(PlatformDependent.getShort(startIndexMemStart + 4));

        final ArrowBuf firstLink = buildInfoList.get(hashTableBatch).getLinks();
        final long firstLinkMemStart = firstLink.memoryAddress() + hashTableOffset * HashTable.BUILD_RECORD_LINK_SIZE;

        final int firstLinkBatch = PlatformDependent.getInt(firstLinkMemStart);

        if (firstLinkBatch == INDEX_EMPTY) {
          PlatformDependent.putInt(firstLinkMemStart, buildBatch);
          PlatformDependent.putShort(firstLinkMemStart + 4, (short) incomingRecordIndex);
        } else {
          /* Insert the current value as the first link and
           * make the current first link as its next
           */
          final int firstLinkOffset = Short.toUnsignedInt(PlatformDependent.getShort(firstLinkMemStart + 4));

          final ArrowBuf nextLink = buildInfoList.get(buildBatch).getLinks();
          final long nextLinkMemStart = nextLink.memoryAddress() + incomingRecordIndex * HashTable.BUILD_RECORD_LINK_SIZE;

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

  private void updateStats(){
    final TimeUnit ns = TimeUnit.NANOSECONDS;
    final OperatorStats stats = context.getStats();

    if(table != null){
      stats.setLongStat(Metric.NUM_ENTRIES, table.size());
      stats.setLongStat(Metric.NUM_BUCKETS,  table.capacity());
      stats.setLongStat(Metric.NUM_RESIZING, table.getRehashCount());
      stats.setLongStat(Metric.RESIZING_TIME_NANOS, table.getRehashTime(ns));
      stats.setLongStat(Metric.PIVOT_TIME_NANOS, table.getBuildPivotTime(ns));
      stats.setLongStat(Metric.INSERT_TIME_NANOS, table.getInsertTime(ns) - table.getRehashTime(ns));
      stats.setLongStat(Metric.HASHCOMPUTATION_TIME_NANOS, table.getBuildHashComputationTime(ns));
      stats.setLongStat(Metric.RUNTIME_FILTER_DROP_COUNT, filterManager.getFilterDropCount());
      stats.setLongStat(Metric.RUNTIME_COL_FILTER_DROP_COUNT, filterManager.getSubFilterDropCount());
    }

    stats.setLongStat(Metric.VECTORIZED, mode.ordinal());
    stats.setLongStat(Metric.LINK_TIME_NANOS, linkWatch.elapsed(ns));

    if(probe != null){
      stats.setLongStat(Metric.PROBE_PIVOT_NANOS, table.getProbePivotTime(ns));
      stats.setLongStat(Metric.PROBE_FIND_NANOS, table.getProbeFindTime(ns));

      stats.setLongStat(Metric.PROBE_LIST_NANOS, probe.getProbeListTime());
      stats.setLongStat(Metric.PROBE_COPY_NANOS, probe.getProbeCopyTime());
      stats.setLongStat(Metric.BUILD_COPY_NANOS, probe.getBuildCopyTime());
      stats.setLongStat(Metric.BUILD_COPY_NOMATCH_NANOS, probe.getBuildNonMatchCopyTime());

      stats.setLongStat(Metric.UNMATCHED_BUILD_KEY_COUNT, probe.getUnmatchedBuildKeyCount());
      stats.setLongStat(Metric.UNMATCHED_PROBE_COUNT, probe.getUnmatchedProbeCount());
      stats.setLongStat(Metric.OUTPUT_RECORDS, outputRecords);
      stats.setLongStat(Metric.PROBE_HASHCOMPUTATION_TIME_NANOS, table.getProbeHashComputationTime(ns));
    }
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if ((table.size() == 0) && !(joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      state = State.DONE;
      return;
    }

    tryPushRuntimeFilter();
    this.probe = new VectorizedProbe();
    this.probe.setup(
        context.getAllocator(),
        hyperContainer,
        left,
        probeOutputs,
        buildOutputs,
        probeIncomingKeys,
        buildOutputKeys,
        mode,
        config.getJoinType(),
        buildInfoList,
        startIndices,
        keyMatchBitVectors,
        maxHashTableIndex,
        table,
        probePivot,
        buildUnpivot,
        context.getTargetBatchSize(),
        comparator);
    state = State.CAN_CONSUME_L;
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for(FieldVector v : probeVectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }

    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    updateStats();

    if(!finishedProbe){
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
    if(joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT){
      // if we need to project build records that didn't match, make sure we do so.
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
    }
  }


  public ArrowBuf newLinksBuffer(int recordCount) {
    // Each link is 6 bytes.
    // First 4 bytes are used to identify the batch and remaining 2 bytes for record within the batch.
    final ArrowBuf linkBuf = context.getAllocator().buffer(recordCount * HashTable.BUILD_RECORD_LINK_SIZE);

    // Initialize the buffer. Write -1 (int) in the first four bytes.
    long bufOffset = linkBuf.memoryAddress();
    final long maxBufOffset = bufOffset + recordCount * HashTable.BUILD_RECORD_LINK_SIZE;
    for(; bufOffset < maxBufOffset; bufOffset += HashTable.BUILD_RECORD_LINK_SIZE) {
      PlatformDependent.putInt(bufOffset, INDEX_EMPTY);
    }

    return linkBuf;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @VisibleForTesting
  void tryPushRuntimeFilter() {
    /*
     * Prepare runtime filters, one each for probe scan targets.
     * a. For partitioned columns, a consolidated bloomfilter made on composite build keys.
     * b. For non-partitioned columns, an individual value filter is used per column. // TODO
     *
     * Only minor fragments [0,1,2] are allowed to send the filter to the scan operator.
     *
     * If it is a shuffle join, each minor fragment will create a filter from the table keys it has and send
     * it to minor fragments [0,1,2]. At merge points, filter will be consolidated before sending to the probe scan.
     *
     * For broadcast hashjoin cases, no exchanges required. Only minor fragments [0,1,2] will create and send the filter.
     */

    RuntimeFilterInfo runtimeFilterInfo = config.getRuntimeFilterInfo();
    int thisMinorFragment = context.getFragmentHandle().getMinorFragmentId();

    // Runtime filter processing will happen only by minor fragments <=2 for broadcast hash joins
    final boolean isSendingFragment = thisMinorFragment <= 2;

    if (runtimeFilterInfo==null || (runtimeFilterInfo.isBroadcastJoin() && !isSendingFragment)) {
      return;
    }

    final Stopwatch filterTime = Stopwatch.createStarted();
    for (RuntimeFilterProbeTarget probeTarget : RuntimeFilterProbeTarget.getProbeTargets(config.getRuntimeFilterInfo())) {
      try (RollbackCloseable closeOnErr = new RollbackCloseable()) {
        logger.debug("Processing filter for target {}", probeTarget);
        final RuntimeFilter.Builder runtimeFilterBuilder = RuntimeFilter.newBuilder()
                .setProbeScanOperatorId(probeTarget.getProbeScanOperatorId())
                .setProbeScanMajorFragmentId(probeTarget.getProbeScanMajorFragmentId());

        final Optional<BloomFilter> partitionColFilter = addPartitionColFilters(probeTarget, runtimeFilterBuilder, closeOnErr);
        final List<ValueListFilter> nonPartitionColFilters = addNonPartitionColFilters(probeTarget, runtimeFilterBuilder, closeOnErr);

        // Drop if all sub-filters are dropped
        final RuntimeFilter runtimeFilter = runtimeFilterBuilder.build();
        if (runtimeFilter.getPartitionColumnFilter().getColumnsCount() == 0
                && runtimeFilter.getNonPartitionColumnFilterCount() == 0) {
          logger.warn("Filter dropped for {}", probeTarget.toTargetIdString());
          this.filterManager.incrementDropCount();
          continue;
        }

        RuntimeFilterManager.RuntimeFilterManagerEntry fmEntry = null;
        if (!runtimeFilterInfo.isBroadcastJoin() && isSendingFragment) {
          // This fragment is one of the merge points. Set up FilterManager for interim use.
          fmEntry = filterManager.coalesce(runtimeFilter, partitionColFilter, nonPartitionColFilters, thisMinorFragment);
        }

        if (runtimeFilterInfo.isBroadcastJoin()) {
          sendRuntimeFilterToProbeScan(runtimeFilter, partitionColFilter, nonPartitionColFilters);
        } else if (fmEntry!=null && fmEntry.isComplete() && !fmEntry.isDropped()) {
          // All other filter pieces have already arrived. This one was last one to join.
          // Send merged filter to probe scan and close this individual piece explicitly.
          sendRuntimeFilterToProbeScan(runtimeFilter, Optional.ofNullable(fmEntry.getPartitionColFilter()),
                  fmEntry.getNonPartitionColFilters());
          filterManager.remove(fmEntry);
          AutoCloseables.close(closeOnErr.getCloseables());
        } else {
          // Send filter to merge points (minor fragments <=2) if not complete.
          sendRuntimeFilterAtMergePoints(runtimeFilter, partitionColFilter, nonPartitionColFilters);
        }
        closeOnErr.commit();
      } catch (Exception e) {
        // This is just an optimisation. Hence, we don't throw the error further.
        logger.warn("Error while processing runtime join filter", e);
      } finally {
        try {
          filterTime.stop();
          logger.debug("Time taken for preparation of join runtime filter at major fragment {}, minor fragment {} is {}ms",
                  context.getFragmentHandle().getMajorFragmentId(),
                  context.getFragmentHandle().getMinorFragmentId(),
                  filterTime.elapsed(TimeUnit.MILLISECONDS));
        } catch (RuntimeException e) {
          logger.debug("Error while recording the time for runtime filter preparation", e);
        }
      }
    }
  }

  private Optional<BloomFilter> addPartitionColFilters(RuntimeFilterProbeTarget probeTarget,
                                                       RuntimeFilter.Builder runtimeFilterBuilder,
                                                       RollbackCloseable closeOnErr) {
    if (CollectionUtils.isEmpty(probeTarget.getPartitionBuildTableKeys())) {
      return Optional.empty();
    }

    // Add partition column filter - always a single bloomfilter
    int maxKeySize = (int) context.getOptions().getOption(RUNTIME_FILTER_KEY_MAX_SIZE);
    final Optional<BloomFilter> bloomFilter = table.prepareBloomFilter(probeTarget.getPartitionBuildTableKeys(),
            config.getRuntimeFilterInfo().isBroadcastJoin(), maxKeySize);
    closeOnErr.add(bloomFilter.orElse(null));
    if (bloomFilter.isPresent() && !bloomFilter.get().isCrossingMaxFPP()) {
      final CompositeColumnFilter partitionFilter = CompositeColumnFilter.newBuilder()
              .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
              .addAllColumns(probeTarget.getPartitionProbeTableKeys())
              .setValueCount(bloomFilter.get().getNumBitsSet())
              .setSizeBytes(bloomFilter.get().getSizeInBytes()).build();
      runtimeFilterBuilder.setPartitionColumnFilter(partitionFilter);
    } else {
      // No valid bloom filter for partition pruning
      logger.debug("No valid partition column filter for {}", probeTarget.toTargetIdString());
      this.filterManager.incrementColFilterDropCount();
    }
    return bloomFilter;
  }

  private List<ValueListFilter> addNonPartitionColFilters(RuntimeFilterProbeTarget probeTarget,
                                                          RuntimeFilter.Builder runtimeFilterBuilder,
                                                          RollbackCloseable closeOnErr) {
    // Add non partition column filters
    if (!isRuntimeFilterEnabledForNonPartitionedCols() || CollectionUtils.isEmpty(probeTarget.getNonPartitionBuildTableKeys())) {
      return Collections.EMPTY_LIST;
    }

    final List<ValueListFilter> valueListFilters = new ArrayList<>(probeTarget.getNonPartitionBuildTableKeys().size());
    for (int colId = 0; colId < probeTarget.getNonPartitionBuildTableKeys().size(); colId++) {
      Optional<ValueListFilter> valueListFilter = table.prepareValueListFilter(
              probeTarget.getNonPartitionBuildTableKeys().get(colId), runtimeValFilterCap);
      if (valueListFilter.isPresent()) {
        closeOnErr.add(valueListFilter.get());
        final CompositeColumnFilter nonPartitionColFilter = CompositeColumnFilter.newBuilder()
                .addColumns(probeTarget.getNonPartitionProbeTableKeys().get(colId))
                .setFilterType(ExecProtos.RuntimeFilterType.VALUE_LIST)
                .setValueCount(valueListFilter.get().getValueCount())
                .setSizeBytes(valueListFilter.get().getSizeInBytes()).build();
        runtimeFilterBuilder.addNonPartitionColumnFilter(nonPartitionColFilter);
        valueListFilter.get().setFieldName(probeTarget.getNonPartitionProbeTableKeys().get(colId));
        valueListFilters.add(valueListFilter.get());
      } else {
        this.filterManager.incrementColFilterDropCount();
      }
    }
    return valueListFilters;
  }

  @VisibleForTesting
  boolean isRuntimeFilterEnabledForNonPartitionedCols() {
    return context.getOptions().getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET);
  }

  @VisibleForTesting
  void sendRuntimeFilterAtMergePoints(RuntimeFilter filter, Optional<BloomFilter> bloomFilter,
                                      List<ValueListFilter> nonPartitionColFilters) throws Exception {
    final List<ArrowBuf> orderedBuffers = new ArrayList<>(nonPartitionColFilters.size() + 1);
    try {
      final ArrowBuf bloomFilterBuf = bloomFilter.map(bf -> bf.getDataBuffer()).orElse(null);
      bloomFilter.ifPresent(bf -> orderedBuffers.add(bloomFilterBuf));
      nonPartitionColFilters.forEach(vlf -> orderedBuffers.add(vlf.buf()));

      // Sends the filters to node endpoints running minor fragments 0,1,2.
      for (FragmentAssignment a : context.getAssignments()) {
        try (RollbackCloseable closeOnErrSend = new RollbackCloseable()) {
          final List<Integer> targetMinorFragments = a.getMinorFragmentIdList().stream()
                  .filter(i -> i <= 2)
                  .filter(i -> i!=context.getFragmentHandle().getMinorFragmentId()) // skipping myself
                  .collect(Collectors.toList());
          if (targetMinorFragments.isEmpty()) {
            continue;
          }

          // Operator ID int is transformed as follows - (fragmentId << 16) + opId;
          logger.debug("Sending filter from {}:{} to {}", context.getFragmentHandle().getMinorFragmentId(),
                  config.getProps().getOperatorId(),
                  targetMinorFragments);
          final OutOfBandMessage message = new OutOfBandMessage(
                  context.getFragmentHandle().getQueryId(),
                  context.getFragmentHandle().getMajorFragmentId(),
                  targetMinorFragments,
                  config.getProps().getOperatorId(),
                  context.getFragmentHandle().getMajorFragmentId(),
                  context.getFragmentHandle().getMinorFragmentId(),
                  config.getProps().getOperatorId(),
                  new OutOfBandMessage.Payload(filter),
                  orderedBuffers.toArray(new ArrowBuf[orderedBuffers.size()]),
                  true);
          closeOnErrSend.add(bloomFilterBuf);
          final NodeEndpoint endpoint = context.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
          context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
          closeOnErrSend.commit();
        } catch (Exception e) {
          logger.warn("Error while sending runtime filter to minor fragments " + a.getMinorFragmentIdList(), e);
        }
      }
    } finally {
      AutoCloseables.close(orderedBuffers);
    }
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    if (message.getBuffers()==null || message.getBuffers().length!=1) {
      logger.warn("Empty runtime filter received from minor fragment: " + message.getSendingMinorFragmentId());
      return;
    }
    // check above ensures there is only one element in the array. This is a merged message buffer.
    final ArrowBuf msgBuf = message.getBuffers()[0];

    try {
      final RuntimeFilter runtimeFilter = message.getPayload(RuntimeFilter.parser());
      long nextSliceStart = 0L;
      ExecProtos.CompositeColumnFilter partitionColFilterProto = runtimeFilter.getPartitionColumnFilter();

      // Partition col filters
      BloomFilter bloomFilterPiece = null;
      if (partitionColFilterProto != null && !partitionColFilterProto.getColumnsList().isEmpty()) {
        checkArgument(msgBuf.capacity() >= partitionColFilterProto.getSizeBytes(), "Invalid filter size. " +
                "Buffer capacity is %s, expected filter size %s", msgBuf.capacity(), partitionColFilterProto.getSizeBytes());
        bloomFilterPiece = BloomFilter.prepareFrom(msgBuf.slice(nextSliceStart, partitionColFilterProto.getSizeBytes()));
        checkState(bloomFilterPiece.getNumBitsSet() == partitionColFilterProto.getValueCount(),
                "Bloomfilter value count mismatched. Expected %s, Actual %s", partitionColFilterProto.getValueCount(), bloomFilterPiece.getNumBitsSet());
        nextSliceStart += partitionColFilterProto.getSizeBytes();
        logger.debug("Received runtime filter piece {}, attempting merge.", bloomFilterPiece.getName());
      }

      final List<ValueListFilter> valueListFilterPieces = new ArrayList<>(runtimeFilter.getNonPartitionColumnFilterCount());
      for (int i =0; i < runtimeFilter.getNonPartitionColumnFilterCount(); i++) {
        ExecProtos.CompositeColumnFilter nonPartitionColFilterProto = runtimeFilter.getNonPartitionColumnFilter(i);
        final String fieldName = nonPartitionColFilterProto.getColumns(0);
        checkArgument(msgBuf.capacity() >= nextSliceStart + nonPartitionColFilterProto.getSizeBytes(),
                "Invalid filter buffer size for non partition col %s.", fieldName);
        final ValueListFilter valueListFilter = ValueListFilterBuilder
                .fromBuffer(msgBuf.slice(nextSliceStart, nonPartitionColFilterProto.getSizeBytes()));
        nextSliceStart += nonPartitionColFilterProto.getSizeBytes();
        valueListFilter.setFieldName(fieldName);
        checkState(valueListFilter.getValueCount() == nonPartitionColFilterProto.getValueCount(),
                "ValueListFilter %s count mismatched. Expected %s, found %s", fieldName,
                nonPartitionColFilterProto.getValueCount(), valueListFilter.getValueCount());
        valueListFilterPieces.add(valueListFilter);
      }

      final RuntimeFilterManager.RuntimeFilterManagerEntry filterManagerEntry;
      filterManagerEntry = filterManager.coalesce(runtimeFilter, Optional.ofNullable(bloomFilterPiece), valueListFilterPieces, message.getSendingMinorFragmentId());

      if (filterManagerEntry.isComplete() && !filterManagerEntry.isDropped()) {
        // composite filter is ready for further processing - no more pieces expected
        logger.debug("All pieces of runtime filter received. Sending to probe scan now. " + filterManagerEntry.getProbeScanCoordinates());
        Optional<RuntimeFilterProbeTarget> probeNode = RuntimeFilterProbeTarget.getProbeTargets(config.getRuntimeFilterInfo())
                .stream()
                .filter(pt -> pt.isSameProbeCoordinate(filterManagerEntry.getCompositeFilter().getProbeScanMajorFragmentId(), filterManagerEntry.getCompositeFilter().getProbeScanOperatorId()))
                .findFirst();
        if (probeNode.isPresent()) {
          sendRuntimeFilterToProbeScan(filterManagerEntry.getCompositeFilter(),
                  Optional.ofNullable(filterManagerEntry.getPartitionColFilter()),
                  filterManagerEntry.getNonPartitionColFilters());
        } else {
          logger.warn("Node coordinates not found for probe target:{}", filterManagerEntry.getProbeScanCoordinates());
        }
        filterManager.remove(filterManagerEntry);
      }
    } catch (Exception e) {
      filterManager.incrementDropCount();
      logger.warn("Error while merging runtime filter piece from " + message.getSendingMinorFragmentId(), e);
    }
  }

  @VisibleForTesting
  void sendRuntimeFilterToProbeScan(RuntimeFilter filter, Optional<BloomFilter> partitionColFilter,
                                    List<ValueListFilter> nonPartitionColFilters) throws Exception {
    logger.debug("Sending join runtime filter to probe scan {}:{}, Filter {}", filter.getProbeScanOperatorId(), filter.getProbeScanMajorFragmentId(), partitionColFilter);
    logger.debug("Partition col filter fpp {}", partitionColFilter.map(BloomFilter::getExpectedFPP).orElse(-1D));
    final List<ArrowBuf> orderedBuffers = new ArrayList<>(nonPartitionColFilters.size() + 1);
    try {
      final ArrowBuf bloomFilterBuf = partitionColFilter.map(bf -> bf.getDataBuffer()).orElse(null);
      partitionColFilter.ifPresent(bf -> orderedBuffers.add(bloomFilterBuf));
      nonPartitionColFilters.forEach(v -> orderedBuffers.add(v.buf()));

      final MajorFragmentAssignment majorFragmentAssignment = context.getExtMajorFragmentAssignments(filter.getProbeScanMajorFragmentId());
      if (majorFragmentAssignment==null) {
        logger.warn("Major fragment assignment for probe scan id {} is null. Dropping the runtime filter.", filter.getProbeScanOperatorId());
        return;
      }

      // Sends the filters to node endpoints running minor fragments 0,1,2.
      for (FragmentAssignment assignment : majorFragmentAssignment.getAllAssignmentList()) {
        try (RollbackCloseable closeOnErrSend = new RollbackCloseable()) {
          logger.info("Sending filter to OpId {}, Frag {}:{}", filter.getProbeScanOperatorId(),
                  filter.getProbeScanMajorFragmentId(), assignment.getMinorFragmentIdList());
          final OutOfBandMessage message = new OutOfBandMessage(
                  context.getFragmentHandle().getQueryId(),
                  filter.getProbeScanMajorFragmentId(),
                  assignment.getMinorFragmentIdList(),
                  filter.getProbeScanOperatorId(),
                  context.getFragmentHandle().getMajorFragmentId(),
                  context.getFragmentHandle().getMinorFragmentId(),
                  config.getProps().getOperatorId(),
                  new OutOfBandMessage.Payload(filter),
                  orderedBuffers.toArray(new ArrowBuf[orderedBuffers.size()]),
                  true);
          closeOnErrSend.add(bloomFilterBuf);
          final NodeEndpoint endpoint = context.getEndpointsIndex().getNodeEndpoint(assignment.getAssignmentIndex());
          context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
          closeOnErrSend.commit();
        } catch (Exception e) {
          logger.warn("Error while sending runtime filter to minor fragments " + assignment.getMinorFragmentIdList(), e);
        }
      }
    } finally {
      AutoCloseables.close(orderedBuffers);
    }
  }

  @VisibleForTesting
  void setTable(JoinTable table) {
    this.table = table;
  }

  @Override
  public void close() throws Exception {
    updateStats();
    List<AutoCloseable> autoCloseables = new ArrayList<>();
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
}
