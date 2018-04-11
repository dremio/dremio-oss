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
package com.dremio.sabot.op.join.vhash;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.calcite.rel.core.JoinRelType;

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
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.hashtable.HashTableStats.Metric;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.hash.BuildInfo;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class VectorizedHashJoinOperator implements DualInputOperator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashJoinOperator.class);

  private static enum Mode {
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

  // List of BuildInfo structures. Used to maintain auxiliary information about the build batches
  // There is one of these for each incoming batch of records
  private final List<BuildInfo> buildInfoList = new ArrayList<>();

  private final List<FieldVector> buildOutputs = new ArrayList<>();
  private final List<FieldVector> probeOutputs = new ArrayList<>();

  private final List<FieldVector> buildVectorsToValidate = new ArrayList<>();
  private final List<FieldVector> probeVectorsToValidate = new ArrayList<>();

  private final VectorContainer outgoing;
  private ExpandableHyperContainer hyperContainer;
  private Mode mode = Mode.UNKNOWN;

  private VectorizedProbe probe;
  private JoinTable table;
  private PivotDef probePivot;
  private PivotDef buildPivot;
  private NullComparator comparator;

  private VectorAccessible left;
  private VectorAccessible right;
  private int buildBatchIndex = 0;
  private State state = State.NEEDS_SETUP;
  private boolean finishedProbe = false;
  private long outputRecords = 0;
  private boolean debugInsertion = false;

  public VectorizedHashJoinOperator(OperatorContext context, HashJoinPOP popConfig) throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.joinType = popConfig.getJoinType();
    this.outgoing = new VectorContainer(context.getAllocator());
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
    final BitSet requiredBits = new BitSet();

    Mode mode = context.getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_HASHJOIN_SPECIFIC) ? Mode.VECTORIZED_BIGINT : Mode.VECTORIZED_GENERIC;
    int fieldIndex = 0;

    if(config.getConditions().size() != 1){
      mode = Mode.VECTORIZED_GENERIC;
    }
    for(JoinCondition c : config.getConditions()){
      final FieldVector build = getField(right, c.getRight());
      buildFields.add(new FieldVectorPair(build, build));
      final FieldVector probe = getField(left, c.getLeft());
      probeFields.add(new FieldVectorPair(probe, probe));

      final Comparator joinComparator = JoinUtils.checkAndReturnSupportedJoinComparator(c);
      switch(joinComparator){
      case EQUALS:
        requiredBits.set(fieldIndex);
        break;
      case IS_NOT_DISTINCT_FROM:
        mode = Mode.VECTORIZED_GENERIC;
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
      if(v instanceof NullableVarBinaryVector || v instanceof NullableVarCharVector){
        buildVectorsToValidate.add(v);
      }
    }

    for(VectorWrapper<?> w : left){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(v instanceof NullableVarBinaryVector || v instanceof NullableVarCharVector){
        probeVectorsToValidate.add(v);
      }
    }

    int i = 0;

    for(VectorWrapper<?> w : outgoing){
      final FieldVector v = (FieldVector) w.getValueVector();
      if(i < right.getSchema().getFieldCount()){
        buildOutputs.add(v);
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

    hyperContainer = new ExpandableHyperContainer(context.getAllocator(), right.getSchema());
    this.mode = mode;
    switch(mode){
    case VECTORIZED_BIGINT:
      this.table = new EightByteInnerLeftProbeOff(context.getAllocator(), (int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), probePivot, buildPivot);
      break;
    case VECTORIZED_GENERIC:
      this.table = new BlockJoinTable(buildPivot, probePivot, context.getAllocator(), comparator, (int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), INITIAL_VAR_FIELD_AVERAGE_SIZE);
      break;
    default:
      throw new UnsupportedOperationException();
    }

    debugInsertion = context.getOptions().getOption(ExecConstants.DEBUG_HASHJOIN_INSERTION);

    state = State.CAN_CONSUME_R;
    return outgoing;
  }

  private FieldVector getField(VectorAccessible accessible, LogicalExpression expr){
    final LogicalExpression materialized = context.getClassProducer().materialize(expr, accessible);
    if(!(materialized instanceof ValueVectorReadExpression)){
      throw new IllegalStateException("Only direct references allowed.");
    }
    return accessible.getValueAccessorById(FieldVector.class, ((ValueVectorReadExpression) materialized).getFieldId().getFieldIds()).getValueVector();

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

    BuildInfo info = new BuildInfo(newLinksBuffer(records), new BitSet(records), records);
    buildInfoList.add(info);

    // ensure we have enough start indices space.
    while(table.size() + records > startIndices.size() * HashTable.BATCH_SIZE){
      startIndices.add(newLinksBuffer(HashTable.BATCH_SIZE));
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
        hashTableOffset = PlatformDependent.getShort(startIndexMemStart + 4);

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
          final short firstLinkOffset = PlatformDependent.getShort(firstLinkMemStart + 4);

          final ArrowBuf nextLink = buildInfoList.get(buildBatch).getLinks();
          final long nextLinkMemStart = nextLink.memoryAddress() + incomingRecordIndex * HashTable.BUILD_RECORD_LINK_SIZE;

          PlatformDependent.putInt(nextLinkMemStart, firstLinkBatch);
          PlatformDependent.putShort(nextLinkMemStart + 4, firstLinkOffset);

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
      stats.setLongStat(Metric.UNMATCHED_BUILD_COUNT, probe.getUnmatchedBuildCount());
      stats.setLongStat(Metric.UNMATCHED_PROBE_COUNT, probe.getUnmatchedProbeCount());
      stats.setLongStat(Metric.OUTPUT_RECORDS, outputRecords);
    }
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (table.size() == 0 && !(joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      state = State.DONE;
      return;
    }

    this.probe = new VectorizedProbe(
        context.getAllocator(),
        hyperContainer,
        left,
        probeOutputs,
        buildOutputs,
        config.getJoinType(),
        buildInfoList,
        startIndices,
        table,
        probePivot,
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
    outgoing.allocateNew();

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

  @Override
  public void close() throws Exception {
    updateStats();
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(hyperContainer);
    autoCloseables.add(table);
    autoCloseables.add(probe);
    autoCloseables.add(outgoing);
    autoCloseables.addAll(buildInfoList);
    autoCloseables.addAll(startIndices);
    AutoCloseables.close(autoCloseables);
  }

}
