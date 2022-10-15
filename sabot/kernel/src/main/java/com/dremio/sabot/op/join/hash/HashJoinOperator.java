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
package com.dremio.sabot.op.join.hash;

import static com.dremio.sabot.op.common.hashtable.HashTable.BUILD_RECORD_LINK_SIZE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.PowerOfTwoLongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.common.hashtable.ChainedHashTable;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.hashtable.HashTable.BatchAddedListener;
import com.dremio.sabot.op.common.hashtable.HashTableConfig;
import com.dremio.sabot.op.common.hashtable.HashTableStats;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.join.vhash.HashJoinStats;
import com.dremio.sabot.op.join.vhash.VectorizedHashJoinOperator;
import com.dremio.sabot.op.join.vhash.spill.VectorizedSpillingHashJoinOperator;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

import io.netty.util.internal.PlatformDependent;

@Options
public class HashJoinOperator implements DualInputOperator {

  public static final BooleanValidator ENABLE_SPILL = new BooleanValidator("exec.op.join.spill", false);
  public static final PowerOfTwoLongValidator NUM_PARTITIONS = new PowerOfTwoLongValidator("exec.op.join.spill.num_partitions", 16, 8);
  // For unit tests, always use with DEBUG flag only.
  public static final StringValidator TEST_SPILL_MODE = new StringValidator("exec.op.join.spill.test_spill_mode", "none");

  private long outputRecords;

  // Constant to indicate index is empty.
  private static final int INDEX_EMPTY = -1;

  // nodes to shift while obtaining batch index from SV4
  private static final int SHIFT_SIZE = 16;

  // Generator mapping for the build side : scalar
  private static final GeneratorMapping PROJECT_BUILD = GeneratorMapping.create("doSetup", "projectBuildRecord", null, null);
  // Generator mapping for the build side : constant
  private static final GeneratorMapping PROJECT_BUILD_CONSTANT = GeneratorMapping.create("doSetup", "doSetup", null, null);
  // Generator mapping for the probe side : scalar
  private static final GeneratorMapping PROJECT_PROBE = GeneratorMapping.create("doSetup", "projectProbeRecord", null, null);
  // Generator mapping for the probe side : constant
  private static final GeneratorMapping PROJECT_PROBE_CONSTANT = GeneratorMapping.create("doSetup", "doSetup", null, null);
  // Mapping set for the build side
  private final MappingSet projectBuildMapping = new MappingSet("buildIndex", "outIndex", "buildBatch", "outgoing", PROJECT_BUILD_CONSTANT, PROJECT_BUILD);
  // Mapping set for the probe side
  private final MappingSet projectProbeMapping = new MappingSet("probeIndex", "outIndex", "probeBatch", "outgoing", PROJECT_PROBE_CONSTANT, PROJECT_PROBE);

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;
  private final OperatorContext context;
  private final OperatorStats stats;
  private final List<JoinCondition> conditions;
  private final List<Comparator> comparators;
  private final HashTableStats hashTableStats;

  // A structure that parallels the
  private final List<ArrowBuf> startIndices = new ArrayList<>();
  // Array of bitvectors. Keeps track of keys on the build side that matched any key on the probe side
  private final List<BitSet> keyMatchBitVectors = new ArrayList<>();
  // Max index in hash table
  private int maxHashTableIndex = -1;

  // List of BuildInfo structures. Used to maintain auxiliary information about the build batches
  // There is one of these for each incoming batch of records
  private final List<BuildInfo> buildInfoList = new ArrayList<>();


  /* Hyper container to store all build side record batches.
   * Records are retrieved from this container when there is a matching record
   * on the probe side
   */
  private ExpandableHyperContainer hyperContainer;
  private final VectorContainer outgoing;

  // Runtime generated class implementing HashJoinProbe interface
  private HashJoinProbe hashJoinProbe;

  // Underlying hashtable used by the hash join
  private HashTable hashTable;
  private VectorAccessible left;
  private VectorAccessible right;
  private int buildBatchIndex = 0;
  private State state = State.NEEDS_SETUP;
  private boolean finishedProbe = false;

  public HashJoinOperator(OperatorContext context, HashJoinPOP popConfig) throws OutOfMemoryException {
    this.context = context;
    this.joinType = popConfig.getJoinType();
    this.conditions = popConfig.getConditions();
    this.stats = context.getStats();
    this.hashTableStats = new HashTableStats();

    List<Comparator> comparators = Lists.newArrayListWithExpectedSize(conditions.size());
    for (int i=0; i<conditions.size(); i++) {
      JoinCondition cond = conditions.get(i);
      comparators.add(JoinUtils.checkAndReturnSupportedJoinComparator(cond));
    }
    this.comparators = ImmutableList.copyOf(comparators);
    this.outgoing = context.createOutputVectorContainer();
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
    this.hyperContainer = new ExpandableHyperContainer(context.getAllocator(), right.getSchema());

    setupHashTable();
    state = State.CAN_CONSUME_R;
    return outgoing;
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);

    addNewBatch(records);

    // For every record in the build batch , hash the key columns
    for (int incomingRecordIndex = 0; incomingRecordIndex < records; incomingRecordIndex++) {
      final int hashTableIndex = hashTable.put(incomingRecordIndex);

      /* Use the global index returned by the hash table, to store
       * the current record index and batch index. This will be used
       * later when we probe and find a match.
       */
      setCurrentIndex(hashTableIndex, buildBatchIndex, incomingRecordIndex);
    }

    /* Completed hashing all records in this batch. Transfer the batch
     * to the hyper vector container. Will be used when we want to retrieve
     * records that have matching keys on the probe side.
     */
    hyperContainer.addBatch(VectorContainer.getTransferClone(right, context.getAllocator()));
    // completed processing a batch, increment batch index
    buildBatchIndex++;
    updateStats();
  }

  private void updateStats() {
    if (hashTable == null) {
      return;
    }
    final OperatorStats stats = this.stats;
    hashTable.getStats(hashTableStats);
    stats.setLongStat(HashJoinStats.Metric.NUM_BUCKETS, hashTableStats.numBuckets);
    stats.setLongStat(HashJoinStats.Metric.NUM_ENTRIES, hashTableStats.numEntries);
    stats.setLongStat(HashJoinStats.Metric.NUM_RESIZING, hashTableStats.numResizing);
    stats.setLongStat(HashJoinStats.Metric.RESIZING_TIME_NANOS, hashTableStats.resizingTime);
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (hashTable.size() == 0 && !(joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL)) {
      // nothing needs to be read on the left side as right side is empty
      state = State.DONE;
      return;
    }
    this.hashJoinProbe = setupHashJoinProbe();
    state = State.CAN_CONSUME_L;
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);
    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    outgoing.allocateNew();

    if(!finishedProbe){
      final int probedRecords = hashJoinProbe.probeBatch();
      if (probedRecords > -1) {
        state = State.CAN_CONSUME_L;
        outputRecords += probedRecords;
        return outgoing.setAllCount(probedRecords);
      } else {
        // we didn't finish everything, will produce again.
        state = State.CAN_PRODUCE;
        outputRecords -= probedRecords;
        return outgoing.setAllCount(-probedRecords);
      }
    } else {
      final int unmatched = hashJoinProbe.projectBuildNonMatches();
      if (unmatched > -1) {
        state = State.DONE;
        outputRecords += unmatched;
        return outgoing.setAllCount(unmatched);
      } else {
        // remainder, need to output again.
        outputRecords -= unmatched;
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

  private void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {
    // Setup the hash table configuration object
    int conditionsSize = conditions.size();
    final List<NamedExpression> rightExpr = new ArrayList<>(conditionsSize);
    List<NamedExpression> leftExpr = new ArrayList<>(conditionsSize);

    // Create named expressions from the conditions
    for (int i = 0; i < conditionsSize; i++) {
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(), new FieldReference("build_side_" + i)));
      leftExpr.add(new NamedExpression(conditions.get(i).getLeft(), new FieldReference("probe_side_" + i)));
    }

    if (left.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
      throw new SchemaChangeException("Hash join does not support probe batch with selection vectors");
    }


    final HashTableConfig htConfig =
        new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
            HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators);

    // Create the chained hash table
    final ChainedHashTable ht =
        new ChainedHashTable(htConfig, context.getClassProducer(), context.getAllocator(), this.right, this.left, outgoing, new Listener());
    hashTable = ht.createAndSetupHashTable(null, context.getOptions());
  }


  private class Listener implements BatchAddedListener {
    @Override
    public void batchAdded() {
      startIndices.add(newLinksBuffer(HashTable.BATCH_SIZE));
      keyMatchBitVectors.add(new BitSet(HashTable.BATCH_SIZE));
    }
  }

  public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {
    final CodeGenerator<HashJoinProbe> cg = context.getClassProducer().createGenerator(HashJoinProbe.TEMPLATE_DEFINITION);
    final ClassGenerator<HashJoinProbe> g = cg.getRoot();

    // Generate the code to project build side records
    g.setMappingSet(projectBuildMapping);

    int fieldId = 0;
    final JExpression buildIndex = JExpr.direct("buildIndex");
    final JExpression outIndex = JExpr.direct("outIndex");
    g.rotateBlock();
    for (final Field field : right.getSchema()) {
      final JVar inVV = g.declareVectorValueSetupAndMember("buildBatch", new TypedFieldId(CompleteType.fromField(field), true, fieldId));
      final JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(CompleteType.fromField(field), false, fieldId));
      g.getEvalBlock().add(outVV.invoke("copyFromSafe")
          .arg(JExpr.cast(g.getModel().INT, buildIndex.band(JExpr.lit((int) Character.MAX_VALUE))))
          .arg(outIndex)
          .arg(inVV.component(JExpr.cast(g.getModel().INT, buildIndex.shrz(JExpr.lit(16))))));

      fieldId++;
    }

    // Generate the code to project probe side records
    g.setMappingSet(projectProbeMapping);

    int outputFieldId = fieldId;
    fieldId = 0;
    final JExpression probeIndex = JExpr.direct("probeIndex");

    for (final Field field : left.getSchema()) {
      final JVar inVV = g.declareVectorValueSetupAndMember("probeBatch", new TypedFieldId(CompleteType.fromField(field), false, fieldId));
      final JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(CompleteType.fromField(field), false, outputFieldId));

      g.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(probeIndex).arg(outIndex).arg(inVV));

      fieldId++;
      outputFieldId++;
    }

    final HashJoinProbe hj = cg.getImplementationClass();

    if(buildInfoList.isEmpty()){
      // if we have no right hand rows, we need to add special handling by adding an empty right.
      hyperContainer.addBatch(right);
    }

    hj.setupHashJoinProbe(
        context.getClassProducer().getFunctionContext(),
        hyperContainer,
        left,
        outgoing,
        hashTable,
        joinType,
        buildInfoList,
        startIndices,
        keyMatchBitVectors,
        maxHashTableIndex,
        context.getTargetBatchSize()
        );
    return hj;
  }

  private void addNewBatch(int recordCount) throws SchemaChangeException {
    // Add a node to the list of BuildInfo's
    BuildInfo info = new BuildInfo(newLinksBuffer(recordCount), recordCount);
    buildInfoList.add(info);
  }

  private void setCurrentIndex(final int hashTableIndex, final int buildBatch, final int incomingRecordIndex) throws SchemaChangeException {

    if (hashTableIndex > maxHashTableIndex) {
      maxHashTableIndex = hashTableIndex;
    }

    /* set the current record batch index and the index
     * within the batch at the specified keyIndex. The keyIndex
     * denotes the global index where the key for this record is
     * stored in the hash table
     */
    int hashTableBatch  = hashTableIndex / HashTable.BATCH_SIZE;
    int hashTableOffset = hashTableIndex % HashTable.BATCH_SIZE;

    final ArrowBuf startIndex = startIndices.get(hashTableBatch);
    final long startIndexMemStart = startIndex.memoryAddress() + hashTableOffset * BUILD_RECORD_LINK_SIZE;

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
      final long firstLinkMemStart = firstLink.memoryAddress() + hashTableOffset * BUILD_RECORD_LINK_SIZE;

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
        final long nextLinkMemStart = nextLink.memoryAddress() + incomingRecordIndex * BUILD_RECORD_LINK_SIZE;

        PlatformDependent.putInt(nextLinkMemStart, firstLinkBatch);
        PlatformDependent.putShort(nextLinkMemStart + 4, (short) firstLinkOffset);

        // As the existing (batch, offset) pair is moved out of firstLink into nextLink,
        // now put the new (batch, offset) in the firstLink
        PlatformDependent.putInt(firstLinkMemStart, buildBatch);
        PlatformDependent.putShort(firstLinkMemStart + 4, (short) incomingRecordIndex);
      }
    }
  }

  public ArrowBuf newLinksBuffer(int recordCount) {
    // Each link is 6 bytes.
    // First 4 bytes are used to identify the batch and remaining 2 bytes for record within the batch.
    final ArrowBuf linkBuf = context.getAllocator().buffer(recordCount * BUILD_RECORD_LINK_SIZE);

    // Initialize the buffer. Write -1 (int) in the first four bytes.
    long bufOffset = linkBuf.memoryAddress();
    final long maxBufOffset = bufOffset + recordCount * BUILD_RECORD_LINK_SIZE;
    for(; bufOffset < maxBufOffset; bufOffset += BUILD_RECORD_LINK_SIZE) {
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
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(hyperContainer);
    autoCloseables.add(hashTable);
    autoCloseables.add(outgoing);
    autoCloseables.addAll(buildInfoList);
    autoCloseables.addAll(startIndices);
    AutoCloseables.close(autoCloseables);
  }

  public static class Creator implements DualInputOperator.Creator<HashJoinPOP>{
    @Override
    public DualInputOperator create(OperatorContext context, HashJoinPOP config) throws ExecutionSetupException {
      if(config.isVectorize()) {
        if (context.getOptions().getOption(ENABLE_SPILL) &&
            config.getExtraCondition() == null) {  // TODO: remove this check after adding support for extra conditions
          return new VectorizedSpillingHashJoinOperator(context, config);
        } else {
          return new VectorizedHashJoinOperator(context, config);
        }
      } else {
        return new HashJoinOperator(context, config);
      }
    }

  }

}
