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
package com.dremio.sabot.op.receiver.merging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.MergingReceiverPOP;
import com.dremio.exec.record.ArrowRecordBatchLoader;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.vector.CopyUtil;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.dremio.sabot.op.receiver.RawFragmentBatchProvider;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * The MergingRecordBatch merges pre-sorted record batches from remote senders.
 */
public class MergingReceiverOperator implements ProducerOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingReceiverOperator.class);

  private final OperatorContext context;
  private final VectorContainer outgoingContainer;
  private final MergingReceiverPOP config;
  private final Node[] nodes;
  private final BatchStreamProvider streamProvider;
  private final OperatorStats stats;

  private static enum OutputState {INIT_ON_NEXT, ACTIVE_OUTPUT};

  private State state = State.NEEDS_SETUP;
  private OutputState outputState = OutputState.INIT_ON_NEXT;
  private PriorityQueue<Node> pqueue;
  private Merger merger;

  private int outgoingPosition = 0;

  public static enum Metric implements MetricDef{
    BYTES_RECEIVED,
    NUM_SENDERS,
    NEXT_WAIT_NANOS;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public MergingReceiverOperator(
      final OperatorContext context,
      final BatchStreamProvider streamProvider,
      final MergingReceiverPOP config
      ) throws OutOfMemoryException {
    this.context = context;
    this.streamProvider = streamProvider;
    this.stats = context.getStats();
    this.config = config;
    this.outgoingContainer = context.createOutputVectorContainer(config.getSchema());
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
    this.nodes = new Node[config.getNumSenders()];
    RawFragmentBatchProvider[] fragProviders = streamProvider.getBuffers(config.getSenderMajorFragmentId());
    assert fragProviders.length == config.getNumSenders();
    for(int i = 0; i < nodes.length; i++){
      nodes[i] = new Node(i, fragProviders[i]);
    }
  }

  @Override
  public State getState() {
    return state == State.BLOCKED && !streamProvider.isPotentiallyBlocked() ? State.CAN_PRODUCE : state;
  }

  @Override
  public VectorAccessible setup() throws Exception {
    state.is(State.NEEDS_SETUP);
    this.merger = createMerger();

    // allocate the priority queue with the generated comparator
    this.pqueue = new PriorityQueue<>(nodes.length);
    state = State.CAN_PRODUCE;
    return outgoingContainer;
  }

  private boolean ensureReady() {
    // populate the priority queue with initial values
    for (Node node : nodes) {
      if(node.isReady()){
        continue;
      } else {
        if(!node.nextPosition()){
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public int outputData() throws Exception {
    // use getState here so we can transition out of blocked.
    getState().is(State.CAN_PRODUCE);

    if(!ensureReady()){
      state = State.BLOCKED;
      return 0;
    }

    if(outputState == OutputState.INIT_ON_NEXT){
      allocateOutgoing();
      outgoingPosition = 0;
      outputState = OutputState.ACTIVE_OUTPUT;
    }

    final int maxRecords = context.getTargetBatchSize();

    /**
     * we exit this loop when:
     * - we don't have enough data to create an outgoing batch (one of the incoming streams is waiting for a downstream message).
     * - when our outgoing batch is full
     * - when there are no more records.
     */
    while (!pqueue.isEmpty()) {
      // pop next value from pq and copy to outgoing batch
      final Node node = pqueue.poll();
      node.copyRecordToOutgoingBatch();

      if (outgoingPosition == maxRecords) {
        logger.debug("Outgoing vectors space is full; breaking");
        outputState = OutputState.INIT_ON_NEXT;

        node.nextPosition();
        return outgoingContainer.setAllCount(outgoingPosition);
      }

      if(!node.nextPosition()){
        state = State.BLOCKED;
        return 0;
      }

    }

    // set the value counts in the outgoing vectors
    for (final VectorWrapper<?> vw : outgoingContainer) {
      vw.getValueVector().setValueCount(outgoingPosition);
    }
    outgoingContainer.setRecordCount(outgoingPosition);

    if (pqueue.isEmpty()) {
      state = State.DONE;
    }

    return outgoingPosition;
  }

  private void allocateOutgoing() {
    for (final VectorWrapper<?> w : outgoingContainer) {
      final ValueVector v = w.getValueVector();
      if (v instanceof FixedWidthVector) {
        AllocationHelper.allocate(v, context.getTargetBatchSize(), 1);
      } else {
        v.allocateNewSafe();
      }
    }
  }

  /**
   * Creates a generate class which implements the copy and compare methods.
   *
   * @return instance of a new merger based on generated code
   * @throws SchemaChangeException
   */
  private Merger createMerger() {

    final CodeGenerator<Merger> cg = context.getClassProducer().createGenerator(Merger.TEMPLATE_DEFINITION);
    final ClassGenerator<Merger> g = cg.getRoot();

    ExpandableHyperContainer batch = null;
    boolean first = true;
    for (final Node node : nodes) {
      if (first) {
        batch = new ExpandableHyperContainer(context.getAllocator(), config.getSchema());
        first = false;
      }
      batch.addBatch(node.loader);
    }

    generateComparisons(g, batch);

    g.setMappingSet(copierMappingSet);
    CopyUtil.generateCopies(g, batch, true);
    g.setMappingSet(mainMapping);
    final Merger merger = cg.getImplementationClass();

    merger.doSetup(context.getFunctionContext(), batch, outgoingContainer);
    return merger;
  }

  private final MappingSet mainMapping = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private final MappingSet leftMapping = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private final MappingSet rightMapping = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private final GeneratorMapping copierMapping = new GeneratorMapping("doSetup", "doCopy", null, null);
  private final MappingSet copierMappingSet = new MappingSet(copierMapping, copierMapping);

  private void generateComparisons(final ClassGenerator<?> g, final VectorAccessible batch) throws SchemaChangeException {
    g.setMappingSet(mainMapping);

    for (final Ordering od : config.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      final LogicalExpression expr = context.getClassProducer().materialize(od.getExpr(), batch);
      g.setMappingSet(leftMapping);
      final HoldingContainer left = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(rightMapping);
      final HoldingContainer right = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      final LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getClassProducer());
      final HoldingContainer out = g.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      final JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
    }

    g.getEvalBlock()._return(JExpr.lit(0));
  }

  /**
   * A SabotNode contains a reference to a single value in a specific incoming batch.  It is used
   * as a wrapper for the priority queue.
   */
  private class Node implements Comparable<Node>, AutoCloseable {
    private final int batchId;      // incoming batch
    private final ArrowRecordBatchLoader loader;
    private final RawFragmentBatchProvider provider;

    private int valueIndex;   // value within the batch
    private int outputCounts;
    private int inputCounts;
    private RawFragmentBatch currentBatch;
    private boolean done = false;

    public Node(int batchId, RawFragmentBatchProvider provider) {
      this.batchId = batchId;
      this.loader = new ArrowRecordBatchLoader(context.getAllocator(), config.getSchema());
      this.provider = provider;
    }

    private void clear(){
      if(currentBatch != null && currentBatch.getBody() != null){
        currentBatch.getBody().close();
      }
      valueIndex = 0;
    }

    /**
     * Attempt to get next value and then readd yourself to the priority queue.
     * @return True if we were able to move forward (whether added to queue or not). False if we are blocked on an incoming message.
     */
    private boolean nextPosition(){
      if(currentBatch == null || valueIndex == loader.getRecordCount() - 1){
        clear();

        // get next batch.
        currentBatch = provider.getNext();


        int size;
        // we didn't get a batch. this is because we're pending on a message or we're finished.
        if (currentBatch == null) {
          if(provider.isStreamDone()){
            done = true;
          }
          return provider.isStreamDone();
        } else {

          size = loader.load(currentBatch);

          valueIndex = 0;
        }

        // we received actual data, let's set things up and add to priority queue.
        stats.addLongStat(Metric.BYTES_RECEIVED, currentBatch.getByteCount());
        stats.batchReceived(0, loader.getRecordCount(), size);
        inputCounts += loader.getRecordCount();
        valueIndex = 0;

        pqueue.add(this);
        return true;

      }else{
        valueIndex++;
        pqueue.add(this);
        return true;
      }
    }

    public boolean isReady(){
      return done || currentBatch != null;
    }

    @Override
    public int compareTo(final Node node2) {
      final int leftIndex = (this.batchId << 16) + this.valueIndex;
      final int rightIndex = (node2.batchId << 16) + node2.valueIndex;
      return merger.doEval(leftIndex, rightIndex);
    }

    private void copyRecordToOutgoingBatch() {
      if (!(++outputCounts <= inputCounts)) {
        throw new RuntimeException(String.format("Stream %d input count: %d output count %d", batchId, inputCounts, outputCounts));
      }
      final int inIndex = (batchId << 16) + valueIndex;
      merger.doCopy(inIndex, outgoingPosition);
      outgoingPosition++;
    }

    @Override
    public void close() throws Exception {
      if(currentBatch != null){
        AutoCloseables.close(currentBatch.getBody(), loader);
      } else {
        loader.close();
      }
    }

  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitProducer(this, value);
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> closeables = new ArrayList<>();
    closeables.add(outgoingContainer);
    closeables.addAll(Arrays.asList(nodes));
    AutoCloseables.close(closeables);
  }

  public static class Creator implements ReceiverCreator<MergingReceiverPOP> {

    @Override
    public ProducerOperator create(BatchStreamProvider streams, OperatorContext context, MergingReceiverPOP config)
        throws ExecutionSetupException {
      return new MergingReceiverOperator(context, streams, config);
    }

  }
}
