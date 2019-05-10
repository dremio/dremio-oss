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
package com.dremio.sabot.op.sender.partition;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.carrotsearch.hppc.IntArrayList;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.vector.CopyUtil;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.BaseSender;
import com.dremio.sabot.op.sender.partition.vectorized.VectorizedPartitionSenderOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.annotations.VisibleForTesting;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JType;

public class PartitionSenderOperator extends BaseSender {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionSenderOperator.class);

  private final OperatorContext context;
  private final HashPartitionSender config;
  private final OperatorStats stats;
  private final double cost;
  private final int outGoingBatchCount;
  private final AtomicIntegerArray remainingReceivers;
  private final AtomicInteger remaingReceiverCount;
  private final TunnelProvider tunnelProvider;

  /**
   * A volatile value set by an external to operation thread that means that this nobody is listening to this operator. As such, it can terminate as soon as it likes.
   */
  private volatile boolean nobodyListening = false;

  private State state = State.NEEDS_SETUP;

  private PartitionerDecorator partitioner;
  private VectorAccessible incoming;

  long minReceiverRecordCount = Long.MAX_VALUE;
  long maxReceiverRecordCount = Long.MIN_VALUE;
  protected final int numberPartitions;
  protected final int actualPartitions;
  private long recordsConsumed = 0;

  private IntArrayList terminations = new IntArrayList();

  public enum Metric implements MetricDef {
    BATCHES_SENT,
    RECORDS_SENT,
    MIN_RECORDS,
    MAX_RECORDS,
    N_RECEIVERS,
    BYTES_SENT,
    SENDING_THREADS_COUNT,
    COST,
    NUM_COPIES,
    FIXED_COPY_NS,
    VARIABLE_COPY_NS,
    BINARY_COPY_NS,
    GENERIC_COPY_NS,
    PRECOPY_NS,
    FLUSH_NS,
    NUM_FLUSHES,
    BUCKET_SIZE;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public PartitionSenderOperator(
      OperatorContext context,
      TunnelProvider tunnelProvider,
      HashPartitionSender config) {
    super(config);
    this.context = context;
    this.tunnelProvider = tunnelProvider;
    this.config = config;
    this.stats = context.getStats();
    outGoingBatchCount = config.getDestinations().size();
    remainingReceivers = new AtomicIntegerArray(outGoingBatchCount);
    remaingReceiverCount = new AtomicInteger(outGoingBatchCount);
    stats.setLongStat(Metric.N_RECEIVERS, outGoingBatchCount);
    // Algorithm to figure out number of threads to parallelize output
    // numberOfRows/sliceTarget/numReceivers/threadfactor
    this.cost = config.getChild().getProps().getCost();
    this.numberPartitions = getNumberPartitions(context, config);
    this.actualPartitions = outGoingBatchCount > numberPartitions ? numberPartitions : outGoingBatchCount;
    this.stats.setLongStat(Metric.SENDING_THREADS_COUNT, actualPartitions);
    this.stats.setDoubleStat(Metric.COST, this.cost);
    logger.debug("Preliminary number of sending threads is: " + numberPartitions);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    checkSchema(incoming.getSchema());
    createPartitioners(incoming);

    state = State.CAN_CONSUME;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    if(nobodyListening){
      state = State.DONE;
      return;
    }

    partitioner.partitionBatch(incoming);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);

    if(nobodyListening){
      state = State.DONE;
      return;
    }

    partitioner.finishWork();
    state = State.DONE;
  }


  @VisibleForTesting
  protected void createPartitioners(VectorAccessible incoming) throws Exception {
    final int divisor = Math.max(1, outGoingBatchCount/actualPartitions);
    final int longTail = outGoingBatchCount % actualPartitions;

    final List<Partitioner> subPartitioners = createClassInstances(actualPartitions);
    int startIndex = 0;
    int endIndex = 0;

    boolean success = false;
    try {
      for (int i = 0; i < actualPartitions; i++) {
        startIndex = endIndex;
        endIndex = (i < actualPartitions - 1) ? startIndex + divisor : outGoingBatchCount;
        if (i < longTail) {
          endIndex++;
        }
        final OperatorStats partitionStats = new OperatorStats(stats, true);
        subPartitioners.get(i).setup(incoming, config, partitionStats, context, tunnelProvider,
            actualPartitions, startIndex, endIndex);
      }

      synchronized (this) {
        partitioner = new PartitionerDecorator(subPartitioners, stats, context);
        for (int index = 0; index < terminations.size(); index++) {
          partitioner.getOutgoingBatches(terminations.buffer[index]).terminate();
        }
        terminations.clear();
      }

      success = true;
    } finally {
      if (!success) {
        AutoCloseables.close(subPartitioners);
      }
    }
  }

  private List<Partitioner> createClassInstances(int actualPartitions) throws SchemaChangeException {
    // set up partitioning function
    final LogicalExpression expr = config.getExpr();
    final ClassGenerator<Partitioner> cg = context.getClassProducer().createGenerator(Partitioner.TEMPLATE_DEFINITION).getRoot();
    ClassGenerator<Partitioner> cgInner = cg.getInnerGenerator("OutgoingRecordBatch");

    final LogicalExpression materializedExpr = context.getClassProducer().materialize(expr, incoming);

    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    JExpression bucket = JExpr.direct("bucket");

    // generate evaluate expression to determine the hash
    ClassGenerator.HoldingContainer exprHolder = cg.addExpr(materializedExpr);
    cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "bucket", exprHolder.getValue().mod(JExpr.lit(outGoingBatchCount)));
    cg.getEvalBlock()._return(cg.getModel().ref(Math.class).staticInvoke("abs").arg(bucket));

    CopyUtil.generateCopies(cgInner, incoming, incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);

    // compile and setup generated code
    List<Partitioner> subPartitioners = cg.getCodeGenerator().getImplementationClass(actualPartitions);
    return subPartitioners;
  }

  /**
   * Find min and max record count seen across the outgoing batches and put them in stats.
   */
  private void updateAggregateStats() {
    for (Partitioner part : partitioner.getPartitioners() ) {
      for (PartitionOutgoingBatch o : part.getOutgoingBatches()) {
        long totalRecords = o.getTotalRecords();
        minReceiverRecordCount = Math.min(minReceiverRecordCount, totalRecords);
        maxReceiverRecordCount = Math.max(maxReceiverRecordCount, totalRecords);
      }
    }
    stats.setLongStat(Metric.MIN_RECORDS, minReceiverRecordCount);
    stats.setLongStat(Metric.MAX_RECORDS, maxReceiverRecordCount);
  }

  @Override
  public void receivingFragmentFinished(FragmentHandle handle) {
    final int id = handle.getMinorFragmentId();
    if (remainingReceivers.compareAndSet(id, 0, 1)) {
      synchronized (this) {
        if (partitioner == null) {
          terminations.add(id);
        } else {
          partitioner.getOutgoingBatches(id).terminate();
        }
      }

      int remaining = remaingReceiverCount.decrementAndGet();
      if (remaining == 0) {
        nobodyListening = true;
      }
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @Override
  public void close() throws Exception {
    if (partitioner != null) {
      updateAggregateStats();
      partitioner.close();
    }
  }

  static int getNumberPartitions(OperatorContext context, HashPartitionSender config){
    final OptionManager optMgr = context.getOptions();
    long sliceTarget = optMgr.getOption(ExecConstants.SLICE_TARGET).getNumVal();
    int threadFactor = optMgr.getOption(PlannerSettings.PARTITION_SENDER_THREADS_FACTOR.getOptionName()).getNumVal()
      .intValue();
    int tmpParts = 1;
    int outGoingBatchCount = config.getDestinations().size();
    double cost = config.getProps().getCost();
    if ( sliceTarget != 0 && outGoingBatchCount != 0 ) {
      tmpParts = (int) Math.round((((cost / (sliceTarget*1.0)) / (outGoingBatchCount*1.0)) / (threadFactor*1.0)));
      if ( tmpParts < 1) {
        tmpParts = 1;
      }
    }
    final int imposedThreads = optMgr.getOption(PlannerSettings.PARTITION_SENDER_SET_THREADS.getOptionName()).getNumVal()
      .intValue();
    if (imposedThreads > 0 ) {
      return imposedThreads;
    } else {
      return Math.min(tmpParts, optMgr.getOption(PlannerSettings.PARTITION_SENDER_MAX_THREADS.getOptionName()).getNumVal()
        .intValue());
    }
  }

  @VisibleForTesting
  protected PartitionerDecorator getPartitioner() {
    return partitioner;
  }

  public static class Creator implements TerminalOperator.Creator<HashPartitionSender> {
    @Override
    public TerminalOperator create(TunnelProvider tunnelProvider, OperatorContext context, HashPartitionSender operator)
        throws ExecutionSetupException {
      if (context.getOptions().getOption(ExecConstants.ENABLE_VECTORIZED_PARTITIONER)) {
        return new VectorizedPartitionSenderOperator(context, tunnelProvider, operator);
      } else {
      return new PartitionSenderOperator(context, tunnelProvider, operator);
      }
    }
  }

}

