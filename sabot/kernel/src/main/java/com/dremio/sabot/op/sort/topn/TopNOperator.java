/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.sort.topn;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.TopN;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.dremio.sabot.op.sort.SortRecordBatchBuilder;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.sabot.op.sort.external.Sv4HyperContainer;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Stopwatch;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class TopNOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopNOperator.class);

  private final int batchPurgeThreshold;
  private final TopN config;
  private final OperatorContext context;

  private State state = State.NEEDS_SETUP;
  private int batchesOutput = 0;
  private VectorAccessible incoming;
  private VectorContainer outgoing;

  // used to determine whether we should purge.
  private long countSincePurge;
  private int batchCount;

  // used once operator has consumed all data.
  private SelectionVector4 finalOrder;

  // generated code.
  private PriorityQueue priorityQueue;
  private Copier copier;

  public TopNOperator(OperatorContext context, TopN popConfig) {
    this.config = popConfig;
    this.context = context;
    this.batchPurgeThreshold = context.getConfig().getInt(ExecConstants.BATCH_PURGE_THRESHOLD);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(final VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    this.outgoing = new VectorContainer(context.getAllocator());
    outgoing.addSchema(incoming.getSchema());
    outgoing.allocateNew();
    outgoing.buildSchema(SelectionVectorMode.NONE);

    priorityQueue = createNewPriorityQueue(context.getClassProducer(), config.getOrderings());
    copier = CopierOperator.getGenerated4Copier(context.getClassProducer(), priorityQueue.getHyperBatch(), outgoing);
    state = State.CAN_CONSUME;
    return outgoing;
  }

  @Override
  public void consumeData(int records) throws Exception {

    countSincePurge += incoming.getRecordCount();
    batchCount++;

    priorityQueue.add(new RecordBatchData(incoming, context.getAllocator()));

    if (countSincePurge > config.getLimit() && batchCount > batchPurgeThreshold) {
      purge();
      countSincePurge = 0;
      batchCount = 0;
    }

  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);

    // get final order.
    priorityQueue.generate();
    finalOrder = priorityQueue.getFinalSv4();

    final Sv4HyperContainer source = priorityQueue.getHyperBatch();
    source.setSelectionVector4(finalOrder);

    copier.setupRemover(context.getFunctionContext(), source, outgoing);

    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    if(batchesOutput > 0){
      // only increment sv4 after first return
      final boolean hasMore = finalOrder.next();

      if(!hasMore){
        state = State.DONE;
        return 0;
      }
    }

    final int targetCount = finalOrder.getCount();
    final int copied = copier.copyRecords(0, targetCount);
    if(copied != targetCount){
      throw UserException.memoryError().message("Ran out of memory while trying to output records.").build(logger);
    }

    batchesOutput++;
    return outgoing.setAllCount(copied);
  }

  private void purge() throws SchemaChangeException {
    final Stopwatch watch = Stopwatch.createStarted();

    // setup a copier to copy into single batches.
    final Sv4HyperContainer source = priorityQueue.getHyperBatch();
    final SelectionVector4 selectionVector4 = priorityQueue.getHeapSv4();
    source.setSelectionVector4(selectionVector4);

    final VectorContainer target = new VectorContainer(context.getAllocator());
    for (VectorWrapper<?> i : source) {
      ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
      target.add(v);
    }
    copier.setupRemover(context.getFunctionContext(), source, target);

    final SortRecordBatchBuilder builder = new SortRecordBatchBuilder(context.getAllocator());
    try {
      do {
        final int count = selectionVector4.getCount();
        final int copiedRecords = copier.copyRecords(0, count);
        if (copiedRecords != count) {
          throw UserException.memoryError().message("Ran out of memory while trying to sort records.").build(logger);
        }
        for (VectorWrapper<?> v : target) {
          ValueVector.Mutator m = v.getValueVector().getMutator();
          m.setValueCount(count);
        }
        target.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        target.setRecordCount(count);
        builder.add(target);
      } while (selectionVector4.next());

      selectionVector4.clear();
      VectorContainer newQueue = new VectorContainer();
      builder.build(newQueue);
      priorityQueue.resetQueue(newQueue, builder.getSv4().createNewWrapperCurrent());
      builder.getSv4().clear();
      selectionVector4.clear();

    } finally {
      builder.close();
    }
    logger.debug("Took {} us to purge", watch.elapsed(TimeUnit.MICROSECONDS));
  }

  private PriorityQueue createNewPriorityQueue(ClassProducer producer, List<Ordering> orderings) throws ClassTransformationException, IOException, SchemaChangeException {

    final MappingSet leftMapping = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet mainMapping = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet rightMapping = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

    final CodeGenerator<PriorityQueue> cg = producer.createGenerator(PriorityQueue.TEMPLATE_DEFINITION);
    final ClassGenerator<PriorityQueue> g = cg.getRoot();
    g.setMappingSet(mainMapping);


    final Sv4HyperContainer hyperBatch = new Sv4HyperContainer(context.getAllocator(), incoming.getSchema());
    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      final LogicalExpression expr = producer.materialize(od.getExpr(), hyperBatch);
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
        FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                       producer);
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));

    PriorityQueue q = cg.getImplementationClass();
    q.init(hyperBatch, config.getLimit(), context.getFunctionContext(), context.getAllocator(), incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE, context.getTargetBatchSize());
    return q;

  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, finalOrder, priorityQueue, copier);
  }

  public static class TopNCreator implements SingleInputOperator.Creator<TopN>{

    @Override
    public SingleInputOperator create(OperatorContext context, TopN operator) throws ExecutionSetupException {
      return new TopNOperator(context, operator);
    }

  }
}
