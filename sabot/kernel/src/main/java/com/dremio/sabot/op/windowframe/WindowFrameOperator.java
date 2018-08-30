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
package com.dremio.sabot.op.windowframe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Order;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

/**
 * support for OVER(PARTITION BY expression1,expression2,... [ORDER BY expressionA, expressionB,...])
 *
 */
public class WindowFrameOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WindowFrameOperator.class);

  private final OperatorContext context;
  private final List<WindowFunction> functions = Lists.newArrayList();
  private final WindowPOP config;
  private final LinkedList<VectorContainer> batches = new LinkedList<>();

  private VectorAccessible incoming;
  private VectorContainer outgoing;
  private ImmutableList<TransferPair> transfers;

  private State state = State.NEEDS_SETUP;
  private WindowFramer[] framers;

  private boolean noMoreToConsume;

  public WindowFrameOperator(OperatorContext context, WindowPOP config) throws OutOfMemoryException {
    this.context = context;
    this.config = config;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);

    incoming = accessible;
    outgoing = context.createOutputVectorContainer();
    createFramers(incoming);
    outgoing.buildSchema();
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME;
    return outgoing;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    batches.add(VectorContainer.getTransferClone(incoming, context.getAllocator()));
    if(canDoWork()){
      state = State.CAN_PRODUCE;
    }
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    noMoreToConsume = true;
    if(!batches.isEmpty()){
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
    }
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    doWork();

    if(batches.isEmpty()){
      state = State.DONE;
    }else if(!noMoreToConsume && !canDoWork()){
      state = State.CAN_CONSUME;
    }
    return outgoing.getRecordCount();
  }


  private int doWork() throws Exception {

    final VectorContainer current = batches.get(0);
    final int recordCount = current.getRecordCount();

    logger.trace("WindowFramer.doWork() START, num batches {}, current batch has {} rows", batches.size(), recordCount);

    // allocate outgoing vectors
    outgoing.allocateNew();

    for (WindowFramer framer : framers) {
      framer.doWork();
    }

    // transfer "non aggregated" vectors
    for (VectorWrapper<?> vw : current) {
      ValueVector v = outgoing.addOrGet(vw.getField());
      TransferPair tp = vw.getValueVector().makeTransferPair(v);
      tp.transfer();
    }

    if(recordCount > 0){
      try{
      outgoing.setAllCount(recordCount);
      }catch(RuntimeException ex){
        throw ex;
      }
    }

    // we can safely free the current batch
    current.close();
    batches.remove(0);

    logger.trace("doWork() END");
    return recordCount;
  }

  /**
   * @return true when all window functions are ready to process the current batch (it's the first batch currently
   * held in memory)
   */
  private boolean canDoWork() {
    if (batches.size() < 2) {
      // we need at least 2 batches even when window functions only need one batch, so we can detect the end of the
      // current partition
      return false;
    }

    final VectorAccessible current = batches.getFirst();
    final int currentSize = current.getRecordCount();
    final VectorAccessible last = batches.getLast();
    final int lastSize = last.getRecordCount();

    final boolean partitionEndReached = !framers[0].isSamePartition(currentSize - 1, current, lastSize - 1, last);
    final boolean frameEndReached = partitionEndReached || !framers[0].isPeer(currentSize - 1, current, lastSize - 1, last);

    for (final WindowFunction function : functions) {
      if (!function.canDoWork(batches.size(), config, frameEndReached, partitionEndReached)) {
        return false;
      }
    }

    return true;
  }


  private void createFramers(VectorAccessible batch) throws SchemaChangeException, IOException, ClassTransformationException {
    assert framers == null : "createFramer should only be called once";

    logger.trace("creating framer(s)");

    final List<LogicalExpression> keyExprs = Lists.newArrayList();
    final List<LogicalExpression> orderExprs = Lists.newArrayList();
    boolean requireFullPartition = false;

    boolean useDefaultFrame = false; // at least one window function uses the DefaultFrameTemplate
    boolean useCustomFrame = false; // at least one window function uses the CustomFrameTemplate

    // all existing vectors will be transferred to the outgoing container in framer.doWork()

    List<TransferPair> transfers = new ArrayList<>();
    for (final VectorWrapper<?> wrapper : batch) {
      ValueVector vector = wrapper.getValueVector();
      TransferPair pair = vector.getTransferPair(context.getAllocator());
      outgoing.add(pair.getTo());
      transfers.add(pair);
    }
    this.transfers = ImmutableList.copyOf(transfers);

    final ClassProducer producer = context.getClassProducer();
    // add aggregation vectors to the container, and materialize corresponding expressions
    for (final NamedExpression ne : config.getAggregations()) {
      final WindowFunction winfun = WindowFunction.fromExpression(ne);

      // build the schema before each pass since we're going to use the outbound schema for value resolution.
      outgoing.buildSchema();

      if (winfun.materialize(ne, outgoing, producer)) {
        functions.add(winfun);
        requireFullPartition |= winfun.requiresFullPartition(config);

        if (winfun.supportsCustomFrames()) {
          useCustomFrame = true;
        } else {
          useDefaultFrame = true;
        }
      }
    }

    outgoing.buildSchema();

    // materialize partition by expressions
    for (final NamedExpression ne : config.getWithins()) {
      keyExprs.add(producer.materialize(ne.getExpr(), batch));
    }

    // materialize order by expressions
    for (final Order.Ordering oe : config.getOrderings()) {
      orderExprs.add(producer.materialize(oe.getExpr(), batch));
    }

    // count how many framers we need
    int numFramers = useDefaultFrame ? 1 : 0;
    numFramers += useCustomFrame ? 1 : 0;
    assert numFramers > 0 : "No framer was needed!";

    framers = new WindowFramer[numFramers];
    int index = 0;
    if (useDefaultFrame) {
      framers[index] = generateFramer(keyExprs, orderExprs, functions, false);
      framers[index].setup(batches, outgoing, context, requireFullPartition, config, context.getFunctionContext());
      index++;
    }

    if (useCustomFrame) {
      framers[index] = generateFramer(keyExprs, orderExprs, functions, true);
      framers[index].setup(batches, outgoing, context, requireFullPartition, config, context.getFunctionContext());
    }
  }

  private WindowFramer generateFramer(final List<LogicalExpression> keyExprs, final List<LogicalExpression> orderExprs,
      final List<WindowFunction> functions, boolean useCustomFrame) throws IOException, ClassTransformationException {

    TemplateClassDefinition<WindowFramer> definition = useCustomFrame ?
      WindowFramer.FRAME_TEMPLATE_DEFINITION : WindowFramer.NOFRAME_TEMPLATE_DEFINITION;
    final ClassGenerator<WindowFramer> cg = context.getClassProducer().createGenerator(definition).getRoot();

    {
      // generating framer.isSamePartition()
      final GeneratorMapping IS_SAME_PARTITION_READ = GeneratorMapping.create("isSamePartition", "isSamePartition", null, null);
      final MappingSet isaB1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      final MappingSet isaB2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      setupIsFunction(cg, keyExprs, isaB1, isaB2);
    }

    {
      // generating framer.isPeer()
      final GeneratorMapping IS_SAME_PEER_READ = GeneratorMapping.create("isPeer", "isPeer", null, null);
      final MappingSet isaP1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      final MappingSet isaP2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      // isPeer also checks if it's the same partition
      setupIsFunction(cg, Iterables.concat(keyExprs, orderExprs), isaP1, isaP2);
    }

    for (final WindowFunction function : functions) {
      // only generate code for the proper window functions
      if (function.supportsCustomFrames() == useCustomFrame) {
        function.generateCode(cg);
      }
    }

    cg.getBlock("resetValues")._return(JExpr.TRUE);

    return cg.getCodeGenerator().getImplementationClass();
  }

  /**
   * setup comparison functions isSamePartition and isPeer
   */
  private void setupIsFunction(final ClassGenerator<WindowFramer> cg, final Iterable<LogicalExpression> exprs,
                               final MappingSet leftMapping, final MappingSet rightMapping) {
    cg.setMappingSet(leftMapping);
    for (LogicalExpression expr : exprs) {
      if (expr == null) {
        continue;
      }

      cg.setMappingSet(leftMapping);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      cg.setMappingSet(rightMapping);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);

      final LogicalExpression fh =
        FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(first, second, context.getClassProducer());
      final ClassGenerator.HoldingContainer out = cg.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> closeables = new ArrayList<>();
    closeables.add(outgoing);
    if (framers != null) {
      closeables.addAll(Arrays.asList(framers));
    }
    closeables.addAll(batches);
    AutoCloseables.close(closeables);
  }

  public static class Creator implements SingleInputOperator.Creator<WindowPOP>{

    @Override
    public SingleInputOperator create(OperatorContext context, WindowPOP operator) throws ExecutionSetupException {
      return new WindowFrameOperator(context, operator);
    }

  }

}
