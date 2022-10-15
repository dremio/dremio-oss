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
package com.dremio.sabot.driver;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.physical.config.AbstractTableFunctionPOP;
import com.dremio.exec.physical.config.BridgeFileReader;
import com.dremio.exec.physical.config.EmptyValues;
import com.dremio.exec.physical.config.HashJoinPOP;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.physical.config.UnionAll;
import com.dremio.exec.physical.config.Values;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.SharedResourcesContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.receiver.IncomingBuffers;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.Operator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Factor class used to generate a PipelineDriver.
 */
public class PipelineCreator {

  private final FragmentExecutionContext fec;
  private final OperatorContext.Creator operatorContextCreator;
  private final FunctionLookupContext functionLookupContext;
  private final TunnelProvider tunnelProvider;
  private final IncomingBuffers buffers;
  private final OperatorCreator creator;
  private final List<Wrapped<?>> operators = new ArrayList<>();
  private final List<Operator.ShrinkableOperator> shrinkableOperators = new ArrayList<>();
  private final SharedResourcesContext sharedResourcesContext;

  private PipelineCreator(
      FragmentExecutionContext fec,
      OperatorContext.Creator operatorContextCreator,
      FunctionLookupContext functionLookupContext,
      IncomingBuffers buffers,
      OperatorCreator creator,
      TunnelProvider tunnelProvider,
      SharedResourcesContext sharedResourcesContext) {
    super();
    this.operatorContextCreator = operatorContextCreator;
    this.functionLookupContext = functionLookupContext;
    this.fec = fec;
    this.buffers = buffers;
    this.creator = creator;
    this.tunnelProvider = tunnelProvider;
    this.sharedResourcesContext = sharedResourcesContext;
  }

  public static Pipeline get(
      FragmentExecutionContext fec,
      IncomingBuffers buffers,
      OperatorCreator creator,
      OperatorContext.Creator operatorContextCreator,
      FunctionLookupContext functionLookupContext,
      PhysicalOperator operator,
      TunnelProvider tunnelProvider,
      SharedResourcesContext sharedResourcesContext
      ) throws Exception {

    PipelineCreator pipelineCreator = new PipelineCreator(fec,
      operatorContextCreator,
      functionLookupContext,
      buffers,
      creator,
      tunnelProvider,
      sharedResourcesContext);
    return pipelineCreator.get(operator);
  }

  private Pipeline get(PhysicalOperator operator) throws Exception {
    try(RollbackCloseable closeable = AutoCloseables.rollbackable(AutoCloseables.all(operators))) {
      final CreatorVisitor visitor = new CreatorVisitor();
      OpPipe opPipe = operator.accept(visitor, null);
      Preconditions.checkNotNull(opPipe.getPipe());
      Pipeline driver = new Pipeline(opPipe.getPipe(), visitor.terminal, operators, shrinkableOperators, sharedResourcesContext);
      closeable.commit();
      return driver;
    }
  }

  private class CreatorVisitor extends AbstractPhysicalVisitor<OpPipe, Void,  Exception> {

    private TerminalOperator terminal;

    /**
     * Record the operators we're generating so that we can close them out.
     * @param operator
     * @return
     */
    private <X extends Operator, T extends SmartOp<X>> T record(T operator){
      operators.add(operator);
      Operator inner = operator.getInner();
      if (inner instanceof Operator.ShrinkableOperator) {
        // add the inner operator to the list of shrinkable operators
        shrinkableOperators.add((Operator.ShrinkableOperator) inner);
      }
      return operator;
    }

    private void terminal(TerminalOperator terminal){
      assert this.terminal == null;
      this.terminal = terminal;
    }

    @Override
    public OpPipe visitUnion(UnionAll config, Void value) throws Exception {
      return dualInput(config);
    }

    @Override
    public OpPipe visitMergeJoin(MergeJoinPOP join, Void value) throws Exception {
      return dualInput(join);
    }

    @Override
    public OpPipe visitHashJoin(HashJoinPOP join, Void value) throws Exception {
      return dualInput(join);
    }

    @Override
    public OpPipe visitNestedLoopJoin(NestedLoopJoinPOP join, Void value) throws Exception {
      return dualInput(join);
    }

    @Override
    public OpPipe visitEmptyValues(EmptyValues op, Void value) throws Exception {
      return visitSubScan(op, value);
    }

    @Override
    public OpPipe visitValues(Values op, Void value) throws Exception {
      return visitSubScan(op, value);
    }

    private OpPipe dualInput(PhysicalOperator config) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      DualInputOperator sink = record(
          SmartOp.contextualize(
              creator.getDualInputOperator(context, config),
              context,
              config,
              functionLookupContext));
      List<PhysicalOperator> operators = Lists.newArrayList(config.iterator());
      Preconditions.checkArgument(operators.size() == 2);
      OpPipe left = operators.get(0).accept(this, null);
      OpPipe right = operators.get(1).accept(this,  null);
      return pair(new WyePipe(sink, left, right), sink).associate(left, right);
    }

    @Override
    public OpPipe visitSender(Sender config, Void value) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      TerminalOperator sink = record(
          SmartOp.contextualize(
              creator.getTerminalOperator(tunnelProvider, context, config),
              context,
              config,
              functionLookupContext));
      terminal(sink);
      OpPipe input = config.getChild().accept(this, null);
      return pair(new StraightPipe(sink, input), sink).associate(input);
    }

    @Override
    public OpPipe visitReceiver(Receiver config, Void value) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      ProducerOperator receiver = record(
          SmartOp.contextualize(
              creator.getReceiverOperator(buffers, context, config),
              context,
              config,
              functionLookupContext));
      return pair(null, receiver);
    }

    @Override
    public OpPipe visitBridgeFileReader(BridgeFileReader config, Void value) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      ProducerOperator receiver = record(
        SmartOp.contextualize(
          creator.getReceiverOperator(buffers, context, config),
          context,
          config,
          functionLookupContext));
      return pair(null, receiver);
    }

    @Override
    public OpPipe visitSubScan(SubScan config, Void value) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      ProducerOperator producer = record(
          SmartOp.contextualize(
              creator.getProducerOperator(fec, context, config),
              context,
              config,
              functionLookupContext));
      return pair(null, producer);
    }

    @Override
    public OpPipe visitGroupScan(GroupScan config, Void value) throws Exception {
      if(config instanceof SubScan){
        // some scans are both subscans and groupscans (e.g. SystemTableScan). If they match do, delegate to visitSubScan.
        return visitSubScan((SubScan) config, value);
      } else {
        return super.visitGroupScan(config, value);
      }
    }

    @Override
    public OpPipe visitScreen(Screen config, Void value) throws Exception {
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      TerminalOperator sink = record(
          SmartOp.contextualize(
              creator.getTerminalOperator(tunnelProvider, context, config), context, config, functionLookupContext));
      terminal(sink);
      OpPipe input = config.getChild().accept(this, null);
      return pair(new StraightPipe(sink, input), sink).associate(input);
    }

    @Override
    public OpPipe visitTableFunction(AbstractTableFunctionPOP config, Void value) throws Exception {
      Preconditions.checkArgument(config instanceof AbstractSingle, "Object %s was expected to be implementation of AbstractSingle, but was not. Class was %s.", config.toString(), config.getClass().getName());
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      SingleInputOperator sink = record(
              SmartOp.contextualize(creator.getSingleInputOperator(fec, context, config),
                      context,
                      config,
                      functionLookupContext));
      OpPipe input = ((AbstractSingle) config).getChild().accept(this, null);
      return pair(new StraightPipe(sink, input), sink).associate(input);
    }

    @Override
    public OpPipe visitOp(PhysicalOperator config, Void value) throws Exception {
      Preconditions.checkArgument(config instanceof AbstractSingle, "Object %s was expected to be implementation of AbstractSingle, but was not. Class was %s.", config.toString(), config.getClass().getName());
      OperatorContext context = operatorContextCreator.newOperatorContext(config);
      SingleInputOperator sink = record(
        SmartOp.contextualize(creator.getSingleInputOperator(context, config),
          context,
          config,
          functionLookupContext));
      OpPipe input = ((AbstractSingle) config).getChild().accept(this, null);
      return pair(new StraightPipe(sink, input), sink).associate(input);
    }

  }

  private static OpPipe pair(Pipe pipe, Operator op){
    return new OpPipe(pipe, op);
  }

  static class OpPipe {
    final Pipe pipe;
    final Operator operator;

    private OpPipe(Pipe pipe, Operator operator) {
      this.operator = operator;
      this.pipe = pipe;
    }

    private OpPipe associate(OpPipe... upstreams){
      for(OpPipe p : upstreams){
        if(p != null && p.pipe != null){
          p.pipe.setDownstream(this.pipe);
        }
      }
      return this;
    }

    public Operator getOperator() {
      return operator;
    }

    public Pipe getPipe() {
      return pipe;
    }

  }

}
