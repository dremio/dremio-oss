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
package com.dremio.sabot.op.filter;

import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ReturnValueExpression;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.Lists;

public class FilterOperator implements SingleInputOperator {

  private final Filter config;
  private final OperatorContext context;
  private final VectorContainer output;

  private State state = State.NEEDS_SETUP;
  private int recordCount;
  private VectorAccessible input;
  private Filterer filter;

  public FilterOperator(Filter pop, OperatorContext context) throws OutOfMemoryException {
    this.config = pop;
    this.context = context;
    this.output = new VectorContainerWithSV(context.getAllocator(), new SelectionVector2(context.getAllocator()));
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    input = accessible;

    switch (input.getSchema().getSelectionVectorMode()) {
      case NONE:
      case TWO_BYTE:
        this.filter = generateSV2Filterer();
        break;
      case FOUR_BYTE:
      default:
        throw new UnsupportedOperationException();
    }
    output.buildSchema(SelectionVectorMode.TWO_BYTE);
    state = State.CAN_CONSUME;
    return output;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    recordCount = filter.filterBatch(records);

    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    state = State.CAN_CONSUME;

    output.setRecordCount(recordCount);
    return recordCount;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state = State.DONE;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(output);
  }

  protected Filterer generateSV2Filterer() throws SchemaChangeException {
    final List<TransferPair> transfers = Lists.newArrayList();
    final ClassGenerator<Filterer> cg = context.getClassProducer().createGenerator(Filterer.TEMPLATE_DEFINITION2).getRoot();

    final LogicalExpression expr = context.getClassProducer().materializeAndAllowComplex(config.getExpr(), input);
    cg.addExpr(new ReturnValueExpression(expr), ClassGenerator.BlockCreateMode.MERGE, true);

    for (final VectorWrapper<?> v : input) {
      final TransferPair pair = v.getValueVector().makeTransferPair(output.addOrGet(v.getField()));
      transfers.add(pair);
    }

    final TransferPair[] tx = transfers.toArray(new TransferPair[transfers.size()]);
    final Filterer filter = cg.getCodeGenerator().getImplementationClass();
    filter.setup(context.getClassProducer().getFunctionContext(), input, output, tx);
    return filter;
  }


  public static class FilterBatchCreator implements SingleInputOperator.Creator<Filter>{

    @Override
    public SingleInputOperator create(OperatorContext context, Filter operator) throws ExecutionSetupException {
      return new FilterOperator(operator, context);
    }


  }
}
