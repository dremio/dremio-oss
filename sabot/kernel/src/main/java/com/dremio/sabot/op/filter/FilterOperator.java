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
package com.dremio.sabot.op.filter;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.proto.UserBitShared.OperatorProfileDetails;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.filter.FilterStats.Metric;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;


public class FilterOperator implements SingleInputOperator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterOperator.class);

  private final Filter config;
  private final OperatorContext context;
  private final ExpressionEvaluationOptions filterOptions;
  private final VectorContainer output;

  private State state = State.NEEDS_SETUP;
  private int recordCount;
  private VectorAccessible input;
  private TransferPair[] tx;
  private Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
  private Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();
  private ExpressionSplitter splitter;

  public FilterOperator(Filter pop, OperatorContext context) throws OutOfMemoryException {
    this.config = pop;
    this.context = context;
    this.filterOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.filterOptions.setCodeGenOption(context.getOptions().getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName()).getStringVal());
    this.output = context.createOutputVectorContainerWithSV();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    input = accessible;

    switch (input.getSchema().getSelectionVectorMode()) {
      case NONE:
        generateSV2Filterer(accessible);
        break;
      case TWO_BYTE:
        throw new UnsupportedOperationException("Filter operator does not support SV2");
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Filter operator does not support SV4");
      default:
        throw new UnsupportedOperationException("Unsupported selection vector mode: " + input.getSchema().getSelectionVectorMode());
    }
    output.buildSchema(SelectionVectorMode.TWO_BYTE);
    state = State.CAN_CONSUME;
    return output;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    if (records == 0) {
      state = State.CAN_PRODUCE;
      return;
    }

    recordCount = splitter.filterData(records, javaCodeGenWatch, gandivaCodeGenWatch);

    doTransfers();
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
    AutoCloseables.close(output, splitter);
    context.getStats().addLongStat(Metric.JAVA_EXECUTE_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    context.getStats().addLongStat(Metric.GANDIVA_EXECUTE_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  protected void generateSV2Filterer(VectorAccessible accessible) throws Exception {
    setupTransfers();
    setupSplitter(accessible);

    OperatorStats stats = context.getStats();
    stats.addLongStat(Metric.JAVA_EXPRESSIONS, splitter.getNumExprsInJava());
    stats.addLongStat(Metric.GANDIVA_EXPRESSIONS, splitter.getNumExprsInGandiva());
    stats.addLongStat(Metric.MIXED_SPLITS, splitter.getNumSplitsInBoth());
    stats.addLongStat(Metric.JAVA_BUILD_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.addLongStat(Metric.GANDIVA_BUILD_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.setProfileDetails(OperatorProfileDetails
      .newBuilder()
      .addAllSplitInfos(splitter.getSplitInfos())
      .build()
    );

    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  private void setupTransfers() {
    final List<TransferPair> transfers = Lists.newArrayList();
    for (final VectorWrapper<?> v : input) {
      final TransferPair pair = v.getValueVector().makeTransferPair(output.addOrGet(v.getField()));
      transfers.add(pair);
    }
    tx = transfers.toArray(new TransferPair[transfers.size()]);
  }

  private void setupSplitter(VectorAccessible accessible) throws Exception {
    final LogicalExpression expr = context.getClassProducer().materializeAndAllowComplex(filterOptions,
      config.getExpr(), input);
    splitter = new ExpressionSplitter(context, accessible, filterOptions,
      context.getClassProducer().getFunctionLookupContext().isDecimalV2Enabled());
    splitter.setupFilter(output, new NamedExpression(expr, new FieldReference("_filter_")), javaCodeGenWatch, gandivaCodeGenWatch);
  }

  private void doTransfers(){
    for(TransferPair t : tx){
      t.transfer();
    }
  }

  public static class FilterBatchCreator implements SingleInputOperator.Creator<Filter>{

    @Override
    public SingleInputOperator create(OperatorContext context, Filter operator) throws ExecutionSetupException {
      return new FilterOperator(operator, context);
    }


  }

  @FunctionalInterface
  public interface FilterFunction {
    Integer apply(Integer t) throws Exception;
  }
}
