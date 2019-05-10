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
package com.dremio.sabot.op.aggregate.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorWriteExpression;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JExpr;

/**
 * Implements aggregation based on the requirement that similar values are clustered.
 *
 * Two input concepts:
 *  - onDeck (pending data to be consumed)
 *  - atBat (primary data batch being consumed)
 *
 *  After first record, consumption does:
 *  - compare first onDeck record with last atBat record.
 *    - If they match, promote onDeck records to atBat then add first new atBat record to aggregation.
 *    - If they don't match, output last atBat record and aggregations, then promote onDeck records to atBat
 *
 * Most work is done in produceOutput(). This means this operator will produce numerous zero records batches
 * (any time we don't have enough data to produce input). This was done for simplicity and to keep the core
 * algorithm in a single place.
 *
 */
public class StreamingAggOperator implements SingleInputOperator  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingAggOperator.class);

  private final OperatorStats stats;
  private final OperatorContext context;
  private final StreamingAggregate config;
  private final VectorContainer outgoing;

  private State state = State.NEEDS_SETUP;
  private boolean done = false;
  private StreamingAggregator aggregator;
  private VectorAccessible onDeckInput;
  private VectorContainer atBatInput;
  private ImmutableList<TransferPair> onDeckAtBatTransfers;

  private boolean allocateNewOutput = true;
  private int nextOutputCount;
  private long consumedCount;

  /*
   * DRILL-2277, DRILL-2411: For straight aggregates without a group by clause we need to perform special handling when
   * the incoming batch is empty. In the case of the empty input into the streaming aggregate we need
   * to return a single batch with one row. For count we need to return 0 and for all other aggregate
   * functions like sum, avg etc we need to return an explicit row with NULL. Since we correctly allocate the type of
   * the outgoing vectors (required for count and nullable for other aggregate functions) all we really need to do
   * is simply set the record count to be 1 in such cases. For nullable vectors we don't need to do anything because
   * if we don't set anything the output will be NULL, however for required vectors we explicitly zero out the vector
   * since we don't zero it out while allocating it.
   *
   * We maintain some state to remember that we have done such special handling.
   */
  private static final int SPECIAL_BATCH_COUNT = 1;

  public StreamingAggOperator(OperatorContext context, StreamingAggregate config) {
    this.config = config;
    this.context = context;
    this.stats = context.getStats();
    this.outgoing = context.createOutputVectorContainer();
  }

  @Override
  public VectorAccessible setup(final VectorAccessible onDeckInput) {
    state.is(State.NEEDS_SETUP);

    this.onDeckInput = onDeckInput;
    this.atBatInput = new VectorContainer(context.getAllocator());

    final ClassGenerator<StreamingAggregator> cg = context.getClassProducer().createGenerator(StreamingAggTemplate.TEMPLATE_DEFINITION).getRoot();

    final LogicalExpression[] keyExprs = new LogicalExpression[config.getGroupByExprs().size()];
    final LogicalExpression[] valueExprs = new LogicalExpression[config.getAggrExprs().size()];
    final TypedFieldId[] keyOutputIds = new TypedFieldId[config.getGroupByExprs().size()];


    for (int i = 0; i < keyExprs.length; i++) {
      final NamedExpression ne = config.getGroupByExprs().get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), onDeckInput);
      if (expr == null) {
        continue;
      }
      keyExprs[i] = expr;
      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      final ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      keyOutputIds[i] = outgoing.add(vector);
    }

    for (int i = 0; i < valueExprs.length; i++) {
      final NamedExpression ne = config.getAggrExprs().get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), onDeckInput);
      if (expr instanceof IfExpression) {
        throw UserException.unsupportedError().message("Union type not supported in aggregate functions").build(logger);
      }
      if (expr == null) {
        continue;
      }

      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      TypedFieldId id = outgoing.add(vector);
      valueExprs[i] = new ValueVectorWriteExpression(id, expr, true);
    }

    List<TransferPair> transfers = new ArrayList<>();
    for(VectorWrapper<?> w : onDeckInput){
      TransferPair pair = w.getValueVector().getTransferPair(context.getAllocator());
      atBatInput.add(pair.getTo());
      transfers.add(pair);
    }
    atBatInput.buildSchema();
    this.onDeckAtBatTransfers = ImmutableList.copyOf(transfers);

    codeGenAtBat(cg, keyExprs, valueExprs);
    codeGenOnDeck(cg, keyExprs, valueExprs);
    codeGenOutputKeys(cg, keyOutputIds, keyExprs);
    codeGenGetVectorIndex(cg);
    cg.getBlock("resetValues")._return(JExpr.TRUE);

    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    aggregator = cg.getCodeGenerator().getImplementationClass();
    aggregator.setup(context.getFunctionContext(), onDeckInput, atBatInput, outgoing, context.getTargetBatchSize(), new TransferOnDeckAtBat());

    state = State.CAN_CONSUME;
    return outgoing;
  }

  private void transitionOnDeckToAtBat(){
    for (TransferPair p : onDeckAtBatTransfers) {
      p.transfer();
    }
    atBatInput.setRecordCount(onDeckInput.getRecordCount());
  }

  public class TransferOnDeckAtBat {
    public void transfer(){
      transitionOnDeckToAtBat();
    }
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    assert records > 0;

    if(allocateNewOutput){
      outgoing.allocateNew();
      allocateNewOutput = false;
    }

    if(consumedCount == 0){
      // this happens for the very first record this operator consumes.
      transitionOnDeckToAtBat();
      aggregator.consumeFirstRecord();
    } else {
      nextOutputCount = aggregator.consumeBoundaryRecordAndSwap();
    }

    consumedCount += records;

    state = State.CAN_PRODUCE;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.CAN_PRODUCE;
    done = true;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    if(allocateNewOutput){
      outgoing.allocateNew();
    }

    // generally, we will output data so we'll need to reallocate on next consumption
    allocateNewOutput = true;

    if(done){
      state = State.DONE;
      if (consumedCount == 0) {
        if(config.getAggrExprs().size() == outgoing.getNumberOfColumns()){
          // no group by.
          constructSpecialBatch();
          state = State.DONE;
          return outgoing.setAllCount(1);
        } else {
          // group by but no records.
          state = State.DONE;
          return 0;
        }
      } else {
        return outgoing.setAllCount(aggregator.closeLastAggregation());
      }
    }

    // we arrived here having hit capacity on the outgoing batch while consuming the boundary record.
    if(nextOutputCount != 0){
      int localOutputCount = nextOutputCount;
      nextOutputCount = 0;
      return outgoing.setAllCount(localOutputCount);
    }

    // let's try to consume more of our current batch.
    final int outputBatchFinishedCount = aggregator.consumeRecords();

    // if we didn't finish filling an outgoing batch, we need more data.
    if(outputBatchFinishedCount == 0){
      state = State.CAN_CONSUME;

      // in this case, we need more data but we have pending output, make sure to avoid allocation on next output.
      allocateNewOutput = false;
      return 0;
    }

    // we filled another outgoing batch, let's return it.
    return outgoing.setAllCount(outputBatchFinishedCount);
  }

  @Override
  public State getState() {
    return state;
  }

  /**
   * Method is invoked when we have a straight aggregate (no group by expression) and our input is empty.
   * In this case we construct an outgoing batch with record count as 1. For the nullable vectors we don't set anything
   * as we want the output to be NULL. For the required vectors (only for count()) we set the value to be zero since
   * we don't zero out our buffers initially while allocating them.
   */
  private void constructSpecialBatch() {
    outgoing.allocateNew();
    List<NamedExpression> exprs = config.getAggrExprs();
    if(outgoing.getNumberOfColumns() != exprs.size()){
      throw new IllegalStateException();
    }
    int exprIndex = 0;
    for (final VectorWrapper<?> vw: outgoing) {
      final ValueVector vv = vw.getValueVector();
      if (!exprs.isEmpty() && isCount(exprs.get(exprIndex))) {
        ((BigIntVector) vv).setSafe(0, 0);
      }
      vv.setValueCount(SPECIAL_BATCH_COUNT);
      exprIndex++;
    }
    outgoing.setRecordCount(SPECIAL_BATCH_COUNT);
  }

  private boolean isCount(NamedExpression expr) {
    LogicalExpression logicalExpression = expr.getExpr();
    if (logicalExpression instanceof FunctionCall) {
      return ((FunctionCall) logicalExpression).getName().equalsIgnoreCase("count");
    }
    return false;
  }

  private void codeGenAtBat(ClassGenerator<StreamingAggregator> cg, LogicalExpression[] keyExprs, LogicalExpression[] valueExprs) {

    //  public abstract void setupAtBat(@Named("atBat") VectorAccessible atBat, @Named("output") VectorAccessible output);
    //  public abstract boolean compareAtBat(@Named("index1") int index1, @Named("index2") int index2);

    final GeneratorMapping compareAtBat = GeneratorMapping.create("setupAtBat", "compareAtBat", null, null);
    final MappingSet index1 = new MappingSet("index1", "--na--", "atBat", "--na--", compareAtBat, compareAtBat);
    final MappingSet index2 = new MappingSet("index2", "--na--", "atBat", "--na--", compareAtBat, compareAtBat);

    cg.setMappingSet(index1);
    for (final LogicalExpression expr : keyExprs) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(index1);
      final HoldingContainer first = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      cg.setMappingSet(index2);
      final HoldingContainer second = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);

      final LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparatorNullsHigh(first, second, context.getClassProducer());
      final HoldingContainer out = cg.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);


    //  public abstract void addAtBatRecord(@Named("atBatIndex") int index);
    // public abstract void outputAggregation(@Named("outIndex") int outputIndex);

    final GeneratorMapping evalInside = GeneratorMapping.create("setupAtBat", "addAtBatRecord", null, null);
    final GeneratorMapping evalOutside = GeneratorMapping.create("setupAtBat", "outputAggregation", "resetValues", "close");
    final MappingSet eval = new MappingSet("atBatIndex", "outputIndex", "atBat", "output", evalInside, evalOutside, evalInside);

    cg.setMappingSet(eval);
    for (final LogicalExpression ex : valueExprs) {
      cg.addExpr(ex);
    }
  }

  private void codeGenOnDeck(ClassGenerator<StreamingAggregator> cg, LogicalExpression[] keyExprs, LogicalExpression[] valueExprs) {

    // Methods associated with onDeck data.

    // public abstract void setupOnDeck(@Named("onDeck") VectorAccessible onDeck, @Named("atBat") VectorAccessible atBat, @Named("output") VectorAccessible output);
    // public abstract boolean compareOnDeckAndAtBat(@Named("onDeckIndex") int onDeckIndex, @Named("atBatIndex") int atBatIndex);

    final GeneratorMapping compareOnDeck = GeneratorMapping.create("setupOnDeck", "compareOnDeckAndAtBat", null, null);
    final MappingSet index1 = new MappingSet("onDeckIndex", "--na--", "onDeck", "--na--", compareOnDeck, compareOnDeck);
    final MappingSet index2 = new MappingSet("atBatIndex", "--na--", "atBat", "--na--", compareOnDeck, compareOnDeck);

    cg.setMappingSet(index1);
    for (final LogicalExpression expr : keyExprs) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(index1);
      final HoldingContainer first = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);
      cg.setMappingSet(index2);
      final HoldingContainer second = cg.addExpr(expr, ClassGenerator.BlockCreateMode.MERGE);

      final LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparatorNullsHigh(first, second, context.getClassProducer());
      final HoldingContainer out = cg.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);

  }

  private void codeGenOutputKeys(ClassGenerator<StreamingAggregator> cg, TypedFieldId[] keyOutputIds, LogicalExpression[] keyExprs) {

    //  public abstract void outputAtBatKeys(@Named("atBatIndex") int inIndex, @Named("outputIndex") int outputIndex);

    final GeneratorMapping mapping = GeneratorMapping.create("setupAtBat", "outputAtBatKeys", null, null);
    final MappingSet recordKeys = new MappingSet("atBatIndex", "outputIndex", "atBat", "output", mapping, mapping);

    cg.setMappingSet(recordKeys);
    for (int i = 0; i < keyExprs.length; i++) {
      cg.addExpr(new ValueVectorWriteExpression(keyOutputIds[i], keyExprs[i], true));
    }
  }

  private void codeGenGetVectorIndex(ClassGenerator<StreamingAggregator> g) {
    switch (onDeckInput.getSchema().getSelectionVectorMode()) {
    case FOUR_BYTE: {
      throw new UnsupportedOperationException();
    }
    case NONE: {
      g.getBlock("getVectorIndex")._return(JExpr.direct("recordIndex"));;
      return;
    }
    case TWO_BYTE: {
      throw new UnsupportedOperationException();
      // this does't work since the we would need to move between ondeck and atbat
      // JVar var = g.declareClassField("sv2_", g.getModel()._ref(SelectionVector2.class));
      // g.getBlock("setup").assign(var, JExpr.direct("incoming").invoke("getSelectionVector2"));
      // g.getBlock("getVectorIndex")._return(var.invoke("getIndex").arg(JExpr.direct("recordIndex")));;
      // return;
    }

    default:
      throw new IllegalStateException();
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close((AutoCloseable) atBatInput, outgoing);
  }

  public static class Creator implements SingleInputOperator.Creator<StreamingAggregate>{

    @Override
    public SingleInputOperator create(OperatorContext context, StreamingAggregate operator) throws ExecutionSetupException {
      return new StreamingAggOperator(context, operator);
    }

  }
}
