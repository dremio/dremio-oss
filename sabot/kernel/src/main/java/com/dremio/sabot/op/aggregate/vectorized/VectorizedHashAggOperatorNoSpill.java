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
package com.dremio.sabot.op.aggregate.vectorized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;
import com.dremio.sabot.op.common.ht2.Pivots;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats.Metric;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.koloboke.collect.hash.HashConfig;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class VectorizedHashAggOperatorNoSpill implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashAggOperator.class);

  private static final int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;
  private final OperatorContext context;
  private final VectorContainer outgoing;
  private final HashAggregate popConfig;

  private final Stopwatch pivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private final Stopwatch accumulateWatch = Stopwatch.createUnstarted();
  private final Stopwatch unpivotWatch = Stopwatch.createUnstarted();

  private ImmutableList<FieldVector> vectorsToValidate;
  private LBlockHashTableNoSpill table;
  private PivotDef pivot;
  private AccumulatorSet accumulator;
  private int outputBatchCount;
  private VectorAccessible incoming;
  private State state = State.NEEDS_SETUP;

  public VectorizedHashAggOperatorNoSpill(HashAggregate popConfig, OperatorContext context) throws ExecutionSetupException {
    this.context = context;
    this.outgoing = new VectorContainer(context.getAllocator());
    this.popConfig = popConfig;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = accessible;
    this.pivot = createPivot();
    final AccumulatorBuilder.MaterializedAggExpressionsResult materializeAggExpressionsResult =
      AccumulatorBuilder.getAccumulatorTypesFromExpressions(context.getClassProducer(), popConfig.getAggrExprs(), incoming);
    this.accumulator = AccumulatorBuilder.getAccumulator(context.getAllocator(), context.getAllocator(),
                                                         materializeAggExpressionsResult,
                                                         outgoing,
                                                         true,
                                                         LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH,
                                                         0,
                                                         0);
    this.outgoing.buildSchema();
    this.table = new LBlockHashTableNoSpill(HashConfig.getDefault(), pivot, context.getAllocator(),
                                     (int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), INITIAL_VAR_FIELD_AVERAGE_SIZE, accumulator);

    state = State.CAN_CONSUME;
    return outgoing;
  }

  private PivotDef createPivot(){
    final List<NamedExpression> groupByExpressions = popConfig.getGroupByExprs();
    final ImmutableList.Builder<FieldVector> validationVectors = ImmutableList.builder();

    final List<FieldVectorPair> fvps = new ArrayList<>();
    //final FieldVector[] readVectors = new FieldVector[]
    for (int i = 0; i < groupByExpressions.size(); i++) {
      final NamedExpression ne = groupByExpressions.get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), incoming);

      if(expr == null){
        throw unsup("Unable to resolve group by expression: " + ne.getExpr().toString());
      }
      if( !(expr instanceof ValueVectorReadExpression) ){
        throw unsup("Group by expression is non-trivial: " + ne.getExpr().toString());
      }

      final ValueVectorReadExpression vvread = (ValueVectorReadExpression) expr;
      final FieldVector inputVector = incoming.getValueAccessorById(FieldVector.class, vvread.getFieldId().getFieldIds()).getValueVector();
      if(inputVector instanceof VarCharVector || inputVector instanceof VarBinaryVector){
        validationVectors.add(inputVector);
      }
      final FieldVector outputVector = TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), context.getAllocator());
      outgoing.add(outputVector);
      fvps.add(new FieldVectorPair(inputVector, outputVector));
    }

    this.vectorsToValidate = validationVectors.build();
    return PivotBuilder.getBlockDefinition(fvps);
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for(FieldVector v : vectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }

    try(FixedBlockVector fbv = new FixedBlockVector(context.getAllocator(), pivot.getBlockWidth());
        VariableBlockVector var = new VariableBlockVector(context.getAllocator(), pivot.getVariableCount());
    ){
      // first we pivot.
      pivotWatch.start();
      Pivots.pivot(pivot, records, fbv, var);
      pivotWatch.stop();
      final long keyFixedAddr = fbv.getMemoryAddress();
      final long keyVarAddr = var.getMemoryAddress();

      try(ArrowBuf offsets = context.getAllocator().buffer(records * 4)){
        long offsetAddr = offsets.memoryAddress();

        // then we add all values to table.
        insertWatch.start();
        for(int i = 0; i < records; i++, offsetAddr += 4){
          PlatformDependent.putInt(offsetAddr, table.add(keyFixedAddr, keyVarAddr, i));
        }
        insertWatch.stop();

        // then we do accumulators.
        accumulateWatch.start();
        accumulator.accumulateNoSpill(offsets.memoryAddress(), records);
        accumulateWatch.stop();
      }

    }

    updateStats();
  }

  private void updateStats(){
    final OperatorStats stats = context.getStats();

    if(table != null){
      stats.setLongStat(Metric.NUM_ENTRIES, table.size());
      stats.setLongStat(Metric.NUM_BUCKETS,  table.capacity());
      stats.setLongStat(Metric.NUM_RESIZING, table.getRehashCount());
      stats.setLongStat(Metric.RESIZING_TIME, table.getRehashTime(TimeUnit.NANOSECONDS));
    }

    stats.setLongStat(Metric.VECTORIZED, 1);
    stats.setLongStat(Metric.PIVOT_TIME, pivotWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.INSERT_TIME, insertWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.ACCUMULATE_TIME, accumulateWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.REVERSE_TIME, 0);
    stats.setLongStat(Metric.UNPIVOT_TIME, unpivotWatch.elapsed(TimeUnit.NANOSECONDS));
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    if(outputBatchCount ==  table.blocks()){
      state = State.DONE;
      return 0;
    }

    final int recordsInBatch = Math.min(LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH, table.size() - (outputBatchCount * LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH));

    unpivotWatch.start();
    table.unpivot(outputBatchCount, recordsInBatch);
    unpivotWatch.stop();

    accumulator.output(outputBatchCount);
    outputBatchCount++;

    updateStats();

    return outgoing.setAllCount(recordsInBatch);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);

    if(table.size() == 0){
      state = State.DONE;
    }else{
      state = State.CAN_PRODUCE;
    }
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
    updateStats();
    AutoCloseables.close(table, accumulator, outgoing);
  }

  private static UserException unsup(String msg){
    throw UserException.unsupportedError().message("Aggregate not supported. %s", msg).build(logger);
  }

}
