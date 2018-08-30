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

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.google.common.collect.ImmutableList;

public class AccumulatorBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccumulatorBuilder.class);

  private static final LongExpression COUNT_ONE_LITERAL_LONG = new LongExpression(1);
  private static final IntExpression COUNT_ONE_LITERAL_INT = new IntExpression(1);
  private AccumulatorBuilder(){}

  private static boolean isCountLiteral(LogicalExpression expr) {
    return COUNT_ONE_LITERAL_INT.equals(expr) || COUNT_ONE_LITERAL_LONG.equals(expr);
  }

  /**
   * Create a new set of accumulators. For each accumulator, add an output vector to outgoing. Wraps all accumulators in a single parent.
   * @param aggregateExpressions set of expressions to accumulate.
   * @param incoming Incoming vectors
   * @param outgoing Outgoing vectors
   * @return A Nested accumulator that holds individual sub-accumulators.
   */
  public static Accumulator getAccumulator(BufferAllocator allocator, ClassProducer producer, List<NamedExpression> aggregateExpressions, VectorAccessible incoming, VectorContainer outgoing){
    final Accumulator[] accums = new Accumulator[aggregateExpressions.size()];

    for (int i = 0; i < aggregateExpressions.size(); i++) {
      final NamedExpression ne = aggregateExpressions.get(i);
      final LogicalExpression expr = producer.materialize(ne.getExpr(), incoming);

      if(expr == null || !(expr instanceof FunctionHolderExpr) ){
        throw unsup("Accumulation expression is not a function: " + expr.toString());
      }

      FunctionHolderExpr func = (FunctionHolderExpr) expr;
      ImmutableList<LogicalExpression> exprs = ImmutableList.copyOf(expr);


      if (func.getName().equals("count") && (exprs.isEmpty() || (exprs.size() == 1 && isCountLiteral(exprs.get(0)) ) ) ) {
        final FieldVector outputVector = TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), allocator);
        outgoing.add(outputVector);
        accums[i] = new CountOneAccumulator(outputVector);
        continue;
      }

      if ((exprs.size() != 1 ||  !(exprs.get(0) instanceof ValueVectorReadExpression))) {
        throw unsup("Accumulation expression has an expected number of type of arguments: " + exprs.toString());
      }

      final ValueVectorReadExpression vvread = (ValueVectorReadExpression) exprs.get(0);
      final FieldVector incomingValues = incoming.getValueAccessorById(FieldVector.class, vvread.getFieldId().getFieldIds()).getValueVector();
      final FieldVector outputVector = TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), allocator);
      outgoing.add(outputVector);
      accums[i] = getAccumulator(func.getName(), incomingValues, outputVector);
    }

    return new NestedAccumulator(accums);
  }

  private static Accumulator getAccumulator(String name, FieldVector incomingValues, FieldVector outputVector) {
    final MinorType type = CompleteType.fromField(incomingValues.getField()).toMinorType();
    switch(name){
    case "count": {
      return new CountColumnAccumulator(incomingValues, outputVector);
    }

    case "sum": {
      switch(type){
      case INT:
        return new SumAccumulators.IntSumAccumulator(incomingValues, outputVector);
      case FLOAT4:
        return new SumAccumulators.FloatSumAccumulator(incomingValues, outputVector);
      case BIGINT:
        return new SumAccumulators.BigIntSumAccumulator(incomingValues, outputVector);
      case FLOAT8:
        return new SumAccumulators.DoubleSumAccumulator(incomingValues, outputVector);
      case DECIMAL:
        return new SumAccumulators.DecimalSumAccumulator(incomingValues, outputVector);
      }
      break;
    }

    case "min": {
      switch(type){
      case INT:
        return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector);
      case FLOAT4:
        return new MinAccumulators.FloatMinAccumulator(incomingValues, outputVector);
      case BIGINT:
        return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector);
      case FLOAT8:
        return new MinAccumulators.DoubleMinAccumulator(incomingValues, outputVector);
      case DECIMAL:
        return new MinAccumulators.DecimalMinAccumulator(incomingValues, outputVector);
      case BIT:
        return new MinAccumulators.BitMinAccumulator(incomingValues, outputVector);
      case DATE:
        // dates represented as NullableDateMilli, which are 8-byte values. For purposes of min(), comparisions
        // of NullableDateMilli are the same as comparisons on the underlying long values
        return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector);
      case TIME:
        // time represented as NullableTimeMilli, which are 4-byte values. For purposes of min(), comparisons
        // of NullableTimeMilli are the same as comparisons on the underlying int values
        return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector);
      case TIMESTAMP:
        // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes of min(), comparisons
        // of NullableTimeStampMilli are the same as comparisons on the underlying long values
        return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector);
      case INTERVALDAY:
        return new MinAccumulators.IntervalDayMinAccumulator(incomingValues, outputVector);
      case INTERVALYEAR:
        // interval-year represented as a NullableIntervalYear, which is a 4-byte value containing the number of months
        // in the interval. Comparisons are the same as comparisons on the underlying int values
        return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector);
      }
      break;
    }

    case "max": {
      switch(type){
      case INT:
        return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector);
      case FLOAT4:
        return new MaxAccumulators.FloatMaxAccumulator(incomingValues, outputVector);
      case BIGINT:
        return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector);
      case FLOAT8:
        return new MaxAccumulators.DoubleMaxAccumulator(incomingValues, outputVector);
      case DECIMAL:
        return new MaxAccumulators.DecimalMaxAccumulator(incomingValues, outputVector);
      case BIT:
        return new MaxAccumulators.BitMaxAccumulator(incomingValues, outputVector);
      case DATE:
        // dates represented as NullableDateMilli, which are 8-byte values. For purposes of max(), comparisions
        // of NullableDateMilli are the same as comparisons on the underlying long values
        return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector);
      case TIME:
        // time represented as NullableTimeMilli, which are 4-byte values. For purposes of max(), comparisons
        // of NullableTimeMilli are the same as comparisons on the underlying int values
        return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector);
      case TIMESTAMP:
        // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes of max(), comparisons
        // of NullableTimeStampMilli are the same as comparisons on the underlying long values
        return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector);
      case INTERVALDAY:
        return new MaxAccumulators.IntervalDayMaxAccumulator(incomingValues, outputVector);
      case INTERVALYEAR:
        // interval-year represented as a NullableIntervalYear, which is a 4-byte value containing the number of months
        // in the interval. Comparisons are the same as comparisons on the underlying int values
        return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector);
      }
      break;
    }

    case "$sum0": {
      switch(type){
      case INT:
        return new SumZeroAccumulators.IntSumZeroAccumulator(incomingValues, outputVector);
      case FLOAT4:
        return new SumZeroAccumulators.FloatSumZeroAccumulator(incomingValues, outputVector);
      case BIGINT:
        return new SumZeroAccumulators.BigIntSumZeroAccumulator(incomingValues, outputVector);
      case FLOAT8:
        return new SumZeroAccumulators.DoubleSumZeroAccumulator(incomingValues, outputVector);
      case DECIMAL:
        return new SumZeroAccumulators.DecimalSumZeroAccumulator(incomingValues, outputVector);
      }
      break;
    }

    }

    throw UserException.unsupportedError().message("Unable to handle function %s for input field %s.", name, Describer.describe(incomingValues.getField())).build(logger);
  }


  private static UserException unsup(String msg){
    throw UserException.unsupportedError().message("Aggregate not supported. %s", msg).build(logger);
  }
}
