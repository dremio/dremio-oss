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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.LogicalExpression;
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
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.FieldVector;

public class AccumulatorBuilderNoSpill {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AccumulatorBuilderNoSpill.class);

  private static final LongExpression COUNT_ONE_LITERAL_LONG = new LongExpression(1);
  private static final IntExpression COUNT_ONE_LITERAL_INT = new IntExpression(1);

  private AccumulatorBuilderNoSpill() {}

  private static boolean isCountLiteral(LogicalExpression expr) {
    return COUNT_ONE_LITERAL_INT.equals(expr) || COUNT_ONE_LITERAL_LONG.equals(expr);
  }

  /**
   * Create a new set of accumulators. For each accumulator, add an output vector to outgoing. Wraps
   * all accumulators in a single parent.
   *
   * @param aggregateExpressions set of expressions to accumulate.
   * @param incoming Incoming vectors
   * @param outgoing Outgoing vectors
   * @param decimalV2Enabled
   * @param bufferManager
   * @param reduceNdvHeap For ndv only, do not preallocate HllSketch objects to save heap
   * @return A Nested accumulator that holds individual sub-accumulators.
   */
  public static AccumulatorNoSpill getAccumulator(
      BufferAllocator allocator,
      ClassProducer producer,
      List<NamedExpression> aggregateExpressions,
      VectorAccessible incoming,
      VectorContainer outgoing,
      boolean decimalV2Enabled,
      BufferManager bufferManager,
      boolean reduceNdvHeap) {
    final AccumulatorNoSpill[] accums = new AccumulatorNoSpill[aggregateExpressions.size()];

    for (int i = 0; i < aggregateExpressions.size(); i++) {
      final NamedExpression ne = aggregateExpressions.get(i);
      final LogicalExpression expr = producer.materialize(ne.getExpr(), incoming);

      if (expr == null || !(expr instanceof FunctionHolderExpr)) {
        throw unsup("Accumulation expression is not a function: " + expr.toString());
      }

      FunctionHolderExpr func = (FunctionHolderExpr) expr;
      ImmutableList<LogicalExpression> exprs = ImmutableList.copyOf(expr);

      if (func.getName().equals("count")
          && (exprs.isEmpty() || (exprs.size() == 1 && isCountLiteral(exprs.get(0))))) {
        final FieldVector outputVector =
            TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), allocator);
        outgoing.add(outputVector);
        accums[i] = new CountOneAccumulatorNoSpill(outputVector);
        continue;
      }

      if ((exprs.size() != 1 || !(exprs.get(0) instanceof ValueVectorReadExpression))) {
        throw unsup(
            "Accumulation expression has an expected number of type of arguments: "
                + exprs.toString());
      }

      final ValueVectorReadExpression vvread = (ValueVectorReadExpression) exprs.get(0);
      final FieldVector incomingValues =
          incoming
              .getValueAccessorById(FieldVector.class, vvread.getFieldId().getFieldIds())
              .getValueVector();
      final FieldVector outputVector =
          TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), allocator);
      outgoing.add(outputVector);
      accums[i] =
          getAccumulator(
              func.getName(),
              incomingValues,
              outputVector,
              decimalV2Enabled,
              bufferManager,
              reduceNdvHeap);
    }

    return new NestedAccumulatorNoSpill(accums);
  }

  private static AccumulatorNoSpill getAccumulator(
      String name,
      FieldVector incomingValues,
      FieldVector outputVector,
      boolean decimalV2Enabled,
      BufferManager bufferManager,
      boolean reduceNdvHeap) {
    final MinorType type = CompleteType.fromField(incomingValues.getField()).toMinorType();
    // String _complete if present.
    String functionName = name.split("_")[0];
    switch (functionName) {
      case "count":
        {
          return new CountColumnAccumulatorNoSpill(incomingValues, outputVector);
        }

      case "sum":
        {
          switch (type) {
            case INT:
              return new SumAccumulatorsNoSpill.IntSumAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT4:
              return new SumAccumulatorsNoSpill.FloatSumAccumulatorNoSpill(
                  incomingValues, outputVector);
            case BIGINT:
              return new SumAccumulatorsNoSpill.BigIntSumAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT8:
              return new SumAccumulatorsNoSpill.DoubleSumAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DECIMAL:
              if (decimalV2Enabled) {
                return new SumAccumulatorsNoSpill.DecimalSumAccumulatorNoSpillV2(
                    incomingValues, outputVector);
              } else {
                return new SumAccumulatorsNoSpill.DecimalSumAccumulatorNoSpill(
                    incomingValues, outputVector);
              }
          }
          break;
        }

      case "min":
        {
          switch (type) {
            case INT:
              return new MinAccumulatorsNoSpill.IntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT4:
              return new MinAccumulatorsNoSpill.FloatMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case BIGINT:
              return new MinAccumulatorsNoSpill.BigIntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT8:
              return new MinAccumulatorsNoSpill.DoubleMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DECIMAL:
              if (decimalV2Enabled) {
                return new MinAccumulatorsNoSpill.DecimalMinAccumulatorNoSpillV2(
                    incomingValues, outputVector);
              } else {
                return new MinAccumulatorsNoSpill.DecimalMinAccumulatorNoSpill(
                    incomingValues, outputVector);
              }
            case BIT:
              return new MinAccumulatorsNoSpill.BitMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DATE:
              // dates represented as NullableDateMilli, which are 8-byte values. For purposes of
              // min(), comparisions
              // of NullableDateMilli are the same as comparisons on the underlying long values
              return new MinAccumulatorsNoSpill.BigIntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case TIME:
              // time represented as NullableTimeMilli, which are 4-byte values. For purposes of
              // min(), comparisons
              // of NullableTimeMilli are the same as comparisons on the underlying int values
              return new MinAccumulatorsNoSpill.IntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case TIMESTAMP:
              // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes
              // of min(), comparisons
              // of NullableTimeStampMilli are the same as comparisons on the underlying long values
              return new MinAccumulatorsNoSpill.BigIntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case INTERVALDAY:
              return new MinAccumulatorsNoSpill.IntervalDayMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case INTERVALYEAR:
              // interval-year represented as a NullableIntervalYear, which is a 4-byte value
              // containing the number of months
              // in the interval. Comparisons are the same as comparisons on the underlying int
              // values
              return new MinAccumulatorsNoSpill.IntMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case VARCHAR:
              return new MinAccumulatorsNoSpill.VarLenMinAccumulatorNoSpill(
                  incomingValues, outputVector);
            case VARBINARY:
              return new MinAccumulatorsNoSpill.VarLenMinAccumulatorNoSpill(
                  incomingValues, outputVector);
          }
          break;
        }

      case "max":
        {
          switch (type) {
            case INT:
              return new MaxAccumulatorsNoSpill.IntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT4:
              return new MaxAccumulatorsNoSpill.FloatMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case BIGINT:
              return new MaxAccumulatorsNoSpill.BigIntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT8:
              return new MaxAccumulatorsNoSpill.DoubleMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DECIMAL:
              if (decimalV2Enabled) {
                return new MaxAccumulatorsNoSpill.DecimalMaxAccumulatorNoSpillV2(
                    incomingValues, outputVector);
              } else {
                return new MaxAccumulatorsNoSpill.DecimalMaxAccumulatorNoSpill(
                    incomingValues, outputVector);
              }
            case BIT:
              return new MaxAccumulatorsNoSpill.BitMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DATE:
              // dates represented as NullableDateMilli, which are 8-byte values. For purposes of
              // max(), comparisions
              // of NullableDateMilli are the same as comparisons on the underlying long values
              return new MaxAccumulatorsNoSpill.BigIntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case TIME:
              // time represented as NullableTimeMilli, which are 4-byte values. For purposes of
              // max(), comparisons
              // of NullableTimeMilli are the same as comparisons on the underlying int values
              return new MaxAccumulatorsNoSpill.IntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case TIMESTAMP:
              // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes
              // of max(), comparisons
              // of NullableTimeStampMilli are the same as comparisons on the underlying long values
              return new MaxAccumulatorsNoSpill.BigIntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case INTERVALDAY:
              return new MaxAccumulatorsNoSpill.IntervalDayMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case INTERVALYEAR:
              // interval-year represented as a NullableIntervalYear, which is a 4-byte value
              // containing the number of months
              // in the interval. Comparisons are the same as comparisons on the underlying int
              // values
              return new MaxAccumulatorsNoSpill.IntMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case VARCHAR:
              return new MaxAccumulatorsNoSpill.VarLenMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
            case VARBINARY:
              return new MaxAccumulatorsNoSpill.VarLenMaxAccumulatorNoSpill(
                  incomingValues, outputVector);
          }
          break;
        }

      case "$sum0":
        {
          switch (type) {
            case INT:
              return new SumZeroAccumulatorsNoSpill.IntSumZeroAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT4:
              return new SumZeroAccumulatorsNoSpill.FloatSumZeroAccumulatorNoSpill(
                  incomingValues, outputVector);
            case BIGINT:
              return new SumZeroAccumulatorsNoSpill.BigIntSumZeroAccumulatorNoSpill(
                  incomingValues, outputVector);
            case FLOAT8:
              return new SumZeroAccumulatorsNoSpill.DoubleSumZeroAccumulatorNoSpill(
                  incomingValues, outputVector);
            case DECIMAL:
              if (decimalV2Enabled) {
                return new SumZeroAccumulatorsNoSpill.DecimalSumZeroAccumulatorNoSpillV2(
                    incomingValues, outputVector);
              } else {
                return new SumZeroAccumulatorsNoSpill.DecimalSumZeroAccumulatorNoSpill(
                    incomingValues, outputVector);
              }
          }
          break;
        }

      case "hll":
        {
          switch (name) {
            case "hll_merge":
              return new NdvAccumulatorsNoSpill.NdvUnionAccumulatorsNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
          }

          switch (type) {
            case INT:
              return new NdvAccumulatorsNoSpill.IntNdvAccumulatorsNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case FLOAT4:
              return new NdvAccumulatorsNoSpill.FloatNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case BIGINT:
              return new NdvAccumulatorsNoSpill.BigIntNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case FLOAT8:
              return new NdvAccumulatorsNoSpill.DoubleNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case DECIMAL:
              if (decimalV2Enabled) {
                return new NdvAccumulatorsNoSpill.DecimalNdvAccumulatorNoSpillV2(
                    incomingValues, outputVector, bufferManager, reduceNdvHeap);
              } else {
                return new NdvAccumulatorsNoSpill.DecimalNdvAccumulatorNoSpill(
                    incomingValues, outputVector, bufferManager, reduceNdvHeap);
              }
            case BIT:
              return new NdvAccumulatorsNoSpill.BitNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case DATE:
              // dates represented as NullableDateMilli, which are 8-byte values. For purposes of
              // min(), comparisions
              // of NullableDateMilli are the same as comparisons on the underlying long values
              return new NdvAccumulatorsNoSpill.BigIntNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case TIME:
              // time represented as NullableTimeMilli, which are 4-byte values. For purposes of
              // min(), comparisons
              // of NullableTimeMilli are the same as comparisons on the underlying int values
              return new NdvAccumulatorsNoSpill.IntNdvAccumulatorsNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case TIMESTAMP:
              // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes
              // of min(), comparisons
              // of NullableTimeStampMilli are the same as comparisons on the underlying long values
              return new NdvAccumulatorsNoSpill.BigIntNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case INTERVALDAY:
              return new NdvAccumulatorsNoSpill.IntervalDayNdvAccumulatorNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case INTERVALYEAR:
              // interval-year represented as a NullableIntervalYear, which is a 4-byte value
              // containing the number of months
              // in the interval. Comparisons are the same as comparisons on the underlying int
              // values
              return new NdvAccumulatorsNoSpill.IntNdvAccumulatorsNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
            case VARCHAR:
            case VARBINARY:
              return new NdvAccumulatorsNoSpill.VarLenNdvAccumulatorsNoSpill(
                  incomingValues, outputVector, bufferManager, reduceNdvHeap);
          }
          break;
        }
    }

    throw UserException.unsupportedError()
        .message(
            "Unable to handle function %s for input field %s.",
            name, Describer.describe(incomingValues.getField()))
        .build(logger);
  }

  private static UserException unsup(String msg) {
    throw UserException.unsupportedError()
        .message("Aggregate not supported. %s", msg)
        .build(logger);
  }
}
