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
package com.dremio.sabot.op.aggregate.vectorized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
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

public class AccumulatorBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccumulatorBuilder.class);

  private static final LongExpression COUNT_ONE_LITERAL_LONG = new LongExpression(1);
  private static final IntExpression COUNT_ONE_LITERAL_INT = new IntExpression(1);
  private AccumulatorBuilder(){}

  private static boolean isCountLiteral(LogicalExpression expr) {
    return COUNT_ONE_LITERAL_INT.equals(expr) || COUNT_ONE_LITERAL_LONG.equals(expr);
  }

  /**
   *
   * @param namedExpressions aggregate expressions from POP
   * @param aggregateExpressions materialized aggregate expressions
   * @param incoming incoming vector container
   * @return Materialized expressions along with input vectors and field info on output vectors
   * for accumulators
   *
   * We do this exactly once to avoid incurring the cost of materializing the expressions and parsing
   * them repeatedly as we setup accumulators for each partition at the very beginning when setup is done
   * for {@link VectorizedHashAggOperator}
   */
  public static MaterializedAggExpressionsResult getAccumulatorTypesFromMaterializedExpressions(
    List<NamedExpression> namedExpressions,
    List<LogicalExpression> aggregateExpressions,
    VectorAccessible incoming) {

    final byte[] accumulatorTypes = new byte[aggregateExpressions.size()];
    final List<Field> outputVectorFields = new ArrayList<>(aggregateExpressions.size());
    final List<FieldVector> inputVectors = new ArrayList<>(aggregateExpressions.size());

    for (int i = 0; i < aggregateExpressions.size(); i++) {
      final NamedExpression ne = namedExpressions.get(i);
      final LogicalExpression expr = aggregateExpressions.get(i);
      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      final FieldVector inputVector;

      outputVectorFields.add(outputField);

      if(expr == null || !(expr instanceof FunctionHolderExpr) ){
        throw unsup("Accumulation expression is not a function: " + expr.toString());
      }

      FunctionHolderExpr func = (FunctionHolderExpr) expr;
      ImmutableList<LogicalExpression> exprs = ImmutableList.copyOf(expr);

      /* COUNT(1) */
      if (func.getName().equals("count") && (exprs.isEmpty() || (exprs.size() == 1 && isCountLiteral(exprs.get(0))))) {
        accumulatorTypes[i] = (byte)AccumulatorType.COUNT1.ordinal();
        /* count1 doesn't need an input accumulator vector */
        inputVectors.add(null);
        continue;
      }

      /* SUM, MIN, MAX, $SUM0, COUNT, HLL, HLL_MERGE */
      final ValueVectorReadExpression vvread = (ValueVectorReadExpression) exprs.get(0);
      inputVector = incoming.getValueAccessorById(FieldVector.class, vvread.getFieldId().getFieldIds()).getValueVector();
      accumulatorTypes[i] = getAccumulatorTypeFromName(func.getName());

      if ((exprs.size() != 1 ||  !(exprs.get(0) instanceof ValueVectorReadExpression))) {
        throw unsup("Accumulation expression has an unexpected number of type of arguments: " + exprs.toString());
      }

      inputVectors.add(inputVector);
    }

    return new MaterializedAggExpressionsResult(accumulatorTypes, inputVectors, outputVectorFields);
  }

  public static MaterializedAggExpressionsResult getAccumulatorTypesFromExpressions(
    ClassProducer producer,
    List<NamedExpression> namedExpressions,
    VectorAccessible incoming) throws Exception {
    List<LogicalExpression> materializedExprs = namedExpressions
      .stream()
      .map(ne -> producer.materialize(ne.getExpr(), incoming))
      .collect(Collectors.toList());

    return getAccumulatorTypesFromMaterializedExpressions(namedExpressions, materializedExprs, incoming);
  }

  /**
   * Create a set of accumulators. For each accumulator, add an output vector to outgoing. Wraps all accumulators in a single parent.
   * @param computationVectorAllocator allocator used for accumulator vectors that stores computed
   *                                   values in each batch
   * @param outputVectorAllocator allocator used for accumulator vectors in outgoing container
   * @param materializedAggExpressions holder for materialized aggregate expressions and info on input/output vectors
   * @param outgoing Outgoing vector container
   * @param maxValuesPerBatch maximum records that can be stored in a hashtable block/batch
   *
   * @param decimalV2Enabled
   * @param varLenAccumulatorCapacity
   * @param tempAccumulatorHolder needed for varlen accumulation vectors
   * @return A Nested accumulator that holds individual sub-accumulators.
   *
   * With partitioning in VectorizedHashAgg operator, accumulators are handled on a
   * per-partition basis. However, we want to add the target accumulation vector
   * that stores the computed values to the outgoing vector container EXACTLY ONCE
   * since that is used for projection and each partition's output accumulator vector
   * simply does a transfer to the vector in outgoing container when operator stats
   * outputting data.
   */
  public static AccumulatorSet getAccumulator(final BufferAllocator computationVectorAllocator,
                                              final BufferAllocator outputVectorAllocator,
                                              MaterializedAggExpressionsResult materializedAggExpressions,
                                              VectorContainer outgoing,
                                              final int maxValuesPerBatch,
                                              final long jointAllocationMin,
                                              final long jointAllocationLimit,
                                              boolean decimalV2Enabled,
                                              int varLenAccumulatorCapacity,
                                              int maxVarWidthVecUsagePercent,
                                              BaseVariableWidthVector[] tempAccumulatorHolder) {
    final byte[] accumulatorTypes = materializedAggExpressions.accumulatorTypes;
    final List<FieldVector> inputVectors = materializedAggExpressions.inputVectors;
    final List<Field> outputVectorFields = materializedAggExpressions.outputVectorFields;

    final Accumulator[] accums = new Accumulator[accumulatorTypes.length];

    for (int i = 0; i < accumulatorTypes.length; i++) {
      final FieldVector inputVector = inputVectors.get(i);
      final FieldVector outputVector = TypeHelper.getNewVector(outputVectorFields.get(i), outputVectorAllocator);
      final FieldVector transferVector = outgoing.addOrGet(outputVector.getField());
      final byte accumulatorType = accumulatorTypes[i];

      accums[i] = getAccumulator(accumulatorType, inputVector, outputVector, transferVector,
        maxValuesPerBatch, computationVectorAllocator, decimalV2Enabled,
        varLenAccumulatorCapacity, maxVarWidthVecUsagePercent, tempAccumulatorHolder[i]);
      if (accums[i] == null) {
        throw new IllegalStateException("ERROR: invalid accumulator state");
      }
    }

    return new AccumulatorSet(jointAllocationMin, jointAllocationLimit,
      computationVectorAllocator, accums);
  }

  private static Accumulator getAccumulator(byte accumulatorType, FieldVector incomingValues,
                                            FieldVector outputVector, FieldVector transferVector,
                                            final int maxValuesPerBatch,
                                            final BufferAllocator computationVectorAllocator,
                                            boolean decimalCompleteEnabled,
                                            int varLenAccumulatorCapacity,
                                            int maxVarWidthVecUsagePercent,
                                            BaseVariableWidthVector tempAccumulatorHolder) {
    if (accumulatorType == AccumulatorType.COUNT1.ordinal()) {
      return new CountOneAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                     computationVectorAllocator);
    }

    final MinorType type = CompleteType.fromField(incomingValues.getField()).toMinorType();
    switch(accumulatorType) {
      case 0 /* SUM */: {
        switch(type){
          case INT:
            return new SumAccumulators.IntSumAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case FLOAT4:
            return new SumAccumulators.FloatSumAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                           computationVectorAllocator);
          case BIGINT:
            return new SumAccumulators.BigIntSumAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case FLOAT8:
            return new SumAccumulators.DoubleSumAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case DECIMAL:
            if (decimalCompleteEnabled) {
              return new SumAccumulators.DecimalSumAccumulatorV2(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            } else {
              return new SumAccumulators.DecimalSumAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            }
        }
        break;
      }

      case 1 /* MIN */: {
        switch(type){
          case INT:
            return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case FLOAT4:
            return new MinAccumulators.FloatMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                           computationVectorAllocator);
          case BIGINT:
            return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case FLOAT8:
            return new MinAccumulators.DoubleMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case DECIMAL:
            if (decimalCompleteEnabled) {
              return new MinAccumulators.DecimalMinAccumulatorV2(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            } else {
              return new MinAccumulators.DecimalMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            }
          case BIT:
            return new MinAccumulators.BitMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case DATE:
            // dates represented as NullableDateMilli, which are 8-byte values. For purposes of min(), comparisions
            // of NullableDateMilli are the same as comparisons on the underlying long values
            return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case TIME:
            // time represented as NullableTimeMilli, which are 4-byte values. For purposes of min(), comparisons
            // of NullableTimeMilli are the same as comparisons on the underlying int values
            return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case TIMESTAMP:
            // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes of min(), comparisons
            // of NullableTimeStampMilli are the same as comparisons on the underlying long values
            return new MinAccumulators.BigIntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case INTERVALDAY:
            return new MinAccumulators.IntervalDayMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                 computationVectorAllocator);
          case INTERVALYEAR:
            // interval-year represented as a NullableIntervalYear, which is a 4-byte value containing the number of months
            // in the interval. Comparisons are the same as comparisons on the underlying int values
            return new MinAccumulators.IntMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);

          case VARCHAR:
          case VARBINARY:
            return new MinAccumulators.VarLenMinAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
              computationVectorAllocator, varLenAccumulatorCapacity, maxVarWidthVecUsagePercent, tempAccumulatorHolder);
        }
        break;
      }

      case 2 /* MAX */: {
        switch(type){
          case INT:
            return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case FLOAT4:
            return new MaxAccumulators.FloatMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                           computationVectorAllocator);
          case BIGINT:
            return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case FLOAT8:
            return new MaxAccumulators.DoubleMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case DECIMAL:
            if (decimalCompleteEnabled) {
              return new MaxAccumulators.DecimalMaxAccumulatorV2(incomingValues,
                outputVector, transferVector, maxValuesPerBatch, computationVectorAllocator);
            } else {
              return new MaxAccumulators.DecimalMaxAccumulator(incomingValues, outputVector,
                transferVector, maxValuesPerBatch, computationVectorAllocator);
            }
          case BIT:
            return new MaxAccumulators.BitMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case DATE:
            // dates represented as NullableDateMilli, which are 8-byte values. For purposes of max(), comparisions
            // of NullableDateMilli are the same as comparisons on the underlying long values
            return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case TIME:
            // time represented as NullableTimeMilli, which are 4-byte values. For purposes of max(), comparisons
            // of NullableTimeMilli are the same as comparisons on the underlying int values
            return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case TIMESTAMP:
            // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes of max(), comparisons
            // of NullableTimeStampMilli are the same as comparisons on the underlying long values
            return new MaxAccumulators.BigIntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                            computationVectorAllocator);
          case INTERVALDAY:
            return new MaxAccumulators.IntervalDayMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                 computationVectorAllocator);
          case INTERVALYEAR:
            // interval-year represented as a NullableIntervalYear, which is a 4-byte value containing the number of months
            // in the interval. Comparisons are the same as comparisons on the underlying int values
            return new MaxAccumulators.IntMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                         computationVectorAllocator);
          case VARCHAR:
          case VARBINARY:
            return new MaxAccumulators.VarLenMaxAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
              computationVectorAllocator, varLenAccumulatorCapacity, maxVarWidthVecUsagePercent, tempAccumulatorHolder);
        }
        break;
      }

      case 3 /* SUM0 */: {
        switch(type){
          case INT:
            return new SumZeroAccumulators.IntSumZeroAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                 computationVectorAllocator);
          case FLOAT4:
            return new SumZeroAccumulators.FloatSumZeroAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                   computationVectorAllocator);
          case BIGINT:
            return new SumZeroAccumulators.BigIntSumZeroAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                    computationVectorAllocator);
          case FLOAT8:
            return new SumZeroAccumulators.DoubleSumZeroAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                                                    computationVectorAllocator);
          case DECIMAL:
            if (decimalCompleteEnabled) {
              return new SumZeroAccumulators.DecimalSumZeroAccumulatorV2(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            } else {
              return new SumZeroAccumulators.DecimalSumZeroAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                computationVectorAllocator);
            }
        }
        break;
      }

      case 4 /* COUNT */: {
        return new CountColumnAccumulator(incomingValues, outputVector, transferVector, maxValuesPerBatch,
                                          computationVectorAllocator);
      }
      case 6 /* HLL */: {
        switch (type) {
          case INT:
            return new NdvAccumulators.IntNdvAccumulators(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case FLOAT4:
            return new NdvAccumulators.FloatNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case BIGINT:
            return new NdvAccumulators.BigIntNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case FLOAT8:
            return new NdvAccumulators.DoubleNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case DECIMAL:
            if (decimalCompleteEnabled) {
              return new NdvAccumulators.DecimalNdvAccumulatorV2(incomingValues, transferVector, maxValuesPerBatch,
                computationVectorAllocator, tempAccumulatorHolder);
            } else {
              return new NdvAccumulators.DecimalNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
                computationVectorAllocator, tempAccumulatorHolder);
            }
          case BIT:
            return new NdvAccumulators.BitNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case DATE:
            // dates represented as NullableDateMilli, which are 8-byte values. For purposes of min(), comparisions
            // of NullableDateMilli are the same as comparisons on the underlying long values
            return new NdvAccumulators.BigIntNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case TIME:
            // time represented as NullableTimeMilli, which are 4-byte values. For purposes of min(), comparisons
            // of NullableTimeMilli are the same as comparisons on the underlying int values
            return new NdvAccumulators.IntNdvAccumulators(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case TIMESTAMP:
            // time represented as NullableTimeStampMilli, which are 8-byte values. For purposes of min(), comparisons
            // of NullableTimeStampMilli are the same as comparisons on the underlying long values
            return new NdvAccumulators.BigIntNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case INTERVALDAY:
            return new NdvAccumulators.IntervalDayNdvAccumulator(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case INTERVALYEAR:
            // interval-year represented as a NullableIntervalYear, which is a 4-byte value containing the number of months
            // in the interval. Comparisons are the same as comparisons on the underlying int values
            return new NdvAccumulators.IntNdvAccumulators(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
          case VARCHAR:
          case VARBINARY:
            return new NdvAccumulators.VarLenNdvAccumulators(incomingValues, transferVector, maxValuesPerBatch,
              computationVectorAllocator, tempAccumulatorHolder);
        }
        break;
      }

      case 7 /* HLL_MERGE */: {
        return new NdvAccumulators.NdvUnionAccumulators(incomingValues, transferVector,
          maxValuesPerBatch, computationVectorAllocator, tempAccumulatorHolder);
      }
    }

    return null;
  }


  private static UserException unsup(String msg){
    throw UserException.unsupportedError().message("Aggregate not supported. %s", msg).build(logger);
  }

  public static class MaterializedAggExpressionsResult {
    final byte[] accumulatorTypes;
    final List<FieldVector> inputVectors;
    final List<Field> outputVectorFields;

    public MaterializedAggExpressionsResult(final byte[] accumulatorTypes,
                                            final List<FieldVector> inputVectors,
                                            final List<Field> outputVectorFields) {
      this.accumulatorTypes = accumulatorTypes;
      this.inputVectors = inputVectors;
      this.outputVectorFields = outputVectorFields;
    }

    public List<Field> getOutputVectorFields() {
      return outputVectorFields;
    }

    public byte[] getAccumulatorTypes() {
      return accumulatorTypes;
    }

    public List<FieldVector> getInputVectors() {
      return inputVectors;
    }
  }

  public enum AccumulatorType {
    SUM,
    MIN,
    MAX,
    SUM0,
    COUNT,
    COUNT1,
    HLL,
    HLL_MERGE,
  }

  private static byte getAccumulatorTypeFromName(String name) {
    // Strip _complete if present.
    String functionName  = name.split("_")[0];
    switch (functionName) {
      case "sum":
        return (byte)AccumulatorType.SUM.ordinal();
      case "min":
        return (byte)AccumulatorType.MIN.ordinal();
      case "max":
        return (byte)AccumulatorType.MAX.ordinal();
      case "$sum0":
        return (byte)AccumulatorType.SUM0.ordinal();
      case "count":
        return (byte)AccumulatorType.COUNT.ordinal();
      case "count1":
        return (byte)AccumulatorType.COUNT1.ordinal();
      case "hll":
        switch (name) {
          case "hll_merge":
            return (byte) AccumulatorType.HLL_MERGE.ordinal();
        }
        return (byte) AccumulatorType.HLL.ordinal();
      default:
        throw UserException.unsupportedError().message("Unable to handle accumulator function %s", name).build(logger);
    }
  }
}
