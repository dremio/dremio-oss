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
package com.dremio.exec.planner.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedListVarcharVector;
import org.apache.arrow.vector.MutableVarcharVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.Numbers;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorBuilder;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorBuilder.MaterializedAggExpressionsResult;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotBuilder.PivotInfo;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.impl.hash.HashConfigWrapper;
import com.koloboke.collect.impl.hash.LHashCapacities;

/**
 * Memory estimate for the pre-allocation (upper bound) required for Vectorized HashAgg (with
 * spilling) operator.
 */
public class HashAggMemoryEstimator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggMemoryEstimator.class);

  private final int numPartitions;
  private final int hashTableBatchSize;
  private final int maxVariableBlockLength;
  private final OptionManager optionManager;
  private final PivotInfo pivotInfo;
  private final AccumulatorBuilder.MaterializedAggExpressionsResult materializedAggExpressions;

  // these are useful for debugging.
  private int memOrdinals;
  private int memHashTable;
  private int memAccumulators;
  private int memAuxStructures;
  private int memLoadingPartition;
  private int memTotal;

  public int memControlBlockSinglePartition;
  private int memFixedBlockSinglePartition;
  private int memVariableBlockSinglePartition;

  private HashAggMemoryEstimator(
    int numPartitions,
    int hashTableBatchSize,
    int maxVariableBlockLength,
    MaterializedAggExpressionsResult materializedAggExpressions,
    PivotInfo pivotInfo,
    OptionManager optionManager) {

    this.numPartitions = numPartitions;
    this.hashTableBatchSize = hashTableBatchSize;
    this.maxVariableBlockLength = maxVariableBlockLength;
    this.materializedAggExpressions = materializedAggExpressions;
    this.pivotInfo = pivotInfo;
    this.optionManager = optionManager;
  }

  public int getMemTotal() {
    return memTotal;
  }

  public int getHashTableBatchSize() {
    return hashTableBatchSize;
  }

  public int getMemHashTable() {
    return memHashTable;
  }

  public int getMemAccumulators() {
    return memAccumulators;
  }

  public int getMemOrdinals() {
    return memOrdinals;
  }

  public int getMemLoadingPartition() {
    return memLoadingPartition;
  }

  public int getMemAuxStructures() {
    return memAuxStructures;
  }

  // Used by the plannner to estimate memory required.
  public static HashAggMemoryEstimator create(
    final List<NamedExpression> groupByExpressions,
    final List<NamedExpression> aggregateExpressions,
    final BatchSchema schema,
    final BatchSchema childSchema,
    final FunctionLookupContext functionLookupContext,
    final OptionManager options) {

    try (final BufferAllocator allocator = new RootAllocator();
         final VectorContainer incoming = new VectorContainer(allocator)) {
      incoming.addSchema(childSchema);

      final int hashTableBatchSize = computeHashTableSize(options, schema);

      // construct pivot info using the group-by exprs.
      final List<LogicalExpression> materializedGroupByExprs = materializeExprs(groupByExpressions,
        childSchema,
        functionLookupContext);
      final PivotInfo pivotInfo = getPivotInfo(materializedGroupByExprs, incoming);

      // construct accumulator types using the agg exprs.
      final List<LogicalExpression> materializedAggExprs = materializeExprs(aggregateExpressions,
        childSchema,
        functionLookupContext);
      final MaterializedAggExpressionsResult accumulatorTypes =
        AccumulatorBuilder.getAccumulatorTypesFromMaterializedExpressions(
          aggregateExpressions, materializedAggExprs, incoming);

      return create(pivotInfo, accumulatorTypes, hashTableBatchSize, options);
    }
  }

  // Used by the executor for verification.
  public static HashAggMemoryEstimator create(
    final PivotInfo pivotInfo,
    final MaterializedAggExpressionsResult materializedAggExpressions,
    final int hashTableBatchSize,
    final OptionManager options) {

    final int variableWidthKeySize =
      (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int maxVariableBlockLength =
      LBlockHashTable.computeVariableBlockMaxLength(
        hashTableBatchSize, pivotInfo.getNumVarColumns(), variableWidthKeySize);

    final int numPartitions =
      (int) options.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS);

    HashAggMemoryEstimator estimator = new HashAggMemoryEstimator(
      numPartitions,
      hashTableBatchSize,
      maxVariableBlockLength,
      materializedAggExpressions,
      pivotInfo,
      options);

    estimator.computePreAllocation();
    return estimator;
  }

  private static List<LogicalExpression> materializeExprs(
    List<NamedExpression> namedExprs,
    BatchSchema childSchema,
    FunctionLookupContext functionLookupContext) {

    return namedExprs
      .stream()
      .map(ne -> ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), childSchema, functionLookupContext))
      .collect(Collectors.toList());
  }

  private void computePreAllocation() {
    /* data structures for hash-table inside all partitions */
    memHashTable = computeForHashTable();

    /* data structures for accumulators inside all partitions */
    memAccumulators = computeForAccumulators();

    /* data structures that are reused for consuming data, computing hash, storing hash values accumulating, sorting etc */
    memOrdinals = computeForOrdinals();
    memAuxStructures = computeForAuxStructures();

    /* data structures that are reused to read a single spilled batch from disk */
    memLoadingPartition = computeForLoadingPartition();

    memTotal = memHashTable + memAccumulators + memOrdinals + memAuxStructures + memLoadingPartition;
  }

  private int computeForHashTable() {
    return computeForHashTableSinglePartition() * numPartitions;
  }

  private int computeForHashTableSinglePartition() {
    computeForControlBlockSinglePartition();
    computeFixedBlockSinglePartition();
    computeVariableBlockSinglePartition();

    return memControlBlockSinglePartition + memFixedBlockSinglePartition +
      memVariableBlockSinglePartition;
  }

  private void computeForControlBlockSinglePartition() {
    final int minHashTableSize = (int)optionManager.getOption(ExecConstants.MIN_HASH_TABLE_SIZE);
    int minHashTableSizePerPartition = (int)Math.ceil((minHashTableSize * 1.0) / numPartitions);
    minHashTableSizePerPartition = LHashCapacities.capacity(new HashConfigWrapper(HashConfig.getDefault()),
      minHashTableSizePerPartition, false);
    memControlBlockSinglePartition = LBlockHashTable.computePreAllocationForControlBlock(
      minHashTableSizePerPartition, hashTableBatchSize);
  }

  private void computeFixedBlockSinglePartition() {
    memFixedBlockSinglePartition = FixedBlockVector.computeSizeForSingleBlock(hashTableBatchSize, pivotInfo.getBlockWidth());
  }

  private void computeVariableBlockSinglePartition() {
    memVariableBlockSinglePartition = VariableBlockVector.computeSizeForSingleBlock(maxVariableBlockLength);
  }

  private int computeForAccumulators() {
    return computeAccumulatorSizeForSinglePartition() * numPartitions;
  }

  private int computeForAuxStructures() {
    return memFixedBlockSinglePartition + memVariableBlockSinglePartition;
  }

  private int computeForOrdinals() {
    return Numbers.nextPowerOfTwo(numPartitions *
      VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH * hashTableBatchSize);
  }

  private int computeForLoadingPartition() {
    return memFixedBlockSinglePartition + memVariableBlockSinglePartition +
      computeAccumulatorSizeForSinglePartition();
  }

  private static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  // compute direct memory required for by the accumulators for one batch.
  private int computeAccumulatorSizeForSinglePartition() {
    int validitySize = 0;
    int dataSize = 0;
    int index = 0;

    for (Field field : materializedAggExpressions.getOutputVectorFields()) {
      int accumType = materializedAggExpressions.getAccumulatorTypes()[index++];
      /* Irrespecive of the minorType, the memory for HLL and LISTAGG is fixed size. */
      if (accumType == AccumulatorBuilder.AccumulatorType.HLL_MERGE.ordinal() ||
        accumType == AccumulatorBuilder.AccumulatorType.HLL.ordinal()) {
        dataSize += (int)optionManager.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES);
        /* Add space for temporary buffer as well */
        dataSize += (int)optionManager.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES) / numPartitions;
        validitySize += 2 * BitVectorHelper.getValidityBufferSize(hashTableBatchSize);
        continue;
      } else if (accumType == AccumulatorBuilder.AccumulatorType.LISTAGG.ordinal() ||
        accumType == AccumulatorBuilder.AccumulatorType.LOCAL_LISTAGG.ordinal() ||
        accumType == AccumulatorBuilder.AccumulatorType.LISTAGG_MERGE.ordinal()) {
        dataSize += FixedListVarcharVector.FIXED_LISTVECTOR_SIZE_TOTAL;
        /* Add space for temporary buffer as well */
        dataSize += FixedListVarcharVector.FIXED_LISTVECTOR_SIZE_TOTAL / numPartitions;
        validitySize += 2 * BitVectorHelper.getValidityBufferSize(hashTableBatchSize);
        continue;
      } else if (accumType == AccumulatorBuilder.AccumulatorType.ARRAY_AGG.ordinal() ||
        accumType == AccumulatorBuilder.AccumulatorType.PHASE1_ARRAY_AGG.ordinal() ||
        accumType == AccumulatorBuilder.AccumulatorType.PHASE2_ARRAY_AGG.ordinal()) {
        dataSize += (int)optionManager.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES);
        /* Add space for temporary buffer as well */
        dataSize += (int)optionManager.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES) / numPartitions;
        validitySize += 2 * BitVectorHelper.getValidityBufferSize(hashTableBatchSize);
        continue;
      }

      TypeProtos.MinorType minorType = CompleteType.fromField(field).toMinorType();
      switch (minorType) {
        case BIT:
          validitySize += getValidityBufferSizeFromCount(hashTableBatchSize);
          dataSize += getValidityBufferSizeFromCount(hashTableBatchSize);
          break;

        /* 8 byte output accumulator */
        case BIGINT:
        case DATE:
        case TIMESTAMP:
        case FLOAT8:
        case INTERVALDAY:
          validitySize += getValidityBufferSizeFromCount(hashTableBatchSize);
          dataSize += (8 * hashTableBatchSize);
          break;

        /* 4 byte output accumulator */
        case FLOAT4:
        case INTERVALYEAR:
        case TIME:
        case INT:
          validitySize += getValidityBufferSizeFromCount(hashTableBatchSize);
          dataSize += (4 * hashTableBatchSize);
          break;

        /* 16 byte output accumulator */
        case DECIMAL:
          validitySize += getValidityBufferSizeFromCount(hashTableBatchSize);
          dataSize += (16 * hashTableBatchSize);
          break;

        case VARCHAR:
        case VARBINARY:
          final int variableWidthKeySize =
            (int) optionManager.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
          /* Calculate the temporary buffer */
          validitySize += getValidityBufferSizeFromCount(hashTableBatchSize);
          /* Offset buffer size */
          int tempVecDataSize = hashTableBatchSize * 4;
          tempVecDataSize += variableWidthKeySize * hashTableBatchSize;
          tempVecDataSize = Numbers.nextPowerOfTwo(tempVecDataSize);
          /* One temporary vector for each partition */
          dataSize += tempVecDataSize / numPartitions;

          /* Calculate the accumulator buffer. */
          validitySize += MutableVarcharVector.getValidityBufferSizeFromCount(hashTableBatchSize);
          /*
           * AccumulatorSet most likely use an unshared buffer for varchar accumulator, which always
           * rounded to nextPowerOfTwo. If we have too many varchar vectors and nextPowerOfTwo is not
           * calculated, the extra memory add's up and we may have mismatch in memory estimator vs
           * actual usage.
           */
          dataSize += Numbers.nextPowerOfTwo(
            MutableVarcharVector.getValidityBufferSizeFromCount(hashTableBatchSize) +
              MutableVarcharVector.getDataBufferSizeFromCount(hashTableBatchSize,
              hashTableBatchSize * variableWidthKeySize));
          break;
      }
    }

    return Numbers.nextPowerOfTwo(validitySize + dataSize);
  }


  private static PivotInfo getPivotInfo(
    final List<LogicalExpression> materializedGroupByExprs,
    final VectorAccessible incoming) {

    final List<FieldVector> inputVectors = new ArrayList<>();
    for (final LogicalExpression expr : materializedGroupByExprs) {
      final ValueVectorReadExpression readExpr = (ValueVectorReadExpression) expr;
      final FieldVector inputVector = incoming.getValueAccessorById(FieldVector.class,
        readExpr.getFieldId().getFieldIds()).getValueVector();
      inputVectors.add(inputVector);
    }
    return PivotBuilder.getBlockInfo(inputVectors);
  }

  private static int computeHashTableSize(final OptionManager options, final BatchSchema schema) {
    /*
     * Estimate the outgoing record size. This is proportional to the sum of the accumulator and
     * pivot sizes.
     */
    final int listSizeEstimate = (int)options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int estimatedVariableWidthKeySize = (int)options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize = schema.estimateRecordSize(listSizeEstimate, estimatedVariableWidthKeySize);

    /*
     * Compute the max hash table batch size, based on the estimated record size.
     */
    final int maxOutgoingBatchSize = (int)options.getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES);
    int maxOutgoingRecordCount = Numbers.nextPowerOfTwo(maxOutgoingBatchSize / estimatedRecordSize);

    final int configuredTargetRecordCount = (int)options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
    final int minTargetRecordCount = (int)options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    int batchSize = Math.min(configuredTargetRecordCount, maxOutgoingRecordCount);
    batchSize = Math.max(batchSize, minTargetRecordCount);
    return PhysicalPlanCreator.optimizeBatchSizeForAllocs(batchSize);
  }
}
