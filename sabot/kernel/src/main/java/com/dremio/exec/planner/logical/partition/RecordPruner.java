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
package com.dremio.exec.planner.logical.partition;

import static com.dremio.exec.expr.ExpressionTreeMaterializer.materializeAndCheckErrors;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.fn.interpreter.InterpreterEvaluator;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.TableMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * Class providing split pruning functionality for filter conditions in the query
 */
public abstract class RecordPruner implements AutoCloseable {
  protected static final Logger logger = LoggerFactory.getLogger(RecordPruner.class);
  protected static final int PARTITION_BATCH_SIZE = Character.MAX_VALUE;

  protected final BitVector outputVector; // vector to hold the result of expression evaluation
  protected final VectorContainer inputContainer; // container for the input vectors
  protected final Stopwatch timer = Stopwatch.createUnstarted();
  private final BufferAllocator allocator;
  private final OptimizerRulesContext optimizerContext;
  protected ValueVector[] vectors;
  protected Map<Integer, MajorType> partitionColIdToTypeMap;
  protected Map<String, MajorType> partitionColNameToTypeMap;

  public RecordPruner(OptimizerRulesContext optimizerContext) {
    this.optimizerContext = optimizerContext;
    this.allocator = optimizerContext.getAllocator().newChildAllocator("prune-scan-rule", 0, Long.MAX_VALUE);
    this.outputVector = new BitVector("", allocator);
    this.inputContainer = new VectorContainer();
  }

  /**
   * Prune splits based on filters in the input filter condition
   * @return count of the surviving records
   */
  public abstract long prune(
    Map<Integer, String> inUseColIdToNameMap,
    Map<String, Integer> partitionColToIdMap,
    Function<RexNode, List<Integer>> usedIndexes,
    List<SchemaPath> projectedColumns,
    TableMetadata tableMetadata,
    RexNode pruneCondition,
    BatchSchema batchSchema,
    RelDataType rowType,
    RelOptCluster cluster);

  @Override
  public void close() {
    AutoCloseables.close(RuntimeException.class, inputContainer, outputVector, allocator);
  }

  protected void setup(Map<Integer, String> inUseColIdToNameMap, BatchSchema batchSchema) {
    partitionColIdToTypeMap = Maps.newHashMap();
    partitionColNameToTypeMap = Maps.newHashMap();
    for (int partitionColIndex : inUseColIdToNameMap.keySet()) {
      final SchemaPath column = SchemaPath.getSimplePath(inUseColIdToNameMap.get(partitionColIndex));
      final CompleteType completeType = batchSchema.getFieldId(column).getFinalType();
      final MajorType type;
      if (completeType.getPrecision() != null && completeType.getScale() != null) {
        type = Types.withScaleAndPrecision(completeType.toMinorType(), TypeProtos.DataMode.OPTIONAL, completeType.getScale(), completeType.getPrecision());
      } else {
        type = Types.optional(completeType.toMinorType());
      }
      partitionColIdToTypeMap.put(partitionColIndex, type);
      partitionColNameToTypeMap.put(column.getAsUnescapedPath(), type);
    }
  }

  protected void setupVectors(Map<Integer, String> inUseColIdToNameMap, Map<String, Integer> partitionColToIdMap, int batchSize) {
    vectors = new ValueVector[partitionColToIdMap.size()];
    outputVector.allocateNew(batchSize);

    for (int i : inUseColIdToNameMap.keySet()) {
      SchemaPath column = SchemaPath.getSimplePath(inUseColIdToNameMap.get(i));
      MajorType type = partitionColIdToTypeMap.get(i);
      Field field = MajorTypeHelper.getFieldForNameAndMajorType(column.getAsUnescapedPath(), type);
      ValueVector v = TypeHelper.getNewVector(field, allocator);
      AllocationHelper.allocateNew(v, batchSize);
      vectors[i] = v;
      inputContainer.add(v);
    }
  }

  protected LogicalExpression materializePruneExpr(RexNode pruneCondition, RelDataType rowType, RelOptCluster cluster) {
    logger.debug("Attempting to prune {}", pruneCondition);
    inputContainer.buildSchema();
    ParseContext parseContext = new ParseContext(optimizerContext.getPlannerSettings());

    timer.start();
    LogicalExpression pruneExpr = RexToExpr.toExpr(parseContext, rowType, cluster.getRexBuilder(), pruneCondition);
    LogicalExpression materializedExpr = materializeAndCheckErrors(pruneExpr, inputContainer.getSchema(), optimizerContext.getFunctionRegistry());
    logger.debug("Elapsed time to create and materialize expression: {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.reset();

    if (materializedExpr == null) {
      throw new IllegalStateException("Unable to materialize prune expression: " + pruneExpr);
    }
    return materializedExpr;
  }

  protected void evaluateExpr(LogicalExpression materializedExpr, int batchSize) {
    timer.start();
    InterpreterEvaluator.evaluate(batchSize, optimizerContext, inputContainer, outputVector, materializedExpr);
    logger.debug("Elapsed time in interpreter evaluation: {}ms with # of partitions: {}",
      timer.elapsed(TimeUnit.MILLISECONDS), batchSize);
    timer.reset();
    adjustVectorMarkers(batchSize);
  }

  /**
   * Set the record count in vectors to the same as the batch size.
   * Calling this method saves on the cost of multiple allocations or vector resets.
   */
  private void adjustVectorMarkers(int batchSize) {
    if (batchSize != PARTITION_BATCH_SIZE) {
      inputContainer.setRecordCount(batchSize);
      outputVector.setValueCount(batchSize);
    }
  }
}