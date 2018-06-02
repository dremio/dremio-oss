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
package com.dremio.exec.planner.logical.partition;

import static com.dremio.common.util.MajorTypeHelper.getFieldForNameAndMajorType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableSmallIntVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableUInt1Vector;
import org.apache.arrow.vector.NullableUInt2Vector;
import org.apache.arrow.vector.NullableUInt4Vector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BitSets;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.HashVisitor;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.fn.interpreter.InterpreterEvaluator;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SplitsKey;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.MetadataUtils;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.parquet.FilterCondition.FilterProperties;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Prune partitions based on partition values
 */
public abstract class PruneScanRuleBase<T extends ScanRelBase & PruneableScan> extends RelOptRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PruneScanRuleBase.class);

  private static final long MIN_TO_LOG_INFO_MS = 5000;

  public static final int PARTITION_BATCH_SIZE = Character.MAX_VALUE;
  final private OptimizerRulesContext optimizerContext;
  final protected SourceType pluginType;

  /**
   * A logic expression and split holder which can be used as a key for map
   */
  private static class EvaluationPruningKey {
    private final LogicalExpression expression;
    private final SplitsKey splitsKey;

    private EvaluationPruningKey(LogicalExpression expression, SplitsKey splitsKey) {
      this.expression = expression;
      this.splitsKey = splitsKey;
    }


    @Override
    public int hashCode() {
      int hash = expression.accept(new HashVisitor(), null);
      return 31 * hash + Objects.hash(splitsKey);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EvaluationPruningKey)) {
        return false;
      }

      EvaluationPruningKey that = (EvaluationPruningKey) obj;

      return Objects.equals(this.expression.toString(), that.expression.toString())
          && Objects.equals(this.splitsKey, that.splitsKey);
    }
  }

  private static class EvaluationPruningResult {
    private final ImmutableList<DatasetSplit> finalSplits;
    private final int totalRecords;

    private EvaluationPruningResult(List<DatasetSplit> finalSplits, int allRecords) {
      this.finalSplits = ImmutableList.copyOf(finalSplits);
      this.totalRecords = allRecords;
    }
  }

  // Local cache to speed multiple evaluations of the pruning
  private final Map<EvaluationPruningKey, EvaluationPruningResult> evalutationPruningCache = new HashMap<>();

  private PruneScanRuleBase(SourceType pluginType, RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.pluginType = pluginType;
    this.optimizerContext = optimizerContext;
  }

  public static class PruneScanRuleFilterOnProject<T extends ScanRelBase & PruneableScan> extends PruneScanRuleBase<T> {
    public PruneScanRuleFilterOnProject(SourceType pluginType, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginType, RelOptHelper.some(FilterRel.class, RelOptHelper.some(ProjectRel.class, RelOptHelper.any(clazz))),
          pluginType.value() + "NewPruneScanRule:Filter_On_Project", optimizerContext);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanRelBase scan = call.rel(2);
      if (scan.getPluginId().getType().equals(pluginType)) {
        try {
          if(scan.getTableMetadata().getSplitRatio() == 1.0d){
            final List<String> partitionColumns = scan.getTableMetadata().getReadDefinition().getPartitionColumnsList();
            return partitionColumns != null && !partitionColumns.isEmpty();
          }
        } catch (NamespaceException e) {
          logger.warn("Unable to calculate split.", e);
          return false;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final Project projectRel = call.rel(1);
      final T scanRel = call.rel(2);
      doOnMatch(call, filterRel, projectRel, scanRel);
    }
  }

  public static class PruneScanRuleFilterOnScan<T extends ScanRelBase & PruneableScan> extends PruneScanRuleBase<T> {
    public PruneScanRuleFilterOnScan(SourceType pluginType, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginType, RelOptHelper.some(FilterRel.class, RelOptHelper.any(clazz)), pluginType.value() + "NewPruneScanRule:Filter_On_Scan", optimizerContext);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanRelBase scan = call.rel(1);
      if (scan.getPluginId().getType().equals(pluginType)) {
        try {
          if(scan.getTableMetadata().getSplitRatio() == 1.0d){
            final List<String> partitionColumns = scan.getTableMetadata().getReadDefinition().getPartitionColumnsList();
            return partitionColumns != null && !partitionColumns.isEmpty();
          }
        } catch (NamespaceException e) {
          logger.warn("Unable to calculate split.", e);
          return false;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final T scanRel = call.rel(1);
      doOnMatch(call, filterRel, null, scanRel);
    }
  }

  private boolean doSargPruning(
      Filter filterRel,
      RexNode pruneCondition,
      T scanRel,
      final Map<String, Field> fieldMap,
      Pointer<TableMetadata> datasetOutput,
      Pointer<RexNode> outputCondition){
    final RexBuilder builder = filterRel.getCluster().getRexBuilder();

    final FindSimpleFilters.StateHolder holder = pruneCondition.accept(new FindSimpleFilters(builder, true));
    TableMetadata datasetPointer = scanRel.getTableMetadata();

    if(!holder.hasConditions()){
      datasetOutput.value = datasetPointer;
      outputCondition.value = pruneCondition;
      return false;
    }

    final ImmutableList<RexCall> conditions = holder.getConditions();
    final RelDataType rowType = filterRel.getRowType();
    final List<FilterProperties> filters = FluentIterable
      .from(conditions)
      .transform(new Function<RexCall, FilterCondition.FilterProperties>() {
        @Override
        public FilterCondition.FilterProperties apply(RexCall input) {
          return new FilterProperties(input, rowType);
        }
      }).toList();

    final ArrayListMultimap<String, FilterProperties> map = ArrayListMultimap.create();

    // create per field conditions
    for (FilterProperties p : filters) {
      map.put(p.getField(), p);
    }

    final List<SearchQuery> splitFilters = Lists.newArrayList();
    for (Map.Entry<String, FilterProperties> entry: map.entries()) {
      splitFilters.add(MetadataUtils.toSplitsSearchQuery(Collections.singletonList(entry.getValue()), fieldMap.get(entry.getKey())));
    }

    try {
      final TableMetadata prunedDatasetPointer = scanRel.getTableMetadata().prune(SearchQueryUtils.and(splitFilters));
      if (prunedDatasetPointer == datasetPointer) {
        datasetOutput.value = datasetPointer;
        outputCondition.value = pruneCondition;
        return false;
      }

      datasetOutput.value = prunedDatasetPointer;
    } catch (NamespaceException ne) {
      logger.error("Failed to prune partitions using partition values from namespace", ne);
    }

    RelOptCluster cluster = filterRel.getCluster();
    outputCondition.value = holder.hasRemainingExpression() ? holder.getNode() : cluster.getRexBuilder().makeLiteral(true);
    return true;
  }

  private boolean doEvalPruning(
      final Filter filterRel,
      final Map<Integer, String> fieldNameMap,
      final Map<String, Integer> partitionColumnsToIdMap,
      final BitSet partitionColumnBitSet,
      final TableMetadata tableMetadata,
      PlannerSettings settings,
      RexNode pruneCondition,
      T scanRel,
      Pointer<List<DatasetSplit>> finalSplits){
    final int batchSize = PARTITION_BATCH_SIZE;

    final ImmutableList.Builder<DatasetSplit> selectedSplits = ImmutableList.builder();
    final Stopwatch miscTimer = Stopwatch.createUnstarted();



    // Convert the condition into an expression
    logger.debug("Attempting to prune {}", pruneCondition);
    LogicalExpression pruningExpression = RexToExpr.toExpr(new ParseContext(settings), scanRel.getRowType(), scanRel.getCluster().getRexBuilder(), pruneCondition);

    // Check cache if pruning condition was eval'ed against the same batch
    final EvaluationPruningKey cacheKey = new EvaluationPruningKey(pruningExpression, tableMetadata.getSplitsKey());
    final EvaluationPruningResult cacheResult = evalutationPruningCache.get(cacheKey);
    if (cacheResult != null) {
      logger.debug("Result found in cache, skipping evaluation");
      logger.debug("In cache: total records: {}, qualified records: {}", cacheResult.totalRecords, cacheResult.finalSplits.size());

      finalSplits.value = cacheResult.finalSplits;
      return cacheResult.finalSplits.size() < cacheResult.totalRecords;
    }

    int batchIndex = 0;
    int recordCount = 0;
    int qualifiedCount = 0;
    Iterator<DatasetSplit> splitIter = tableMetadata.getSplits();
    LogicalExpression materializedExpr = null;

    do {
      miscTimer.start();

      List<DatasetSplit> splitsInBatch = new ArrayList<>();
      for(int splitsLoaded = 0; splitsLoaded < batchSize && splitIter.hasNext(); ++splitsLoaded) {
        final DatasetSplit split = splitIter.next();
        splitsInBatch.add(split);
      }

      logger.debug("Elapsed time to get list of splits for the current batch: {} ms within batchIndex: {}", miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
      miscTimer.reset();

      try(final BufferAllocator allocator = optimizerContext.getAllocator().newChildAllocator("prune-scan-rule", 0, Long.MAX_VALUE);
      final NullableBitVector output = new NullableBitVector("", allocator);
      final VectorContainer container = new VectorContainer();
          ){
        final ValueVector[] vectors = new ValueVector[partitionColumnsToIdMap.size()];
        // setup vector for each partition TODO (AH) Do we have to setup container each time?
        final Map<Integer, MajorType> partitionColumnIdToTypeMap = Maps.newHashMap();

        for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
          final SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
          final CompleteType completeType = scanRel.getBatchSchema().getFieldId(column).getFinalType();
          final MajorType type;
          if (completeType.getPrecision() != null && completeType.getScale() != null) {
            type = Types.withScaleAndPrecision(completeType.toMinorType(), DataMode.OPTIONAL, completeType.getScale(), completeType.getPrecision());
          } else {
            type = Types.optional(completeType.toMinorType());
          }
          final ValueVector v = TypeHelper.getNewVector(getFieldForNameAndMajorType(column.getAsUnescapedPath(), type), allocator);
          v.allocateNew();
          vectors[partitionColumnIndex] = v;
          container.add(v);
          partitionColumnIdToTypeMap.put(partitionColumnIndex, type);
        }

        // track how long we spend populating partition column vectors
        miscTimer.start();

        int splitsLoaded = 0;
        for(DatasetSplit split: splitsInBatch) {
          if (split.getPartitionValuesList() == null) {
            ++splitsLoaded;
            continue;
          }

          // load partition values
          for (PartitionValue partitionValue : split.getPartitionValuesList()) {
            final int columnIndex = partitionColumnsToIdMap.get(partitionValue.getColumn());
            // TODO (AH) handle invisible columns partitionColumnIdToTypeMap is built from row data type which may or may not have $update column
            if (partitionColumnIdToTypeMap.containsKey(columnIndex)) {
              final ValueVector vv = vectors[columnIndex];
              writePartitionValue(vv, splitsLoaded, partitionValue, partitionColumnIdToTypeMap.get(columnIndex), allocator);
            }
          }

          ++splitsLoaded;
        }
        logger.debug("Elapsed time to populate partitioning column vectors: {} ms within batchIndex: {}", miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
        miscTimer.reset();

        // materialize the expression; only need to do this once
        if (batchIndex == 0) {
          materializedExpr = materializePruneExpr(pruningExpression, settings, scanRel, container);
          if (materializedExpr == null) {
            throw new IllegalStateException("Unable to materialize prune expression: " + pruneCondition.toString());
          }
        }
        output.allocateNew(splitsLoaded);

        // start the timer to evaluate how long we spend in the interpreter evaluation
        miscTimer.start();
        InterpreterEvaluator.evaluate(splitsLoaded, optimizerContext, container, output, materializedExpr);
        logger.debug("Elapsed time in interpreter evaluation: {} ms within batchIndex: {} with # of partitions : {}", miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex, splitsLoaded);
        miscTimer.reset();


        // Inner loop: within each batch iterate over the each partition in this batch
        for (int i = 0; i < splitsLoaded; ++i) {
          if (!output.isNull(i) && output.get(i) == 1) {
            // select this partition
            qualifiedCount++;
            final DatasetSplit split = splitsInBatch.get(i);
            selectedSplits.add(split);
          }
          recordCount++;
        }

        logger.debug("Within batch {}: total records: {}, qualified records: {}", batchIndex, recordCount, qualifiedCount);
        batchIndex++;

      }
    } while (splitIter.hasNext());

    List<DatasetSplit> finalNewSplits = selectedSplits.build();

    // Store results in local cache
    evalutationPruningCache.put(cacheKey, new EvaluationPruningResult(finalNewSplits, recordCount));

    finalSplits.value = finalNewSplits;
    return qualifiedCount < recordCount;
  }

  public void doOnMatch(RelOptRuleCall call, Filter filterRel, Project projectRel, T scanRel) {
    Stopwatch totalPruningTime = Stopwatch.createStarted();
    boolean longRun = true;
    try {
      final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());

      RexNode condition;
      if (projectRel == null) {
        condition = filterRel.getCondition();
      } else {
        // get the filter as if it were below the projection.
        condition = RelOptUtil.pushPastProject(filterRel.getCondition(), projectRel);
      }

      RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, filterRel.getCluster().getRexBuilder());
      condition = condition.accept(visitor);

      final Map<Integer, String> fieldNameMap = Maps.newHashMap();
      final Map<String, Integer> partitionColumnsToIdMap = Maps.newHashMap();
      int index = 0;
      for (String column : scanRel.getTableMetadata().getReadDefinition().getPartitionColumnsList()) {
        partitionColumnsToIdMap.put(column, index++);
      }
      final List<String> fieldNames = scanRel.getRowType().getFieldNames();
      final BitSet columnBitset = new BitSet();
      final BitSet partitionColumnBitSet = new BitSet();
      final Map<String, Field> fieldMap = Maps.newHashMap();

      int relColIndex = 0;
      for (String field : fieldNames) {
        final Integer partitionIndex = partitionColumnsToIdMap.get(field);
        if (partitionIndex != null) {
          fieldNameMap.put(partitionIndex, field);
          partitionColumnBitSet.set(partitionIndex);
          columnBitset.set(relColIndex);
          fieldMap.put(field, scanRel.getBatchSchema().findField(field));
        }
        relColIndex++;
      }

      if (partitionColumnBitSet.isEmpty()) {
        logger.debug("No partition columns are projected from the scan..continue. " +
          "Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
        return;
      }

      // stop watch to track how long we spend in different phases of pruning
      Stopwatch miscTimer = Stopwatch.createUnstarted();

      // track how long we spend building the filter tree
      miscTimer.start();

      // get conditions based on partition column only
      FindPartitionConditions c = new FindPartitionConditions(columnBitset, filterRel.getCluster().getRexBuilder());
      c.analyze(condition);
      RexNode pruneCondition = c.getFinalCondition();

      final long elapsed = miscTimer.elapsed(TimeUnit.MILLISECONDS);
      logger.debug("Total elapsed time to build and analyze filter tree: {} ms",
      elapsed);
      miscTimer.reset();

      longRun = elapsed > MIN_TO_LOG_INFO_MS;
      if (pruneCondition == null) {
        if(longRun) {
          logger.info("No conditions were found eligible for partition pruning." +
            "Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
        }
        return;
      }

      final Pointer<TableMetadata> dataset = new Pointer<>();
      final Pointer<RexNode> outputCondition = new Pointer<>();

      // do index-based pruning
      Stopwatch stopwatch = Stopwatch.createStarted();
      final boolean sargPruned = doSargPruning(filterRel, pruneCondition, scanRel, fieldMap, dataset, outputCondition);
      stopwatch.stop();
      logger.debug("Partition pruning using search index took {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      final boolean evalPruned;
      final List<DatasetSplit> finalNewSplits;
      if(!outputCondition.value.isAlwaysTrue()){
        // do interpreter-based evaluation
        Pointer<List<DatasetSplit>> prunedOutput = new Pointer<>();
        stopwatch.start();
        evalPruned = doEvalPruning(filterRel, fieldNameMap, partitionColumnsToIdMap, partitionColumnBitSet, dataset.value, settings, outputCondition.value, scanRel, prunedOutput);
        stopwatch.stop();
        finalNewSplits = prunedOutput.value;
        logger.debug("Partition pruning using expression evaluation took {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }else {
        finalNewSplits = ImmutableList.copyOf(dataset.value.getSplits());
        evalPruned = false;
      }

      final boolean scanUnchangedAfterPruning = !evalPruned && !sargPruned;

      final Pointer<Boolean> conjunctionRemoved = new Pointer<>(false);
      List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
      List<RexNode> pruneConjuncts = RelOptUtil.conjunctions(pruneCondition);
      final Set<String> prunedConjunctsStr = FluentIterable.from(pruneConjuncts).transform(
        new Function<RexNode, String>() {
          @Nullable
          @Override
          public String apply(@Nullable RexNode input) {
            return input.toString();
          }
        }
      ).toSet();
      conjuncts = FluentIterable.from(conjuncts).filter(
        new Predicate<RexNode>() {
          @Override
          public boolean apply(@Nullable RexNode input) {
            if(prunedConjunctsStr.contains(input.toString())){
              // if we remove any conditions, we should record those here.
              conjunctionRemoved.value = true;
              return false;
            }else {
              return true;
            }
          }
        }
      ).toList();

      final RexNode newCondition = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), conjuncts, false);

      RelNode inputRel;
      if(scanUnchangedAfterPruning){
        if(!conjunctionRemoved.value){
          // no change in scan. no change in filter. no need to create additional nodes.
          return;
        } else {
          // filter changed but scan did not. avoid generating another scan
          inputRel = scanRel;
        }
      }else if(finalNewSplits.isEmpty()) {
        // no splits left, replace with an empty rel.
        inputRel = new EmptyRel(scanRel.getCluster(), scanRel.getTraitSet(), scanRel.getRowType(), scanRel.getProjectedSchema());
      } else {
        // some splits but less than original.
        inputRel = scanRel.applyDatasetPointer(dataset.value.prune(finalNewSplits));
      }

      if (projectRel != null) {
        inputRel = projectRel.copy(projectRel.getTraitSet(), Collections.singletonList(inputRel));
      }

      if (newCondition.isAlwaysTrue()) {
        call.transformTo(inputRel);
      } else {

        // TODO: pruning a filter should remove its conditions. For now, it doesn't so we'll avoid creating a duplicate filter/scan if the scan didn't change.
        if(scanUnchangedAfterPruning){
          return;
        }

        final RelNode newFilter = filterRel.copy(filterRel.getTraitSet(), Collections.singletonList(inputRel));

        call.transformTo(newFilter);
      }

    } catch (Exception e) {
      logger.warn("Exception while using the pruned partitions.", e);
    } finally {
      if(longRun) {
        logger.info("Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
      } else {
        logger.debug("Total pruning elapsed time: {} ms", totalPruningTime.elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  private void writePartitionValue(ValueVector vv, int index, PartitionValue pv, MajorType majorType, BufferAllocator allocator) {
    switch (majorType.getMinorType()) {
      case INT: {
        NullableIntVector intVector = (NullableIntVector) vv;
        if(pv.getIntValue() != null){
          intVector.setSafe(index, pv.getIntValue());
        }
        return;
      }
      case SMALLINT: {
        NullableSmallIntVector smallIntVector = (NullableSmallIntVector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          smallIntVector.setSafe(index, value.shortValue());
        }
        return;
      }
      case TINYINT: {
        NullableTinyIntVector tinyIntVector = (NullableTinyIntVector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          tinyIntVector.setSafe(index, value.byteValue());
        }
        return;
      }
      case UINT1: {
        NullableUInt1Vector intVector = (NullableUInt1Vector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          intVector.setSafe(index, value.byteValue());
        }
        return;
      }
      case UINT2: {
        NullableUInt2Vector intVector = (NullableUInt2Vector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          intVector.setSafe(index, (char) value.shortValue());
        }
        return;
      }
      case UINT4: {
        NullableUInt4Vector intVector = (NullableUInt4Vector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          intVector.setSafe(index, value);
        }
        return;
      }
      case BIGINT: {
        NullableBigIntVector bigIntVector = (NullableBigIntVector) vv;
        Long value = pv.getLongValue();
        if(value != null){
          bigIntVector.setSafe(index, value);
        }
        return;
      }
      case FLOAT4: {
        NullableFloat4Vector float4Vector = (NullableFloat4Vector) vv;
        Float value = pv.getFloatValue();
        if(value != null){
          float4Vector.setSafe(index, value);
        }
        return;
      }
      case FLOAT8: {
        NullableFloat8Vector float8Vector = (NullableFloat8Vector) vv;
        Double value = pv.getDoubleValue();
        if(value != null){
          float8Vector.setSafe(index, value);
        }
        return;
      }
      case VARBINARY: {
        NullableVarBinaryVector varBinaryVector = (NullableVarBinaryVector) vv;
        if(pv.getBinaryValue() != null){
          byte[] bytes = pv.getBinaryValue().toByteArray();
          varBinaryVector.setSafe(index, bytes, 0, bytes.length);
        }
        return;
      }
      case DATE: {
        NullableDateMilliVector dateVector = (NullableDateMilliVector) vv;
        Long value = pv.getLongValue();
        if(value != null){
          dateVector.setSafe(index, value);
        }
        return;
      }
      case TIME: {
        NullableTimeMilliVector timeVector = (NullableTimeMilliVector) vv;
        Integer value = pv.getIntValue();
        if(value != null){
          timeVector.setSafe(index, value);
        }
        return;
      }
      case TIMESTAMP: {
        NullableTimeStampMilliVector timeStampVector = (NullableTimeStampMilliVector) vv;
        Long value = pv.getLongValue();
        if(value != null){
          timeStampVector.setSafe(index, value);
        }
        return;
      }
      case BIT: {
        NullableBitVector bitVect = (NullableBitVector) vv;
        if(pv.getBitValue() != null){
          bitVect.setSafe(index, pv.getBitValue() ? 1 : 0);
        }
        return;
      }
      case DECIMAL: {
        NullableDecimalVector decimal = (NullableDecimalVector) vv;
        if(pv.getBinaryValue() != null){
          byte[] bytes = pv.getBinaryValue().toByteArray();
          Preconditions.checkArgument(bytes.length == 16, "Expected 16 bytes, received %d", bytes.length);
          /* set the bytes in LE format in the buffer of decimal vector, we will swap
           * the bytes while writing into the vector.
           */
          decimal.setBigEndianSafe(index, bytes);
        }
        return;
      }
      case VARCHAR: {
        NullableVarCharVector varCharVector = (NullableVarCharVector) vv;
        if(pv.getStringValue() != null){
          byte[] bytes = pv.getStringValue().getBytes();
          varCharVector.setSafe(index, bytes, 0, bytes.length);
        }
        return;
      }
      default:
        throw new UnsupportedOperationException("Unsupported type: " + majorType);
    }
  }

  private LogicalExpression materializePruneExpr(LogicalExpression pruneCondition,
                                                 PlannerSettings settings,
                                                 RelNode scanRel,
                                                 VectorContainer container) {
    // materialize the expression
    container.buildSchema();
    return ExpressionTreeMaterializer.materializeAndCheckErrors(pruneCondition, container.getSchema(), optimizerContext.getFunctionRegistry());
  }

}
