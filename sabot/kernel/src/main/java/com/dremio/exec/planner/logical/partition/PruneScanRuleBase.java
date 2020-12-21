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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitSets;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.Types;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.catalog.StoragePluginId;
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
import com.dremio.exec.planner.logical.SampleRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SplitsKey;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.MetadataUtils;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetFilterCondition.FilterProperties;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.github.slugify.Slugify;
import com.google.common.base.Function;
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

  private static final Slugify SLUGIFY = new Slugify();
  private static final long MIN_TO_LOG_INFO_MS = 10000;

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
    private final ImmutableList<PartitionChunkMetadata> finalSplits;
    private final int totalRecords;

    private EvaluationPruningResult(List<PartitionChunkMetadata> finalSplits, int allRecords) {
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
    public PruneScanRuleFilterOnProject(StoragePluginId pluginId, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginId.getType(), RelOptHelper.some(FilterRel.class, RelOptHelper.some(ProjectRel.class, RelOptHelper.any(clazz))),
        pluginId.getType().value() + "NewPruneScanRule:Filter_On_Project."
          + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString(), optimizerContext);
    }

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
    public PruneScanRuleFilterOnScan(StoragePluginId pluginId, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginId.getType(), RelOptHelper.some(FilterRel.class, RelOptHelper.any(clazz)),
        pluginId.getType().value() + "NewPruneScanRule:Filter_On_Scan."
          + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString(), optimizerContext);
    }

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

  // new prune scan rule for filter on SampleRel, fires when filter above a sample rel with pruneable scan.
  public static class PruneScanRuleFilterOnSampleScan<T extends ScanRelBase & PruneableScan> extends PruneScanRuleBase<T> {
    public PruneScanRuleFilterOnSampleScan(StoragePluginId pluginId, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginId.getType(), RelOptHelper.some(FilterRel.class, RelOptHelper.some(SampleRel.class, RelOptHelper.any(clazz))),
        pluginId.getType().value() + "NewPruneScanRule:Filter_On_SampleRel_Scan."
          + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString(), optimizerContext);
    }

    public PruneScanRuleFilterOnSampleScan(SourceType pluginType, Class<T> clazz, OptimizerRulesContext optimizerContext) {
      super(pluginType, RelOptHelper.some(FilterRel.class, RelOptHelper.some(SampleRel.class, RelOptHelper.any(clazz))),
        pluginType.value() + "NewPruneScanRule:Filter_On_SampleRel_Scan", optimizerContext);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanRelBase scan = call.rel(2);
      double splitRatio;
      if (scan.getPluginId().getType().equals(pluginType)) {
        try {
          splitRatio = scan.getTableMetadata().getSplitRatio();
        } catch (NamespaceException e) {
          logger.warn("Unable to calculate split.", e);
          return false;
        }
        if(splitRatio == 1.0d){
          final List<String> partitionColumns = scan.getTableMetadata().getReadDefinition().getPartitionColumnsList();
          return partitionColumns != null && !partitionColumns.isEmpty();
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final T scanRel = call.rel(2);
      doOnMatch(call, filterRel, null, scanRel, true);
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

    final FindSimpleFilters.StateHolder holder = pruneCondition.accept(new FindSimpleFilters(builder, true, false));
    TableMetadata datasetPointer = scanRel.getTableMetadata();

    // index based matching does not support decimals, the values(literal/index) are not scaled
    // at this point. TODO : DX-17748
    Pointer<Boolean> hasDecimalCols = new Pointer<>(false);
    holder.getConditions().stream().forEach(condition -> condition.getOperands().stream().forEach(
            (operand -> {
              if(operand.getKind().equals(SqlKind.INPUT_REF) && operand.getType().getSqlTypeName
                      ().equals(SqlTypeName.DECIMAL)){
                hasDecimalCols.value = true;
              }
            })));


    if(!holder.hasConditions() || hasDecimalCols.value) {
      datasetOutput.value = datasetPointer;
      outputCondition.value = pruneCondition;
      return false;
    }

    final ImmutableList<RexCall> conditions = holder.getConditions();
    final RelDataType incomingRowType = scanRel.getRowType();
    final List<FilterProperties> filters = FluentIterable
      .from(conditions)
      .transform(new Function<RexCall, ParquetFilterCondition.FilterProperties>() {
        @Override
        public ParquetFilterCondition.FilterProperties apply(RexCall input) {
          return new FilterProperties(input, incomingRowType);
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
      Pointer<List<PartitionChunkMetadata>> finalSplits){
    final int batchSize = PARTITION_BATCH_SIZE;

    final ImmutableList.Builder<PartitionChunkMetadata> selectedSplits = ImmutableList.builder();
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
    Iterator<PartitionChunkMetadata> splitIter = tableMetadata.getSplits();
    LogicalExpression materializedExpr = null;

    do {
      miscTimer.start();

      List<PartitionChunkMetadata> splitsInBatch = new ArrayList<>();
      for(int splitsLoaded = 0; splitsLoaded < batchSize && splitIter.hasNext(); ++splitsLoaded) {
        final PartitionChunkMetadata split = splitIter.next();
        splitsInBatch.add(split);
      }

      logger.debug("Elapsed time to get list of splits for the current batch: {} ms within batchIndex: {}", miscTimer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
      miscTimer.reset();

      try(final BufferAllocator allocator = optimizerContext.getAllocator().newChildAllocator("prune-scan-rule", 0, Long.MAX_VALUE);
      final BitVector output = new BitVector("", allocator);
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
        for(PartitionChunkMetadata split: splitsInBatch) {
          // load partition values
          for (PartitionValue partitionValue : split.getPartitionValues()) {
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
            final PartitionChunkMetadata split = splitsInBatch.get(i);
            selectedSplits.add(split);
          }
          recordCount++;
        }

        logger.debug("Within batch {}: total records: {}, qualified records: {}", batchIndex, recordCount, qualifiedCount);
        batchIndex++;

      }
    } while (splitIter.hasNext());

    List<PartitionChunkMetadata> finalNewSplits = selectedSplits.build();

    // Store results in local cache
    evalutationPruningCache.put(cacheKey, new EvaluationPruningResult(finalNewSplits, recordCount));

    finalSplits.value = finalNewSplits;
    return qualifiedCount < recordCount;
  }

  public void doOnMatch(RelOptRuleCall call, Filter filterRel, Project projectRel, T scanRel) {
    doOnMatch(call, filterRel, projectRel, scanRel, false);
  }

  public void doOnMatch(RelOptRuleCall call, Filter filterRel, Project projectRel, T scanRel, Boolean hasSampleRel) {
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
        logger.debug("No partition columns are projected from the scan.");
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
          logger.info("No conditions were found eligible for partition pruning.");
        }
        return;
      }

      final Pointer<TableMetadata> dataset = new Pointer<>();
      final Pointer<RexNode> outputCondition = new Pointer<>();

      // do index-based pruning
      Stopwatch stopwatch = Stopwatch.createStarted();
      boolean sargPruned = doSargPruning(filterRel, pruneCondition, scanRel, fieldMap,
                dataset, outputCondition);
      stopwatch.stop();
      logger.debug("Partition pruning using search index took {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      final boolean evalPruned;
      final List<PartitionChunkMetadata> finalNewSplits;
      if(!outputCondition.value.isAlwaysTrue()){
        // do interpreter-based evaluation
        Pointer<List<PartitionChunkMetadata>> prunedOutput = new Pointer<>();
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

      if (hasSampleRel) {
        inputRel = new SampleRel(inputRel.getCluster(), inputRel.getTraitSet(), inputRel);
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
      totalPruningTime.stop();
      final long elapsed = totalPruningTime.elapsed(TimeUnit.MILLISECONDS);
      longRun = elapsed > MIN_TO_LOG_INFO_MS;
      if(longRun) {
        logger.info("Total pruning elapsed time: {} ms", elapsed);
      } else {
        logger.debug("Total pruning elapsed time: {} ms", elapsed);
      }
    }
  }

  private void writePartitionValue(ValueVector vv, int index, PartitionValue pv, MajorType majorType, BufferAllocator allocator) {
    switch (majorType.getMinorType()) {
      case INT: {
        IntVector intVector = (IntVector) vv;
        if(pv.hasIntValue()){
          intVector.setSafe(index, pv.getIntValue());
        }
        return;
      }
      case SMALLINT: {
        SmallIntVector smallIntVector = (SmallIntVector) vv;
        if(pv.hasIntValue()){
          smallIntVector.setSafe(index, (short) pv.getIntValue());
        }
        return;
      }
      case TINYINT: {
        TinyIntVector tinyIntVector = (TinyIntVector) vv;
        if(pv.hasIntValue()){
          tinyIntVector.setSafe(index, (byte) pv.getIntValue());
        }
        return;
      }
      case UINT1: {
        UInt1Vector intVector = (UInt1Vector) vv;
        if(pv.hasIntValue()){
          intVector.setSafe(index, pv.getIntValue());
        }
        return;
      }
      case UINT2: {
        UInt2Vector intVector = (UInt2Vector) vv;
        if(pv.hasIntValue()){
          intVector.setSafe(index, (char) pv.getIntValue());
        }
        return;
      }
      case UINT4: {
        UInt4Vector intVector = (UInt4Vector) vv;
        if(pv.hasIntValue()){
          intVector.setSafe(index, pv.getIntValue());
        }
        return;
      }
      case BIGINT: {
        BigIntVector bigIntVector = (BigIntVector) vv;
        if(pv.hasLongValue()){
          bigIntVector.setSafe(index, pv.getLongValue());
        }
        return;
      }
      case FLOAT4: {
        Float4Vector float4Vector = (Float4Vector) vv;
        if(pv.hasFloatValue()){
          float4Vector.setSafe(index, pv.getFloatValue());
        }
        return;
      }
      case FLOAT8: {
        Float8Vector float8Vector = (Float8Vector) vv;
        if(pv.hasDoubleValue()){
          float8Vector.setSafe(index, pv.getDoubleValue());
        }
        return;
      }
      case VARBINARY: {
        VarBinaryVector varBinaryVector = (VarBinaryVector) vv;
        if(pv.hasBinaryValue()){
          byte[] bytes = pv.getBinaryValue().toByteArray();
          varBinaryVector.setSafe(index, bytes, 0, bytes.length);
        }
        return;
      }
      case DATE: {
        DateMilliVector dateVector = (DateMilliVector) vv;
        if(pv.hasLongValue()){
          dateVector.setSafe(index, pv.getLongValue());
        }
        return;
      }
      case TIME: {
        TimeMilliVector timeVector = (TimeMilliVector) vv;
        if(pv.hasIntValue()){
          timeVector.setSafe(index, pv.getIntValue());
        }
        return;
      }
      case TIMESTAMP: {
        TimeStampMilliVector timeStampVector = (TimeStampMilliVector) vv;
        if(pv.hasLongValue()){
          timeStampVector.setSafe(index, pv.getLongValue());
        }
        return;
      }
      case BIT: {
        BitVector bitVect = (BitVector) vv;
        if(pv.hasBitValue()){
          bitVect.setSafe(index, pv.getBitValue() ? 1 : 0);
        }
        return;
      }
      case DECIMAL: {
        DecimalVector decimal = (DecimalVector) vv;
        if(pv.hasBinaryValue()){
          byte[] bytes = pv.getBinaryValue().toByteArray();
          /* set the bytes in LE format in the buffer of decimal vector, we will swap
           * the bytes while writing into the vector.
           */
          decimal.setBigEndianSafe(index, bytes);
        }
        return;
      }
      case VARCHAR: {
        VarCharVector varCharVector = (VarCharVector) vv;
        if(pv.hasStringValue()){
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
