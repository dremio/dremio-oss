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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.ExecConstants.ENABLE_PARTITION_STATS_USAGE;
import static com.dremio.exec.store.RecordReader.COL_IDS;
import static com.dremio.exec.store.RecordReader.SPLIT_IDENTITY;
import static com.dremio.exec.store.RecordReader.SPLIT_INFORMATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.io.InputFile;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PartitionStatsBasedPruner;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.logical.partition.RecordPruner;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ExpressionInputRewriter;
import com.dremio.exec.store.MinMaxRewriter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Iceberg dataset prel
 */
public class IcebergScanPrel extends ScanRelBase implements Prel, PrelFinalizable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergScanPrel.class);
  private static final Function<RexNode, List<Integer>> getUsedIndices = cond -> {
    Set<Integer> usedIndices = new HashSet<>();
    cond.accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitInputRef(RexInputRef inputRef) {
        usedIndices.add(inputRef.getIndex());
        return null;
      }
    });
    return usedIndices.stream().sorted().collect(Collectors.toList());
  };

  private final ScanFilter filter;
  private final boolean arrowCachingEnabled;
  private final PruneFilterCondition pruneCondition;
  private final OptimizerRulesContext context;
  private final SupportsIcebergRootPointer icebergRootPointerPlugin;
  private final String partitionStatsFile;
  private Long survivingRecords;
  private final boolean isConvertedIcebergDataset;
  private final boolean isPruneConditionOnImplicitCol;

  public IcebergScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                         TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
                         ScanFilter filter, boolean arrowCachingEnabled, PruneFilterCondition pruneCondition,
                         OptimizerRulesContext context, String partitionStatsFile, boolean isConvertedIcebergDataset) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    this.filter = filter;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.pruneCondition = pruneCondition;
    this.context = context;
    this.icebergRootPointerPlugin = context.getCatalogService().getSource(pluginId);
    this.partitionStatsFile = partitionStatsFile;
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
    this.isPruneConditionOnImplicitCol = pruneCondition != null && pruneCondition.getPartitionExpression() != null && isConditionOnImplicitCol();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(),
      observedRowcountAdjustment, this.filter, this.arrowCachingEnabled, this.pruneCondition, context, partitionStatsFile, isConvertedIcebergDataset);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    ScanFilter newFilter = (filter != null && filter instanceof ParquetScanFilter) ?
      ((ParquetScanFilter) filter).applyProjection(projection, rowType, getCluster(), getBatchSchema()) : filter;
    PruneFilterCondition pruneFilterCondition = pruneCondition == null ? null :
      pruneCondition.applyProjection(projection, rowType, getCluster(), getBatchSchema());
    return new IcebergScanPrel(getCluster(), getTraitSet(), table, pluginId, tableMetadata, projection,
      observedRowcountAdjustment, newFilter, this.arrowCachingEnabled, pruneFilterCondition, context, partitionStatsFile, isConvertedIcebergDataset);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  private BatchSchema getManifestFileReaderSchema(List<SchemaPath> manifestFileReaderColumns) {
    BatchSchema manifestFileReaderSchema = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;
    if (pruneCondition == null) {
      return manifestFileReaderSchema;
    }

    // add partition column filter conditions
    manifestFileReaderSchema = getSchemaWithUsedColumns(pruneCondition.getPartitionExpression(), manifestFileReaderColumns, manifestFileReaderSchema);

    // add non partition column filter conditions for native iceberg tables
    if (!isConvertedIcebergDataset()) {
      manifestFileReaderSchema = getSchemaWithMinMaxUsedColumns(pruneCondition.getNonPartitionRange(), manifestFileReaderColumns, manifestFileReaderSchema);
    }
    return manifestFileReaderSchema;
  }

  @Override
  public Prel finalizeRel() {
    List<SchemaPath> manifestListReaderColumns = new ArrayList<>(Arrays.asList(SchemaPath.getSimplePath(SPLIT_IDENTITY), SchemaPath.getSimplePath(SPLIT_INFORMATION), SchemaPath.getSimplePath(COL_IDS)));
    BatchSchema manifestListReaderSchema = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

    List<SchemaPath> manifestFileReaderColumns = new ArrayList<>(Arrays.asList(SchemaPath.getSimplePath(SPLIT_IDENTITY), SchemaPath.getSimplePath(SPLIT_INFORMATION), SchemaPath.getSimplePath(COL_IDS)));
    BatchSchema manifestFileReaderSchema = getManifestFileReaderSchema(manifestFileReaderColumns);
    if (pruneCondition != null && !isPruneConditionOnImplicitCol) {
      manifestListReaderSchema = getSchemaWithMinMaxUsedColumns(pruneCondition.getPartitionRange(), manifestListReaderColumns, manifestListReaderSchema);
    }

    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    IcebergManifestListPrel manifestListPrel = new IcebergManifestListPrel(getCluster(), getTraitSet(), tableMetadata, manifestListReaderSchema, manifestListReaderColumns,
      getRowTypeFromProjectedColumns(manifestListReaderColumns, manifestListReaderSchema, getCluster()));

    RelNode input = manifestListPrel;
    RexNode manifestListCondition = getManifestListFilter(getCluster().getRexBuilder(), manifestListPrel);
    if (manifestListCondition != null) {
      // Manifest list filter
      input = new FilterPrel(getCluster(), getTraitSet(), manifestListPrel, manifestListCondition);
    }

    // exchange above manifest list scan, which is a leaf level easy scan
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
            input, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, true));

    // Manifest scan phase
    TableFunctionConfig manifestScanTableFunctionConfig =  TableFunctionUtil.getManifestScanTableFunctionConfig(
      tableMetadata, manifestFileReaderColumns, manifestFileReaderSchema, null);

    RelDataType rowTypeFromProjectedColumns = getRowTypeFromProjectedColumns(manifestFileReaderColumns, manifestFileReaderSchema, getCluster());
    TableFunctionPrel manifestScanTF = new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY),
      getTable(), manifestSplitsExchange, tableMetadata, ImmutableList.copyOf(manifestFileReaderColumns),
      manifestScanTableFunctionConfig, rowTypeFromProjectedColumns);

    RelNode input2 = manifestScanTF;
    final RexNode manifestFileCondition = getManifestFileFilter(getCluster().getRexBuilder(), manifestScanTF);
    if (manifestFileCondition != null) {
      // Manifest file filter
      input2 = new FilterPrel(getCluster(), getTraitSet(), manifestScanTF, manifestFileCondition);
    }

    // Exchange above manifest scan phase
    HashToRandomExchangePrel parquetSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
            input2, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, false));

    // table scan phase
    TableFunctionConfig tableFunctionConfig = TableFunctionUtil.getDataFileScanTableFunctionConfig(
      tableMetadata, filter, getProjectedColumns(), arrowCachingEnabled, isConvertedIcebergDataset);
    return new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY), getTable(), parquetSplitsExchange, tableMetadata,
      ImmutableList.copyOf(getProjectedColumns()), tableFunctionConfig, getRowType(), getSurvivingRecords());
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    double selectivityEstimateFactor = 1.0;
    if (filter != null) {
      final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
      selectivityEstimateFactor = plannerSettings.getFilterMinSelectivityEstimateFactor();
    }
    return getSurvivingRecords() * selectivityEstimateFactor;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .itemIf("arrowCachingEnable", arrowCachingEnabled, arrowCachingEnabled)
      .itemIf("filter", filter, filter != null)
      .itemIf("pruneCondition", pruneCondition, pruneCondition != null);
  }

  private Optional<Long> prune() {
    List<String> partitionColumns = tableMetadata.getReadDefinition().getPartitionColumnsList();
    Optional<Long> survivingRecords = Optional.empty();
    if (shouldPrune(partitionColumns)) {
      /*
       * Build a mapping between partition column name and partition column ID.
       * The partition columns can have special columns added by Dremio. The ID is artificial,
       * i.e. it has nothing to do with the column ID or order of the column in the table.
      */
      Map<String, Integer> partitionColToIdMap = Maps.newHashMap();
      int index = 0;
      for (String column : partitionColumns) {
        partitionColToIdMap.put(column, index++);
      }

      /*
       * Build a mapping between partition column ID and the partition columns which are used
       * in the query. For example, for
       * partitionColToIdMap = {col2 -> 0, col4 -> 1, $_dremio_ -> 2, col6 -> 3}, and
       * getRowType().getFieldNames() = [col1, col3, col4, col6]
       * inUseColIdToNameMap would be {1 -> col4, 3 -> col6}
       */
      Map<Integer, String> inUseColIdToNameMap = Maps.newHashMap();
      for (String col : getRowType().getFieldNames()) {
        Integer partitionIndex = partitionColToIdMap.get(col);
        if (partitionIndex != null) {
          inUseColIdToNameMap.put(partitionIndex, col);
        }
      }

      PartitionSpec spec = IcebergUtils.getIcebergPartitionSpec(getBatchSchema(), partitionColumns, null);
      InputFile inputFile = new DremioFileIO(icebergRootPointerPlugin.getFsConfCopy()).newInputFile(partitionStatsFile);
      PartitionStatsReader partitionStatsReader = new PartitionStatsReader(inputFile, spec);
      try (RecordPruner pruner = new PartitionStatsBasedPruner(partitionStatsReader, context, spec)) {
        survivingRecords = Optional.of(pruner.prune(inUseColIdToNameMap, partitionColToIdMap, getUsedIndices, projectedColumns,
          tableMetadata, pruneCondition.getPartitionExpression(), getBatchSchema(), getRowType(), getCluster()));
      } catch (RuntimeException e) {
        logger.error("Encountered exception during row count estimation: ", e);
      }
    }
    return survivingRecords;
  }

  private boolean shouldPrune(List<String> partitionColumns) {
    return pruneCondition != null && pruneCondition.getPartitionExpression() != null
      && partitionColumns != null && StringUtils.isNotEmpty(partitionStatsFile) && !isPruneConditionOnImplicitCol;
  }

  private boolean isConditionOnImplicitCol() {
    int updateColIndex = projectedColumns.indexOf(SchemaPath.getSimplePath(IncrementalUpdateUtils.UPDATE_COLUMN));
    final AtomicBoolean isImplicit = new AtomicBoolean(false);
    pruneCondition.getPartitionExpression().accept(new RexVisitorImpl<Void>(true) {
      public Void visitInputRef(RexInputRef inputRef) {
        isImplicit.set(updateColIndex==inputRef.getIndex());
        return null;
      }
    });
    return isImplicit.get();
  }

  private long getSurvivingRecords() {
    if (survivingRecords == null) {
      boolean enablePartitionStats = context.getPlannerSettings().getOptions().getOption(ENABLE_PARTITION_STATS_USAGE);
      if (enablePartitionStats) {
        // use partition stats to enable row count estimation
        survivingRecords = prune()
          .orElseGet(tableMetadata::getApproximateRecordCount); // unpartitioned table or filter on non-partition columns
      } else {
        survivingRecords = tableMetadata.getApproximateRecordCount();
      }
    }
    return survivingRecords;
  }

  private RexNode getFilterWithIsNullCond(RexNode cond, RexBuilder builder, RelNode input) {
    // checking for isNull with any one of the min/max col is sufficient
    int colIdx = getUsedIndices.apply(cond).stream().findFirst().get();
    RexNode isNullCond = builder.makeCall(SqlStdOperatorTable.IS_NULL, builder.makeInputRef(input, colIdx));
    return RexUtil.flatten(builder, builder.makeCall(SqlStdOperatorTable.OR, isNullCond, cond));
  }

  public RexNode getManifestListFilter(RexBuilder builder, RelNode input) {
    if (pruneCondition == null || isPruneConditionOnImplicitCol) {
      return null;
    }
    RexNode partitionRange = pruneCondition.getPartitionRange();
    if (partitionRange == null) {
      return null;
    }
    return getFilterWithIsNullCond(pruneCondition.getPartitionRange().accept(new MinMaxRewriter(builder, getRowType(), input)),
      builder, input);
  }

  private boolean isConvertedIcebergDataset() {
    return isConvertedIcebergDataset;
  }

  public RexNode getManifestFileFilter(RexBuilder builder, RelNode input) {
    if (pruneCondition == null) {
      return null;
    }
    List<RexNode> filters = new ArrayList<>();
    // add non partition filter conditions for native iceberg tables
    if (!isConvertedIcebergDataset()) {
      RexNode nonPartitionRange = pruneCondition.getNonPartitionRange();
      RelDataType rowType = getRowType();
      if (nonPartitionRange != null) {
        filters.add(getFilterWithIsNullCond(nonPartitionRange.accept(new MinMaxRewriter(builder, rowType, input)), builder, input));
      }
    }
    RexNode partitionExpression = pruneCondition.getPartitionExpression();
    if (partitionExpression != null) {
      filters.add(partitionExpression.accept(new ExpressionInputRewriter(builder, rowType, input, "_val")));
    }
    return filters.size() == 0 ? null : (filters.size()  == 1 ? filters.get(0) : RexUtil.flatten(builder, builder.makeCall(SqlStdOperatorTable.AND, filters)));
  }


  public BatchSchema getSchemaWithMinMaxUsedColumns(RexNode cond, List<SchemaPath> outputColumns, BatchSchema schema) {
    if (cond == null) {
      return schema;
    }

    List<SchemaPath> usedColumns = getUsedIndices.apply(cond).stream()
      .map(i -> FieldReference.getWithQuotedRef(rowType.getFieldNames().get(i))).collect(Collectors.toList());
    // TODO only add _min, _max columns which are used
    usedColumns.forEach(c -> {
      List<String> nameSegments = c.getNameSegments();
      Preconditions.checkArgument(nameSegments.size() == 1);
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_min"));
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_max"));
    });

    List<Field> fields = tableMetadata.getSchema().maskAndReorder(usedColumns).getFields().stream()
      .flatMap(f -> Stream.of(new Field(f.getName() + "_min", f.getFieldType(), f.getChildren()), new Field(f.getName() + "_max", f.getFieldType(), f.getChildren())))
      .collect(Collectors.toList());
    return schema.cloneWithFields(fields);
  }

  public BatchSchema getSchemaWithUsedColumns(RexNode cond, List<SchemaPath> outputColumns, BatchSchema schema) {
    if (cond == null) {
      return schema;
    }

    List<SchemaPath> usedColumns = getUsedIndices.apply(cond).stream()
      .map(i -> FieldReference.getWithQuotedRef(rowType.getFieldNames().get(i))).collect(Collectors.toList());
    usedColumns.forEach(c -> {
      List<String> nameSegments = c.getNameSegments();
      Preconditions.checkArgument(nameSegments.size() == 1);
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_val"));
    });

    List<Field> fields = tableMetadata.getSchema().maskAndReorder(usedColumns).getFields().stream()
      .map(f -> new Field(f.getName() + "_val", f.getFieldType(), f.getChildren()))
      .collect(Collectors.toList());
    return schema.cloneWithFields(fields);
  }
}
