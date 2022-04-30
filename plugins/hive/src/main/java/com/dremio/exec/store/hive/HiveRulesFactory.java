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
package com.dremio.exec.store.hive;

import static com.dremio.exec.store.dfs.FileSystemRulesFactory.IcebergMetadataFilesystemScanPrule.getInternalIcebergTableMetadata;
import static com.dremio.exec.store.dfs.FileSystemRulesFactory.IcebergMetadataFilesystemScanPrule.supportsConvertedIcebergDataset;
import static com.dremio.exec.store.dfs.FileSystemRulesFactory.getPartitionStatsFile;
import static com.dremio.exec.store.dfs.FileSystemRulesFactory.isIcebergMetadata;
import static com.dremio.service.namespace.DatasetHelper.supportsPruneFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnProject;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnScan;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.exec.store.hive.orc.ORCFilterPushDownRule;
import com.dremio.exec.store.iceberg.HiveIcebergScanTableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPrel;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.file.proto.FileType;
import com.github.slugify.Slugify;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Rules factory for Hive. This must be instance based because any matches
 * on Hive nodes are ClassLoader-specific, which are tied to a plugin.
 *
 * Note that Calcite uses the description field in RelOptRule to uniquely
 * identify different rules so the description must be based on the plugin for
 * all rules.
 */
public class HiveRulesFactory implements StoragePluginRulesFactory {
  private static final Slugify SLUGIFY = new Slugify();
  private static final Logger logger = LoggerFactory.getLogger(HiveRulesFactory.class);

  private static class HiveScanDrule extends ConverterRule {
    private final String pluginName;
    public HiveScanDrule(StoragePluginId pluginId) {
      super(ScanCrel.class, Convention.NONE, Rel.LOGICAL, pluginId.getType().value() + ".HiveScanDrule."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
      pluginName = pluginId.getName();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      ScanCrel scan = call.rel(0);
      return scan.getPluginId().getName().equals(pluginName);
    }

    @Override
    public final Rel convert(RelNode rel) {
      final ScanCrel crel = (ScanCrel) rel;
      return new HiveScanDrel(crel.getCluster(), crel.getTraitSet().plus(Rel.LOGICAL), crel.getTable(),
        crel.getPluginId(), crel.getTableMetadata(), crel.getProjectedColumns(), crel.getObservedRowcountAdjustment(), null);
    }

  }

  public static class HiveScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {

    private final ScanFilter filter;
    private final HiveReaderProto.ReaderType readerType;
    private final PruneFilterCondition partitionFilter;

    public HiveScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
        ScanFilter filter) {
      this(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment, filter, null, null);
    }

    private HiveScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                         TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
                         ScanFilter filter, HiveReaderProto.ReaderType readerType, PruneFilterCondition partitionFilter) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
      assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
      this.filter = filter;
      this.readerType = Optional.ofNullable(readerType)
        .orElseGet(() -> {
          try {
            return HiveTableXattr.parseFrom(getTableMetadata().getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer()).getReaderType();
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Unable to retrieve the reader type table attribute.", e);
          }
        });
      this.partitionFilter = partitionFilter;
    }

    // Clone with partition filter
    private HiveScanDrel(HiveScanDrel that, PruneFilterCondition partitionFilter) {
      super(that.getCluster(), that.getTraitSet(), that.getTable(), that.getPluginId(), that.getTableMetadata(), that.getProjectedColumns(), that.getObservedRowcountAdjustment());
      assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
      this.filter = that.getFilter();
      this.readerType = that.readerType;
      this.partitionFilter = partitionFilter;
    }

    @Override
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
      if(tableMetadata.getSplitCount() == 0){
        return planner.getCostFactory().makeInfiniteCost();
      }
      return super.computeSelfCost(planner, mq);
    }

    public ScanFilter getFilter() {
      return filter;
    }

    @Override
    public PruneFilterCondition getPartitionFilter() {
      return partitionFilter;
    }

    public HiveReaderProto.ReaderType getReaderType() {
      return readerType;
    }

    @Override
    public double getCostAdjustmentFactor(){
      return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
    }

    @Override
    public double getFilterReduction(){
      if(filter != null){
        double selectivity = 0.15d;

        double max = PrelUtil.getPlannerSettings(getCluster()).getFilterMaxSelectivityEstimateFactor();
        double min = PrelUtil.getPlannerSettings(getCluster()).getFilterMinSelectivityEstimateFactor();

        if(selectivity < min) {
          selectivity = min;
        }
        if(selectivity > max) {
          selectivity = max;
        }

        return selectivity;
      }else {
        return 1d;
      }
    }

    @Override
    public FilterableScan applyFilter(ScanFilter filter) {
      return new HiveScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, getProjectedColumns(),
        observedRowcountAdjustment, filter, readerType, partitionFilter);
    }

    @Override
    public HiveScanDrel applyPartitionFilter(PruneFilterCondition partitionFilter) {
      Preconditions.checkArgument(supportsPruneFilter(getTableMetadata().getDatasetConfig()));
      return new HiveScanDrel(this, partitionFilter);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new HiveScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(),
        observedRowcountAdjustment, filter, readerType, partitionFilter);
    }

    @Override
    public RelNode applyDatasetPointer(TableMetadata newDatasetPointer) {
      return new HiveScanDrel(getCluster(), traitSet, new RelOptNamespaceTable(newDatasetPointer, getCluster()),
        pluginId, newDatasetPointer, getProjectedColumns(), observedRowcountAdjustment, filter, readerType, partitionFilter);
    }

    @Override
    public HiveScanDrel cloneWithProject(List<SchemaPath> projection, boolean preserveFilterColumns) {
      if (filter != null && preserveFilterColumns) {
        final List<SchemaPath> newProjection = new ArrayList<>(projection);
        final Set<SchemaPath> projectionSet = new HashSet<>(projection);
        final List<SchemaPath> paths = filter.getPaths();
        if (paths != null) {
          for (SchemaPath col : paths) {
            if (!projectionSet.contains(col)) {
              newProjection.add(col);
            }
          }
          return cloneWithProject(newProjection);
        }
      }
      return cloneWithProject(projection);
    }

    @Override
    public HiveScanDrel cloneWithProject(List<SchemaPath> projection) {
      return new HiveScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection,
        observedRowcountAdjustment, filter, readerType, partitionFilter == null ? null : partitionFilter.applyProjection(projection, rowType, getCluster(), getBatchSchema()));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      pw = super.explainTerms(pw);
      if (partitionFilter != null) {
        pw.item("partitionFilters", partitionFilter.toString());
      }
      return pw.itemIf("filters",  filter, filter != null);
    }
  }

  private static class EliminateEmptyScans extends RelOptRule {

    public EliminateEmptyScans(StoragePluginId pluginId) {
      // Note: matches to HiveScanDrel.class with this rule instance are guaranteed to be local to the same plugin
      // because this match implicitly ensures the classloader is the same.
      super(RelOptHelper.any(HiveScanDrel.class), "Hive::eliminate_empty_scans."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveScanDrel scan = call.rel(0);
      if(scan.getTableMetadata().getSplitCount() == 0){
        call.transformTo(new EmptyRel(scan.getCluster(), scan.getTraitSet(), scan.getRowType(), scan.getProjectedSchema()));
      }
    }
  }

  public static class HiveScanPrel extends ScanPrelBase implements PruneableScan {

    private final ScanFilter filter;
    private final HiveReaderProto.ReaderType readerType;
    public static final TypeValidators.BooleanValidator C3_RUNTIME_AFFINITY = new TypeValidators.BooleanValidator("c3.runtime.affinity", false);

    public HiveScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
                        ScanFilter filter, HiveReaderProto.ReaderType readerType, List<Info> runtimeFilters) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment, runtimeFilters);
      this.filter = filter;
      this.readerType = readerType;
    }

    @Override
    public boolean hasFilter() {
      return filter != null;
    }

    public ScanFilter getFilter() {
      return filter;
    }

    @Override
    public RelNode applyDatasetPointer(TableMetadata newDatasetPointer) {
      return new HiveScanPrel(getCluster(), traitSet, getTable(), pluginId, newDatasetPointer, getProjectedColumns(),
        observedRowcountAdjustment, filter, readerType, getRuntimeFilters());
    }

    @Override
    public HiveScanPrel cloneWithProject(List<SchemaPath> projection) {
      return new HiveScanPrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, getProjectedColumns(),
        observedRowcountAdjustment, filter, readerType, getRuntimeFilters());
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(getProjectedColumns());
      return new HiveGroupScan(
        creator.props(this, tableMetadata.getUser(), schema, HiveSettings.RESERVE, HiveSettings.LIMIT),
        tableMetadata, getProjectedColumns(), filter, creator.getContext(),
        creator.getContext().getOptions().getOption(C3_RUNTIME_AFFINITY));
    }

    @Override
    public double getCostAdjustmentFactor(){
      return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      pw = super.explainTerms(pw);
      pw = pw.item("mode", readerType.name());
      return pw.itemIf("filters", filter, filter != null);
    }

    @Override
    public double getFilterReduction(){
      if(filter != null){
        return 0.15d;
      }else {
        return 1d;
      }
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new HiveScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(),
        observedRowcountAdjustment, filter, readerType, getRuntimeFilters());
    }

  }

  private static class HiveScanPrule extends ConverterRule {
    private final OptimizerRulesContext context;

    public HiveScanPrule(StoragePluginId pluginId, OptimizerRulesContext context) {
      // Note: matches to HiveScanDrel.class with this rule instance are guaranteed to be local to the same plugin
      // because this match implicitly ensures the classloader is the same.
      super(HiveScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginId.getType().value() + "HiveScanPrule."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      HiveScanDrel drel = (HiveScanDrel) rel;
      return new HiveScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL), drel.getTable(),
        drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.getReaderType(), ImmutableList.of());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      HiveScanDrel drel = call.rel(0);
      return !isIcebergMetadata(drel.getTableMetadata()) && !isHiveIcebergDataset(drel);
    }
  }

  private static class InternalIcebergScanPrule extends ConverterRule {
    private final OptimizerRulesContext context;

    public InternalIcebergScanPrule(StoragePluginId pluginId, OptimizerRulesContext context) {
      super(HiveScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginId.getType().value() + "IcebergScanPrule."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode relNode) {
      HiveScanDrel drel = (HiveScanDrel) relNode;
      InternalIcebergScanTableMetadata icebergTableMetadata = getInternalIcebergTableMetadata(drel.getTableMetadata(), context);
      String partitionStatsFile = icebergTableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionStatsFile();
      return new IcebergScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL),
        drel.getTable(), icebergTableMetadata.getIcebergTableStoragePlugin(), icebergTableMetadata, drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), drel.getFilter(), false, /* TODO enable */
        drel.getPartitionFilter(), context, partitionStatsFile, true);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      HiveScanDrel drel = call.rel(0);
      return supportsConvertedIcebergDataset(context, drel.getTableMetadata()) && !isHiveIcebergDataset(drel);
    }
  }

  private static class HiveIcebergScanPrule extends ConverterRule {
    private final OptimizerRulesContext context;
    private final String storagePluginName;

    public HiveIcebergScanPrule(StoragePluginId pluginId, OptimizerRulesContext context) {
      super(HiveScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginId.getType().value() + "HiveIcebergScanPrule."
        + SLUGIFY.slugify(pluginId.getName()) + "." + UUID.randomUUID().toString());
      this.context = context;
      this.storagePluginName = pluginId.getName();
    }

    @Override
    public RelNode convert(RelNode relNode) {
      HiveScanDrel drel = (HiveScanDrel) relNode;
      String partitionStatsFile = getPartitionStatsFile(drel);
      HiveIcebergScanTableMetadata icebergScanTableMetadata = new HiveIcebergScanTableMetadata(drel.getTableMetadata(),
        context.getCatalogService().getSource(storagePluginName));

      return new IcebergScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL),
        drel.getTable(), drel.getPluginId(), icebergScanTableMetadata, drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), drel.getFilter(), false,
        drel.getPartitionFilter(), context, partitionStatsFile, false);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      HiveScanDrel drel = call.rel(0);
      return isHiveIcebergDataset(drel);
    }
  }

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {
    return ImmutableSet.of();
  }

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginId pluginId) {
    switch(phase){
      case LOGICAL:
        ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
        builder.add(new HiveScanDrule(pluginId));
        builder.add(new EliminateEmptyScans(pluginId));


        final PlannerSettings plannerSettings = optimizerContext.getPlannerSettings();

        if (plannerSettings.isPartitionPruningEnabled()) {
          builder.add(new PruneScanRuleFilterOnProject<>(pluginId, HiveScanDrel.class, optimizerContext));
          builder.add(new PruneScanRuleFilterOnScan<>(pluginId, HiveScanDrel.class, optimizerContext));
          builder.add(new PruneScanRuleBase.PruneScanRuleFilterOnSampleScan<>(pluginId, HiveScanDrel.class, optimizerContext));
        }

        final HiveSettings hiveSettings = new HiveSettings(plannerSettings.getOptions());
        if (hiveSettings.vectorizeOrcReaders() && hiveSettings.enableOrcFilterPushdown()) {
          builder.add(new ORCFilterPushDownRule(pluginId));
        }

        return builder.build();

      case PHYSICAL:
        return ImmutableSet.of(
          new HiveScanPrule(pluginId, optimizerContext),
          new InternalIcebergScanPrule(pluginId, optimizerContext),
          new HiveIcebergScanPrule(pluginId, optimizerContext)
        );

      default:
        return ImmutableSet.of();

    }
  }

  private static boolean isHiveIcebergDataset(HiveScanDrel drel) {
    if (drel == null ||
          drel.getTableMetadata() == null ||
          drel.getTableMetadata().getDatasetConfig() == null ||
          drel.getTableMetadata().getDatasetConfig().getPhysicalDataset() == null ||
          drel.getTableMetadata().getDatasetConfig().getPhysicalDataset().getIcebergMetadata() == null ||
          drel.getTableMetadata().getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getFileType() == null) {
      return false;
    }

    return drel.getTableMetadata().getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getFileType() == FileType.ICEBERG;
  }
}
