/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.IOException;
import java.util.List;
import java.util.Set;

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

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.logical.LogicalPlanImplementor;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnProject;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnScan;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.exec.store.hive.HiveRulesFactory.HiveScanDrel;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.parquet.FilterConditions;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.StoragePluginType;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

public class HiveRulesFactory implements StoragePluginTypeRulesFactory {

  private static class HiveScanDrule extends SourceLogicalConverter {

    public HiveScanDrule(StoragePluginType pluginType) {
      super(pluginType);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
      return new HiveScanDrel(scan.getCluster(), scan.getTraitSet().plus(Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), null);
    }

  }

  public static class HiveScanDrel extends ScanRelBase implements Rel, FilterableScan, PruneableScan {

    private final List<FilterCondition> conditions;

    public HiveScanDrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, List<FilterCondition> conditions) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
      assert traitSet.getTrait(ConventionTraitDef.INSTANCE) == Rel.LOGICAL;
      this.conditions = conditions;
    }

    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
      if(tableMetadata.getSplitCount() == 0){
        return planner.getCostFactory().makeInfiniteCost();
      }
      return super.computeSelfCost(planner, mq);
    }


    @Override
    public LogicalOperator implement(LogicalPlanImplementor implementor) {
      throw new UnsupportedOperationException();
    }

    public List<FilterCondition> getFilterConditions() {
      return conditions;
    }

    @Override
    public double getCostAdjustmentFactor(){
      return FilterConditions.getCostAdjustment(conditions);
    }

    protected double getFilterReduction(){
      if(conditions != null && !conditions.isEmpty()){
        return 0.15d;
      }else {
        return 1d;
      }
    }

    @Override
    public FilterableScan applyConditions(List<FilterCondition> conditions) {
      return new HiveScanDrel(getCluster(), traitSet, table, pluginId, tableMetadata, getProjectedColumns(), observedRowcountAdjustment, conditions);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new HiveScanDrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment, conditions);
    }

    @Override
    public RelNode applyDatasetPointer(TableMetadata newDatasetPointer) {
      return new HiveScanDrel(getCluster(), traitSet, new RelOptNamespaceTable(newDatasetPointer, getCluster()), pluginId, newDatasetPointer, getProjectedColumns(), observedRowcountAdjustment, conditions);
    }

    @Override
    public HiveScanDrel cloneWithProject(List<SchemaPath> projection) {
      return new HiveScanDrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projection, observedRowcountAdjustment, conditions);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      pw = super.explainTerms(pw);
      if(conditions != null && !conditions.isEmpty()){
        return pw.item("filters",  conditions);
      }
      return pw;
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof HiveScanDrel)) {
        return false;
      }
      HiveScanDrel castOther = (HiveScanDrel) other;
      return Objects.equal(conditions, castOther.conditions) && super.equals(other);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), conditions);
    }
  }

  static class EliminateEmptyScans extends RelOptRule {

    public static EliminateEmptyScans INSTANCE = new EliminateEmptyScans();

    public EliminateEmptyScans() {
      super(RelOptHelper.any(HiveScanDrel.class), "Hive::eliminate_empty_scans");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveScanDrel scan = call.rel(0);
      if(scan.getTableMetadata().getSplitCount() == 0){
        call.transformTo(new EmptyRel(scan.getCluster(), scan.getTraitSet(), scan.getRowType(), scan.getBatchSchema()));
      }
    }

  }

  private static class HiveScanPrel extends ScanPrelBase {

    private final List<FilterCondition> conditions;

    public HiveScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
        TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, List<FilterCondition> conditions) {
      super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
      this.conditions = conditions;
    }

    private HiveTableXattr extended;

    private HiveTableXattr getExtended(){
      if(extended == null){
        try {
          extended = HiveTableXattr.parseFrom(getTableMetadata().getReadDefinition().getExtendedProperty().toByteArray());
        } catch (InvalidProtocolBufferException e) {
          throw Throwables.propagate(e);
        }
      }
      return extended;
    }

    @Override
    public HiveScanPrel cloneWithProject(List<SchemaPath> projection) {
      return new HiveScanPrel(getCluster(), getTraitSet(), getTable(), pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment, conditions);
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      return creator.addMetadata(this, new HiveGroupScan(tableMetadata, projectedColumns, conditions == null ? ImmutableList.<FilterCondition>of(): conditions));
    }

    @Override
    public double getCostAdjustmentFactor(){
      return FilterConditions.getCostAdjustment(conditions);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      pw = super.explainTerms(pw);
      pw = pw.item("mode", getExtended().getReaderType().name());
      if(conditions != null && !conditions.isEmpty()){
        return pw.item("filters",  conditions);
      }
      return pw;
    }

    protected double getFilterReduction(){
      if(conditions != null && !conditions.isEmpty()){
        return 0.15d;
      }else {
        return 1d;
      }
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof HiveScanPrel)) {
        return false;
      }
      HiveScanPrel castOther = (HiveScanPrel) other;
      return Objects.equal(conditions, castOther.conditions) && super.equals(other);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), conditions);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new HiveScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment, conditions);
    }

  }

  private static class HiveScanPrule extends ConverterRule {
    private final StoragePluginType pluginType;

    public HiveScanPrule(StoragePluginType pluginType) {
      super(HiveScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.generateRuleName("HiveScanPrule"));
      this.pluginType = pluginType;
    }

    @Override
    public RelNode convert(RelNode rel) {
      HiveScanDrel drel = (HiveScanDrel) rel;
      return new HiveScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL), drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(), drel.getObservedRowcountAdjustment(), drel.getFilterConditions());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      HiveScanDrel drel = (HiveScanDrel) call.rel(0);
      return drel.getPluginId().getType().equals(pluginType);
    }

  }

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase,
      StoragePluginType pluginType) {
    switch(phase){
    case LOGICAL:
      ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
      builder.add(new HiveScanDrule(pluginType));
      builder.add(EliminateEmptyScans.INSTANCE);

      if(optimizerContext.getPlannerSettings().isPartitionPruningEnabled()){
        builder.add(new PruneScanRuleFilterOnProject<HiveScanDrel>(pluginType, HiveScanDrel.class, optimizerContext));
        builder.add(new PruneScanRuleFilterOnScan<HiveScanDrel>(pluginType, HiveScanDrel.class, optimizerContext));
      }

      return builder.build();

    case PHYSICAL:
      return ImmutableSet.<RelOptRule>of(
          new HiveScanPrule(pluginType)
          );

    default:
      return ImmutableSet.<RelOptRule>of();

    }
  }

}
