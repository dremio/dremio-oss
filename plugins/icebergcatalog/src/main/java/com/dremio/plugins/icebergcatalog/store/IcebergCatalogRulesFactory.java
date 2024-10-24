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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.store.dfs.FileSystemRulesFactory.isIcebergDataset;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.TableModifyRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableModifyPruleBase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rules for Iceberg Catalog source. */
public class IcebergCatalogRulesFactory extends StoragePluginTypeRulesFactory {
  private static class IcebergCatalogDrule extends SourceLogicalConverter {

    public IcebergCatalogDrule(SourceType pluginType) {
      super(pluginType);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
      return new FilesystemScanDrel(
          scan.getCluster(),
          scan.getTraitSet().plus(Rel.LOGICAL),
          scan.getTable(),
          scan.getPluginId(),
          scan.getTableMetadata(),
          scan.getProjectedColumns(),
          scan.getObservedRowcountAdjustment(),
          scan.getHints(),
          false,
          scan.getSnapshotDiffContext());
    }
  }

  private static class IcebergCatalogScanPrule extends ConverterRule {
    private final SourceType pluginType;
    private final OptimizerRulesContext context;

    public IcebergCatalogScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "IcebergCatalogScanPrule");
      this.pluginType = pluginType;
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return IcebergScanPlanBuilder.fromDrel(
              drel,
              context,
              drel.isArrowCachingEnabled(),
              drel.canUsePartitionStats(),
              drel.isPartitionValuesEnabled())
          .build();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return pluginType.equals(drel.getPluginId().getType())
          && isIcebergDataset(drel.getTableMetadata());
    }
  }

  private static class IcebergCatalogTableModifyPrule extends TableModifyPruleBase {
    public IcebergCatalogTableModifyPrule(OptimizerRulesContext context) {
      super(
          RelOptHelper.some(TableModifyRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)),
          "Prel.IcebergCatalogTableModifyPrule",
          context);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return call.<TableModifyRel>rel(0).getCreateTableEntry().getPlugin()
          instanceof IcebergCatalogPlugin;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      onMatch(
          call,
          ((DremioPrepareTable) call.<TableModifyRel>rel(0).getTable()).getTable().getDataset());
    }
  }

  @Override
  public Set<RelOptRule> getRules(
      OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(new IcebergCatalogDrule(pluginType));

      case PHYSICAL:
        return ImmutableSet.of(
            new IcebergCatalogScanPrule(pluginType, optimizerContext),
            new IcebergCatalogTableModifyPrule(optimizerContext));

      default:
        return ImmutableSet.of();
    }
  }
}
