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
package com.dremio.exec.store.dfs;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnProject;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnSampleScan;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnScan;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.dfs.easy.EasyScanPrel;
import com.dremio.exec.store.iceberg.IcebergScanPrel;
import com.dremio.exec.store.parquet.ParquetScanPrel;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableSet;

/**
 * Rules for file system sources.
 */
public class FileSystemRulesFactory extends StoragePluginTypeRulesFactory {

  private static class FileSystemDrule extends SourceLogicalConverter {

    public FileSystemDrule(SourceType pluginType) {
      super(pluginType);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
      return new FilesystemScanDrel(scan.getCluster(), scan.getTraitSet().plus(Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), false);
    }
  }

  private static class ParquetFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;

    public ParquetFilesystemScanPrule(SourceType pluginType) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "ParquetFileSystemScanPrule");
      this.pluginType = pluginType;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new ParquetScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL), drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.isArrowCachingEnabled());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType()) && isParquetDataset(drel.getTableMetadata());
    }
  }

  private static class EasyFilesystemScanPrule extends ConverterRule {

    public EasyFilesystemScanPrule(SourceType pluginType) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "EasyFileSystemScanPrule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      // TODO: this singleton check should be removed once DX-7175 is fixed
      boolean singleton = !drel.getTableMetadata().getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) && drel.getTableMetadata().getSplitCount() == 1;
      return new EasyScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL).plus(singleton ? DistributionTrait.SINGLETON : DistributionTrait.ANY), drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(), drel.getObservedRowcountAdjustment());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return !isParquetDataset(drel.getTableMetadata()) && !isIcebergDataset(drel.getTableMetadata());
    }
  }

  private static class IcebergFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;
    private final OptimizerRulesContext context;

    public IcebergFilesystemScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "IcebergFilesystemScanPrule");
      this.pluginType = pluginType;
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      if (context.getPlannerSettings().getOptions().getOption(PlannerSettings.ENABLE_ICEBERG_EXECUTION)) {
        FilesystemScanDrel drel = (FilesystemScanDrel) rel;
        boolean singleton = !drel.getTableMetadata().getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) && drel.getTableMetadata().getSplitCount() == 1;
        return new IcebergScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL).plus(singleton ? DistributionTrait.SINGLETON : DistributionTrait.ANY), drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(), drel.getObservedRowcountAdjustment());
      } else {
        FilesystemScanDrel drel = (FilesystemScanDrel) rel;
        return new ParquetScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL), drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(),
          drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.isArrowCachingEnabled());
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType()) && isIcebergDataset(drel.getTableMetadata());
    }
  }

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {

    switch(phase){
      case LOGICAL:
        ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
        builder.add(new FileSystemDrule(pluginType));

        if(optimizerContext.getPlannerSettings().isPartitionPruningEnabled()){
          builder.add(new PruneScanRuleFilterOnProject<>(pluginType, FilesystemScanDrel.class, optimizerContext));
          builder.add(new PruneScanRuleFilterOnScan<>(pluginType, FilesystemScanDrel.class, optimizerContext));
          builder.add(new PruneScanRuleFilterOnSampleScan<>(pluginType, FilesystemScanDrel.class, optimizerContext));
        }

        return builder.build();

      case PHYSICAL:
        return ImmutableSet.<RelOptRule>of(
            new EasyFilesystemScanPrule(pluginType),
            new ParquetFilesystemScanPrule(pluginType),
            new IcebergFilesystemScanPrule(pluginType, optimizerContext),
            ConvertCountToDirectScan.getAggOnScan(pluginType),
            ConvertCountToDirectScan.getAggProjOnScan(pluginType)
            );

      default:
        return ImmutableSet.<RelOptRule>of();

    }
  }

  private static boolean isIcebergDataset(TableMetadata datasetPointer) {
    return datasetPointer.getFormatSettings().getType() == FileType.ICEBERG;
  }

  private static boolean isParquetDataset(TableMetadata datasetPointer) {
    return DatasetHelper.hasParquetDataFiles(datasetPointer.getFormatSettings());
  }
}
