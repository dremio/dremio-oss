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

import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.isUnlimitedSplitIncrementalRefresh;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;
import static com.dremio.service.namespace.DatasetHelper.hasDeltaLakeParquetDataFiles;
import static com.dremio.service.namespace.DatasetHelper.hasIcebergParquetDataFiles;
import static com.dremio.service.namespace.DatasetHelper.hasParquetDataFiles;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnProject;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnSampleScan;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnScan;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FileSystemTableOptimizePrule;
import com.dremio.exec.planner.physical.FileSystemVacuumTablePrule;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.VacuumCatalogPrule;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.deltalake.DeltaLakeHistoryScanPlanBuilder;
import com.dremio.exec.store.deltalake.DeltaLakeHistoryScanTableMetadata;
import com.dremio.exec.store.deltalake.DeltaLakeScanPrel;
import com.dremio.exec.store.dfs.easy.EasyScanPrel;
import com.dremio.exec.store.iceberg.IcebergManifestFileContentScanPrel;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.store.iceberg.IcebergScanPrel;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.mfunctions.TableFilesFunctionTableMetadata;
import com.dremio.exec.store.parquet.ParquetScanPrel;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rules for file system sources. */
public class FileSystemRulesFactory extends StoragePluginTypeRulesFactory {
  private static final Logger logger = LoggerFactory.getLogger(FileSystemRulesFactory.class);

  private static class FileSystemDrule extends SourceLogicalConverter {

    public FileSystemDrule(SourceType pluginType) {
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

  private static class ParquetFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;

    public ParquetFilesystemScanPrule(SourceType pluginType) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "ParquetFileSystemScanPrule");
      this.pluginType = pluginType;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new ParquetScanPrel(
          drel.getCluster(),
          drel.getTraitSet().plus(Prel.PHYSICAL),
          drel.getTable(),
          drel.getPluginId(),
          drel.getTableMetadata(),
          drel.getProjectedColumns(),
          drel.getObservedRowcountAdjustment(),
          drel.getHints(),
          drel.getFilter(),
          drel.getRowGroupFilter(),
          drel.isArrowCachingEnabled(),
          ImmutableList.of());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return !isTableFilesMetadataFunction(drel.getTableMetadata())
          && !isDeltaLakeHistoryFunction(drel.getTableMetadata())
          && pluginType.equals(drel.getPluginId().getType())
          && isParquetDataset(drel.getTableMetadata());
    }
  }

  private static class EasyFilesystemScanPrule extends ConverterRule {
    public EasyFilesystemScanPrule(SourceType pluginType) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "EasyFileSystemScanPrule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      // TODO: this singleton check should be removed once DX-7175 is fixed
      boolean singleton =
          !drel.getTableMetadata()
                  .getStoragePluginId()
                  .getCapabilities()
                  .getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY)
              && drel.getTableMetadata().getSplitCount() == 1;
      return new EasyScanPrel(
          drel.getCluster(),
          drel.getTraitSet()
              .plus(Prel.PHYSICAL)
              .plus(singleton ? DistributionTrait.SINGLETON : DistributionTrait.ANY),
          drel.getTable(),
          drel.getPluginId(),
          drel.getTableMetadata(),
          drel.getProjectedColumns(),
          drel.getObservedRowcountAdjustment(),
          drel.getHints(),
          ImmutableList.of());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return !isTableFilesMetadataFunction(drel.getTableMetadata())
          && !isDeltaLakeHistoryFunction(drel.getTableMetadata())
          && !isParquetDataset(drel.getTableMetadata())
          && !isIcebergDataset(drel.getTableMetadata())
          && !isDeltaLakeDataset(drel.getTableMetadata())
          && !isIcebergMetadata(drel.getTableMetadata())
          && !isUnlimitedSplitIncrementalRefresh(
              drel.getSnapshotDiffContext(), drel.getTableMetadata());
    }
  }

  private static class TableFilesFunctionScanPrule extends ConverterRule {
    public TableFilesFunctionScanPrule(SourceType pluginType) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "TableFilesFunctionScanPrule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new IcebergManifestFileContentScanPrel(
          drel.getCluster(),
          drel.getTraitSet().plus(Prel.PHYSICAL),
          drel.getTable(),
          drel.getTableMetadata(),
          drel.getProjectedColumns(),
          drel.getObservedRowcountAdjustment(),
          drel.getHints());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return isTableFilesMetadataFunction(drel.getTableMetadata());
    }
  }

  private static class IcebergFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;
    private final OptimizerRulesContext context;

    public IcebergFilesystemScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "IcebergFilesystemScanPrule");
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
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType())
          && isIcebergDataset(drel.getTableMetadata());
    }
  }

  private static class DeltaLakeFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;
    private final OptimizerRulesContext context;

    public DeltaLakeFilesystemScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "DeltaLakeFilesystemScanPrule");
      this.pluginType = pluginType;
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new DeltaLakeScanPrel(
          drel.getCluster(),
          drel.getTraitSet().plus(Prel.PHYSICAL),
          drel.getTable(),
          drel.getPluginId(),
          drel.getTableMetadata(),
          drel.getProjectedColumns(),
          drel.getObservedRowcountAdjustment(),
          drel.getHints(),
          drel.getFilter(),
          drel.getRowGroupFilter(),
          drel.isArrowCachingEnabled(),
          drel.getPartitionFilter());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType())
          && isDeltaLakeDataset(drel.getTableMetadata());
    }
  }

  private static class DeltaLakeFilesystemHistoryScanPrule extends ConverterRule {
    public DeltaLakeFilesystemHistoryScanPrule(SourceType pluginType) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "DeltaLakeFilesystemHistoryScanPrule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return DeltaLakeHistoryScanPlanBuilder.fromDrel(drel).build();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return isDeltaLakeHistoryFunction(drel.getTableMetadata());
    }
  }

  @Override
  public Set<RelOptRule> getRules(
      OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {

    switch (phase) {
      case LOGICAL:
        ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
        builder.add(new FileSystemDrule(pluginType));

        if (optimizerContext.getPlannerSettings().isPartitionPruningEnabled()) {
          builder.add(
              new PruneScanRuleFilterOnProject<>(
                  pluginType, FilesystemScanDrel.class, optimizerContext));
          builder.add(
              new PruneScanRuleFilterOnScan<>(
                  pluginType, FilesystemScanDrel.class, optimizerContext));
          builder.add(
              new PruneScanRuleFilterOnSampleScan<>(
                  pluginType, FilesystemScanDrel.class, optimizerContext));
        }

        return builder.build();

      case PHYSICAL:
        return ImmutableSet.<RelOptRule>of(
            new IcebergMetadataFilesystemScanPrule(pluginType, optimizerContext),
            new EasyFilesystemScanPrule(pluginType),
            new ParquetFilesystemScanPrule(pluginType),
            new IcebergFilesystemScanPrule(pluginType, optimizerContext),
            new DeltaLakeFilesystemScanPrule(pluginType, optimizerContext),
            new DeltaLakeFilesystemHistoryScanPrule(pluginType),
            ConvertCountToDirectScan.getAggOnScan(pluginType),
            ConvertCountToDirectScan.getAggProjOnScan(pluginType),
            new TableFilesFunctionScanPrule(pluginType),
            new FileSystemTableOptimizePrule(optimizerContext),
            new FileSystemVacuumTablePrule(optimizerContext),
            new VacuumCatalogPrule(optimizerContext));

      default:
        return ImmutableSet.<RelOptRule>of();
    }
  }

  /**
   * table_files are being supported for iceberg dataset and iceberg metadata(internal iceberg
   * table). it must be false for all the other prules except TableFilesFunctionScanPrule
   */
  public static boolean isTableFilesMetadataFunction(TableMetadata datasetPointer) {
    return datasetPointer instanceof TableFilesFunctionTableMetadata;
  }

  public static boolean isDeltaLakeHistoryFunction(TableMetadata datasetPointer) {
    return datasetPointer instanceof DeltaLakeHistoryScanTableMetadata;
  }

  public static boolean isIcebergDataset(TableMetadata datasetPointer) {
    return !isTableFilesMetadataFunction(datasetPointer)
        && datasetPointer.getFormatSettings() != null
        && !isIcebergMetadata(datasetPointer)
        && datasetPointer.getFormatSettings().getType() == FileType.ICEBERG;
  }

  public static boolean isDeltaLakeDataset(TableMetadata datasetPointer) {
    return !isDeltaLakeHistoryFunction(datasetPointer)
        && datasetPointer.getFormatSettings() != null
        && !isIcebergMetadata(datasetPointer)
        && datasetPointer.getFormatSettings().getType() == FileType.DELTA;
  }

  private static boolean isParquetDataset(TableMetadata datasetPointer) {
    return datasetPointer.getFormatSettings() != null
        && !isIcebergMetadata(datasetPointer)
        && DatasetHelper.hasParquetDataFiles(datasetPointer.getFormatSettings());
  }

  public static boolean isIcebergMetadata(TableMetadata datasetPointer) {
    return datasetPointer.getDatasetConfig().getPhysicalDataset() != null
        && Boolean.TRUE.equals(
            datasetPointer.getDatasetConfig().getPhysicalDataset().getIcebergMetadataEnabled());
  }

  public static boolean checkSupportForParquetPushdown(TableMetadata tableMetadata) {
    FileConfig fileConfig = tableMetadata.getFormatSettings();
    if (fileConfig != null) {
      return hasParquetDataFiles(fileConfig)
          || hasIcebergParquetDataFiles(fileConfig)
          || hasDeltaLakeParquetDataFiles(fileConfig);
    }
    if (isTableFilesMetadataFunction(tableMetadata) || isDeltaLakeHistoryFunction(tableMetadata)) {
      return false;
    }
    Optional<FileType> filetype =
        Optional.ofNullable(tableMetadata.getDatasetConfig())
            .map(DatasetConfig::getPhysicalDataset)
            .map(PhysicalDataset::getIcebergMetadata)
            .map(IcebergMetadata::getFileType);
    return filetype.isPresent()
        // we only support Parquet for native Iceberg
        && (FileType.PARQUET.equals(filetype.get()) || FileType.ICEBERG.equals(filetype.get()));
  }

  public static class IcebergMetadataFilesystemScanPrule extends ConverterRule {
    private final OptimizerRulesContext context;

    public IcebergMetadataFilesystemScanPrule(
        SourceType pluginType, OptimizerRulesContext context) {
      super(
          FilesystemScanDrel.class,
          Rel.LOGICAL,
          Prel.PHYSICAL,
          pluginType.value() + "IcebergMetadataFilesystemScanPrule");
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      InternalIcebergScanTableMetadata icebergTableMetadata =
          getInternalIcebergTableMetadata(drel.getTableMetadata(), context);
      IcebergScanPrel icebergScanPrel =
          new IcebergScanPrel(
              drel.getCluster(),
              drel.getTraitSet().plus(Prel.PHYSICAL),
              drel.getTable(),
              icebergTableMetadata.getIcebergTableStoragePlugin(),
              icebergTableMetadata,
              drel.getProjectedColumns(),
              drel.getObservedRowcountAdjustment(),
              drel.getHints(),
              drel.getFilter(),
              drel.getRowGroupFilter(),
              drel.isArrowCachingEnabled(),
              drel.getPartitionFilter(),
              context,
              true,
              drel.getSurvivingRowCount(),
              drel.getSurvivingFileCount(),
              drel.canUsePartitionStats(),
              ManifestScanFilters.empty(),
              drel.getSnapshotDiffContext(),
              drel.isPartitionValuesEnabled(),
              drel.getPartitionStatsStatus());
      // generate query plans for cases when we are querying the data changes between snapshots
      // for example Incremental Refresh by Snapshot (Append only or By Partition)
      if (drel.getSnapshotDiffContext().isEnabled()) {
        final IcebergScanPlanBuilder icebergScanPlanBuilder =
            new IcebergScanPlanBuilder(icebergScanPrel);
        return icebergScanPlanBuilder.build();
      }
      return icebergScanPrel;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return supportsConvertedIcebergDataset(context, drel.getTableMetadata());
    }

    public static InternalIcebergScanTableMetadata getInternalIcebergTableMetadata(
        TableMetadata tableMetadata, OptimizerRulesContext context) {
      FileSystemPlugin<?> metadataStoragePlugin =
          context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      // Checks if the iceberg table directory exists
      IcebergMetadata icebergMetadata =
          tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
      if (icebergMetadata != null && StringUtils.isNotEmpty(icebergMetadata.getTableUuid())) {
        return new InternalIcebergScanTableMetadata(
            tableMetadata, metadataStoragePlugin, icebergMetadata.getTableUuid());
      } else {
        throw UserException.invalidMetadataError()
            .message(
                "Error accessing table metadata created by Dremio, re-promote to refresh metadata")
            .build(logger);
      }
    }

    public static boolean supportsConvertedIcebergDataset(
        OptimizerRulesContext context, TableMetadata datasetPointer) {
      if (isTableFilesMetadataFunction(datasetPointer)
          || isDeltaLakeHistoryFunction(datasetPointer)) {
        return false;
      }
      boolean isIcebergMetadata = isIcebergMetadata(datasetPointer);
      if (!isIcebergMetadata) {
        return false;
      }

      StoragePlugin plugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      if (plugin == null) {
        String message =
            String.format(
                "Metadata source plugin is not available for the dataset %s . ",
                datasetPointer.getName().getSchemaPath());
        throw new IllegalStateException(message);
      }
      return true;
    }
  }
}
