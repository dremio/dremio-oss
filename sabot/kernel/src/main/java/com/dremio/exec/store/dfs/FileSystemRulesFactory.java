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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static com.dremio.exec.planner.physical.PlannerSettings.UNLIMITED_SPLITS_SUPPORT;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnProject;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnSampleScan;
import com.dremio.exec.planner.logical.partition.PruneScanRuleBase.PruneScanRuleFilterOnScan;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.deltalake.DeltaLakeScanPrel;
import com.dremio.exec.store.dfs.easy.EasyScanPrel;
import com.dremio.exec.store.iceberg.IcebergManifestFileContentScanPrel;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.store.iceberg.IcebergScanPrel;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.mfunctions.TableFilesFunctionTableMetadata;
import com.dremio.exec.store.parquet.ParquetScanPrel;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Rules for file system sources.
 */
public class FileSystemRulesFactory extends StoragePluginTypeRulesFactory {
  private static final Logger logger = LoggerFactory.getLogger(FileSystemRulesFactory.class);

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
        drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.isArrowCachingEnabled(), ImmutableList.of());
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
      return new EasyScanPrel(
        drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL).plus(singleton ? DistributionTrait.SINGLETON : DistributionTrait.ANY),
        drel.getTable(), drel.getPluginId(), drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), ImmutableList.of());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return !(drel.getTableMetadata() instanceof TableFilesFunctionTableMetadata) && !isParquetDataset(drel.getTableMetadata()) && !isIcebergDataset(drel.getTableMetadata())
              && !isDeltaLakeDataset(drel.getTableMetadata()) && !isIcebergMetadata(drel.getTableMetadata());
    }
  }

  private static class TableFilesFunctionScanPrule extends ConverterRule {
    public TableFilesFunctionScanPrule(SourceType pluginType) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "TableFilesFunctionScanPrule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new IcebergManifestFileContentScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL),
        drel.getTable(), drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = call.rel(0);
      return drel.getTableMetadata() instanceof TableFilesFunctionTableMetadata;
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
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return IcebergScanPlanBuilder.fromDrel(drel, context, drel.isArrowCachingEnabled(), drel.canUsePartitionStats()).build();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType()) && isIcebergDataset(drel.getTableMetadata());
    }
  }

  private static class DeltaLakeFilesystemScanPrule extends ConverterRule {
    private final SourceType pluginType;
    private final OptimizerRulesContext context;

    public DeltaLakeFilesystemScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "DeltaLakeFilesystemScanPrule");
      this.pluginType = pluginType;
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      if (!context.getPlannerSettings().getOptions().getOption(PlannerSettings.ENABLE_DELTALAKE)) {
        throw UserException.unsupportedError(new IllegalStateException("Please contact customer support for steps to enable the Delta Lake tables feature.")).buildSilently();
      }
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      return new DeltaLakeScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL),
              drel.getTable(), drel.getPluginId(), drel.getTableMetadata(),
              drel.getProjectedColumns(), drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.isArrowCachingEnabled(), drel.getPartitionFilter());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return pluginType.equals(drel.getPluginId().getType()) && isDeltaLakeDataset(drel.getTableMetadata());
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
            new IcebergMetadataFilesystemScanPrule(pluginType, optimizerContext),
            new EasyFilesystemScanPrule(pluginType),
            new ParquetFilesystemScanPrule(pluginType),
            new IcebergFilesystemScanPrule(pluginType, optimizerContext),
            new DeltaLakeFilesystemScanPrule(pluginType, optimizerContext),
            ConvertCountToDirectScan.getAggOnScan(pluginType),
            ConvertCountToDirectScan.getAggProjOnScan(pluginType),
            new TableFilesFunctionScanPrule(pluginType)
            );

      default:
        return ImmutableSet.<RelOptRule>of();

    }
  }

  public static boolean isIcebergDataset(TableMetadata datasetPointer) {
    return datasetPointer.getFormatSettings() != null && !isIcebergMetadata(datasetPointer) && datasetPointer.getFormatSettings().getType() == FileType.ICEBERG;
  }

  private static boolean isDeltaLakeDataset(TableMetadata datasetPointer) {
    return datasetPointer.getFormatSettings() != null && !isIcebergMetadata(datasetPointer) && datasetPointer.getFormatSettings().getType() == FileType.DELTA;
  }

  private static boolean isParquetDataset(TableMetadata datasetPointer) {
    return datasetPointer.getFormatSettings() != null && !isIcebergMetadata(datasetPointer) && DatasetHelper.hasParquetDataFiles(datasetPointer.getFormatSettings());
  }

  public static boolean isIcebergMetadata(TableMetadata datasetPointer) {
    return datasetPointer.getDatasetConfig().getPhysicalDataset() != null && Boolean.TRUE.equals(datasetPointer.getDatasetConfig().getPhysicalDataset().getIcebergMetadataEnabled());
  }

  public static class IcebergMetadataFilesystemScanPrule extends ConverterRule {
    private final OptimizerRulesContext context;

    public IcebergMetadataFilesystemScanPrule(SourceType pluginType, OptimizerRulesContext context) {
      super(FilesystemScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, pluginType.value() + "IcebergMetadataFilesystemScanPrule");
      this.context = context;
    }

    @Override
    public RelNode convert(RelNode rel) {
      FilesystemScanDrel drel = (FilesystemScanDrel) rel;
      InternalIcebergScanTableMetadata icebergTableMetadata = getInternalIcebergTableMetadata(drel.getTableMetadata(), context);
      return new IcebergScanPrel(drel.getCluster(), drel.getTraitSet().plus(Prel.PHYSICAL),
        drel.getTable(), icebergTableMetadata.getIcebergTableStoragePlugin(), icebergTableMetadata, drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment(), drel.getFilter(), drel.isArrowCachingEnabled(), drel.getPartitionFilter(), context, true, drel.getSurvivingRowCount(), drel.getSurvivingFileCount(), drel.canUsePartitionStats());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      FilesystemScanDrel drel = (FilesystemScanDrel) call.rel(0);
      return supportsConvertedIcebergDataset(context, drel.getTableMetadata());
    }

    public static InternalIcebergScanTableMetadata getInternalIcebergTableMetadata(TableMetadata tableMetadata, OptimizerRulesContext context) {
      FileSystemPlugin<?> metadataStoragePlugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      // Checks if the iceberg table directory exists
      IcebergMetadata icebergMetadata = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
      if (icebergMetadata!=null && StringUtils.isNotEmpty(icebergMetadata.getTableUuid())) {
        return new InternalIcebergScanTableMetadata(tableMetadata, metadataStoragePlugin, icebergMetadata.getTableUuid());
      } else {
        throw UserException.invalidMetadataError()
                .message("Error accessing table metadata created by Dremio, re-promote to refresh metadata")
                .build(logger);
      }
    }

    public static boolean supportsConvertedIcebergDataset(OptimizerRulesContext context, TableMetadata datasetPointer) {
      if (datasetPointer instanceof TableFilesFunctionTableMetadata) {
        return false;
      }
      boolean isIcebergMetadata = isIcebergMetadata(datasetPointer);
      if (!isIcebergMetadata) {
        return false;
      }

      StoragePlugin plugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
      OptionResolver optionResolver = context.getPlannerSettings().getOptions();
      boolean supportOptionsEnabled = plugin != null && optionResolver.getOption(ExecConstants.ENABLE_ICEBERG)
              && optionResolver.getOption(UNLIMITED_SPLITS_SUPPORT);
      if (!supportOptionsEnabled) {
        String message = String.format("The dataset %s has been promoted with a different style of metadata. " +
                "To use the dataset, turn on following flags [%s, %s], which are currently set to [%b, %b] respectively. " +
                "Alternatively, you can forget and re-promote the dataset.", datasetPointer.getName().getSchemaPath(),
                ENABLE_ICEBERG.getOptionName(), UNLIMITED_SPLITS_SUPPORT.getOptionName(),
                context.getOptions().getOption(ENABLE_ICEBERG),
                context.getOptions().getOption(UNLIMITED_SPLITS_SUPPORT));
        throw new IllegalStateException(message);
      }
      return true;
    }
  }

  public static String getPartitionStatsFile(ScanRelBase drel) {
    if(DatasetHelper.isInternalIcebergTable(drel.getTableMetadata().getDatasetConfig())) {
      return drel.getTableMetadata().getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionStatsFile();
    } else {
      byte[] byteBuffer = drel.getTableMetadata().getReadDefinition().getExtendedProperty().toByteArray();

      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr;
      try {
        icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetXAttr.PARSER, byteBuffer);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      return icebergDatasetXAttr.getPartitionStatsFile();
    }
  }
}
