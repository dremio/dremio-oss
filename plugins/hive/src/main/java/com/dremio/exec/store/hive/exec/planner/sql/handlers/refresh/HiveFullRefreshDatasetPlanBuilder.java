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
package com.dremio.exec.store.hive.exec.planner.sql.handlers.refresh;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.hive.metadata.HiveDatasetMetadata;
import com.dremio.exec.store.hive.metadata.HiveMetadataUtils;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Extension of AbstractRefreshPlanBuilder. Builds a plan for
 * full refresh for Hive.
 */
public class HiveFullRefreshDatasetPlanBuilder extends AbstractRefreshPlanBuilder {

  private static final Logger logger = LoggerFactory.getLogger(HiveFullRefreshDatasetPlanBuilder.class);

  private DatasetHandle handle;

  public HiveFullRefreshDatasetPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, UnlimitedSplitsMetadataProvider metadataProvider) {
    super(config, sqlRefreshDataset, metadataProvider);
    logger.debug("Doing a hive full refresh on dataset. Dataset's full path is {}", datasetPath);
  }

  @Override
  protected DatasetConfig setupDatasetConfig() {
    DatasetConfig config = super.setupDatasetConfig();
    config.setType(DatasetType.PHYSICAL_DATASET);
    config.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.HIVE.getFactor());
    return config;
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions datasetRetrievalOptions) throws ConnectorException {
    // Create the PartitionChunkListing
    SourceMetadata sourceMetadata = (SourceMetadata) plugin;
    EntityPath entityPath = MetadataObjectsUtils.toEntityPath(tableNSKey);
    Optional<DatasetHandle> datasetHandle = sourceMetadata.getDatasetHandle((entityPath));
    if (!datasetHandle.isPresent()) { // dataset is not in the source
      throw new DatasetNotFoundException(entityPath);
    }
    handle = datasetHandle.get();
    this.tableNSKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());
    return sourceMetadata.listPartitionChunks(handle, datasetRetrievalOptions.asListPartitionChunkOptions(datasetConfig));
  }

  @Override
  public void setupMetadataForPlanning(PartitionChunkListing partitionChunkListing, DatasetRetrievalOptions options) throws ConnectorException, InvalidProtocolBufferException {
    List<PartitionChunkMetadata> chunkMetadata = convertToPartitionChunkMetadata(partitionChunkListing, datasetConfig);

    // For plugins like Hive, Aws glue we get dataset metadata like partition spec, partition values, table schema
    // in the coordinator, whereas for FileSystemPlugins metadata we send the existing metadata present in Kvstore.
    DatasetMetadata datasetMetadata = ((SourceMetadata) plugin).getDatasetMetadata(handle, partitionChunkListing, options.asGetMetadataOptions(datasetConfig));
    HiveDatasetMetadata hiveDatasetMetadata = datasetMetadata.unwrap(HiveDatasetMetadata.class);

    // Override datasetConfig from DatasetMetadata except manifestScanStats
    ScanStats manifestScanStats = datasetConfig.getReadDefinition().getManifestScanStats();
    MetadataObjectsUtils.overrideExtended(datasetConfig, datasetMetadata, Optional.ofNullable(datasetConfig.getReadDefinition().getReadSignature()), datasetConfig.getReadDefinition().getScanStats().getRecordCount(), options.maxMetadataLeafColumns());
    if (manifestScanStats != null) {
      datasetConfig.getReadDefinition().setManifestScanStats(manifestScanStats);
    }

    // Set metadata information from HiveDatasetMetadata
    tableSchema = new BatchSchema(datasetMetadata.getRecordSchema().getFields());
    partitionCols = datasetMetadata.getPartitionColumns();
    readSignatureEnabled = plugin.supportReadSignature(datasetMetadata, isFileDataset);
    datasetFileType = HiveMetadataUtils.getFileTypeFromInputFormat(hiveDatasetMetadata.getMetadataAccumulator().getCurrentInputFormat());
    tableRootPath = hiveDatasetMetadata.getMetadataAccumulator().getTableLocation();

    SplitsPointer splitsPointer = MaterializedSplitsPointer.of(0, chunkMetadata, chunkMetadata.size());
    refreshExecTableMetadata = new RefreshExecTableMetadata(storagePluginId, datasetConfig, userName, splitsPointer, tableSchema,
      null);
    final NamespaceTable nsTable = new NamespaceTable(refreshExecTableMetadata, true);
    final DremioCatalogReader catalogReader = config.getConverter().getUserQuerySqlValidatorAndToRelContextBuilderFactory().builder()
      .build().getDremioCatalogReader();
    this.table = new DremioPrepareTable(catalogReader, JavaTypeFactoryImpl.INSTANCE, nsTable);
  }

  protected int getPartitionCount() {
    return refreshExecTableMetadata.getSplitCount();
  }

  @Override
  public double getRowCountEstimates(String type) {
    double baseRowCount = datasetConfig.getReadDefinition().getManifestScanStats().getRecordCount();

    //Factor by which to overestimate footerRead row count. In practice have seen that footer read is
    //32 times slower than dirList. Kept the factor as 50 for safety now.
    double threadFactor = config.getContext().getOptions().getOption(PlannerSettings.FOOTER_READING_DIRLIST_RATIO);

    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster);
    double sliceTarget = plannerSettings.getSliceTarget();

    int partitionCount = getPartitionCount();

    switch (type) {
      case "DirList":
        baseRowCount =   partitionCount * sliceTarget;
        break;
      case "FooterReadTableFunction":
        //Number of threads should be thread factor times that of dirList
        baseRowCount =   partitionCount * threadFactor * sliceTarget;
        break;
    }


    return Math.max(baseRowCount, 1);
  }

  @Override
  public boolean updateDatasetConfigWithIcebergMetadataIfNecessary() {
    return false;
  }
}
