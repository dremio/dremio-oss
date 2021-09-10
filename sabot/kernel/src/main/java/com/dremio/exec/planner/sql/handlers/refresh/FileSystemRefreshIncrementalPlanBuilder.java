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
package com.dremio.exec.planner.sql.handlers.refresh;

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;
import static com.dremio.io.file.Path.SEPARATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingInvocationPrel;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Builds plan for filesystems in case of incremental and partial refresh.
 */
public class FileSystemRefreshIncrementalPlanBuilder extends FileSystemFullRefreshPlanBuilder implements SupportPartialRefresh {

  private final boolean isPartialRefresh;
  private PartitionChunk inputPartitionChunk;

  public FileSystemRefreshIncrementalPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlNode, UnlimitedSplitsMetadataProvider metadataProvider) {
    super(config, sqlNode, metadataProvider);
    logger.debug("Doing a filesystem incremental refresh on dataset. Dataset's full path is {}", datasetPath);
    isPartialRefresh = sqlNode.isPartialRefresh();
    icebergCommandType = IcebergCommandType.INCREMENTAL_METADATA_REFRESH;
  }

  @Override
  protected DatasetConfig setupDatasetConfig() {
    datasetFileType = FileType.PARQUET;
    //TODO: currently in the final table we write iceberg as the format of the dataset but we expect
    // all operators to have a parquet as the file type. So forcefully setting file type as parquet.
    DatasetConfig datasetConfig =  metadataProvider.getDatasetConfig();
    datasetConfig.getPhysicalDataset().getFormatSettings().setType(FileType.PARQUET);
    checkAndUpdateIsFileDataset();
    return datasetConfig;
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions datasetRetrievalOptions) throws ConnectorException {
    DirListInputSplitProto.DirListInputSplit dirListInputSplit;

    if (isPartialRefresh) {
      List<String> operatingPath = generatePathsForPartialRefresh();
      dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
        .setRootPath(datasetPath.toString())
        .setOperatingPath(operatingPath.get(0))
        .setReadSignature(Long.MAX_VALUE).setIsFile(super.isFileDataset)
        .build();
    } else {
      dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
        .setRootPath(datasetPath.toString())
        .setOperatingPath(datasetPath.toString())
        .setReadSignature(Long.MAX_VALUE).setIsFile(super.isFileDataset)
        .build();
    }
    DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(Collections.emptyList(), split);
    partitionChunkListing.computePartitionChunks();
    return partitionChunkListing;
  }

  @Override
  public Prel getDataFileListingPrel() {
    final FileSystemPlugin<?> metaStoragePlugin = config.getContext().getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
    List<String> paths = isPartialRefresh ? generatePathsForPartialRefresh() : Collections.emptyList();

    return new DirListingInvocationPrel(cluster, cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL),
      table, storagePluginId, refreshExecTableMetadata,
      1.0d, metaStoragePlugin, metadataProvider.getTableUUId(), isPartialRefresh, metadataProvider, paths, x -> getRowCountEstimates("DirList"));
  }

  public Prel getDirListToFooterReadExchange(Prel child) {
    return getHashToRandomExchangePrel(child);
  }

  @Override
  protected void checkAndUpdateIsFileDataset() {
    isFileDataset = DatasetType.PHYSICAL_DATASET_SOURCE_FILE.equals(metadataProvider.getDatasetConfig().getType());
  }

  public List<String> generatePathsForPartialRefresh() {
    RefreshDatasetValidator validator = new FileSystemPartitionValidator(metadataProvider);
    validator.validate(sqlNode);

    //Validated input partition chunks
    inputPartitionChunk = PartitionChunk.of(
      validator.getPartitionValues(),
      /* TODO */ ImmutableList.of(), BytesOutput.NONE);

    List<PartitionValue> partitionValues = inputPartitionChunk.getPartitionValues();

    List<String> partitionPaths = new ArrayList<>();
    partitionPaths.addAll(Arrays.asList(datasetPath.toString().split(SEPARATOR)));

    partitionValues.stream().forEach(x -> {
      String partition_path = ((PartitionValue.StringPartitionValue) x).getValue();
      partitionPaths.add(partition_path);
    });

    return Collections.singletonList(FileSelection.getPathBasedOnFullPath(partitionPaths).toString());
  }

  @Override
  public double getRowCountEstimates(String type) {
    Preconditions.checkState(datasetConfig != null, "Unexpected state");
    Preconditions.checkState(datasetConfig.getReadDefinition() != null, "Unexpected state");
    Preconditions.checkState(datasetConfig.getReadDefinition().getManifestScanStats() != null, "Unexpected state");
    double baseRowCount = datasetConfig.getReadDefinition().getManifestScanStats().getRecordCount();
    double increaseFactor = config.getContext().getOptions().getOption(PlannerSettings.METADATA_REFRESH_INCREASE_FACTOR);

    //Factor by which to overestimate footerRead row count. In practice have seen that footer read is
    //32 times slower than dirList. Kept the factor as 50 for safety now.
    double threadFactor = config.getContext().getOptions().getOption(PlannerSettings.FOOTER_READING_DIRLIST_RATIO);
    double minFilesChanged = config.getContext().getOptions().getOption(PlannerSettings.MIN_FILES_CHANGED_DURING_REFRESH);

    double addedFilesAfterLastRefresh = Math.max(baseRowCount * increaseFactor, minFilesChanged);

    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster);
    double sliceTarget = plannerSettings.getSliceTarget();

    switch (type) {
      case "DirList":
        baseRowCount = baseRowCount + addedFilesAfterLastRefresh;
        break;
      case "FooterReadTableFunction":
        //Number of threads should be thread factor times that of dirList
        baseRowCount = 1 * threadFactor * sliceTarget;
        break;
    }

    return Math.max(baseRowCount, 1);
  }

  @Override
  public boolean updateDatasetConfigWithIcebergMetadataIfNecessary() {
    return repairAndSaveDatasetConfigIfNecessary();
  }

}
