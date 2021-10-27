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

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.RefreshDatasetValidator;
import com.dremio.exec.planner.sql.handlers.refresh.SupportPartialRefresh;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingInvocationPrel;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class HiveIncrementalRefreshDatasetPlanBuilder extends HiveFullRefreshDatasetPlanBuilder implements SupportPartialRefresh {

  private static final Logger logger = LoggerFactory.getLogger(HiveIncrementalRefreshDatasetPlanBuilder.class);
  private final boolean isPartialRefresh;

  public HiveIncrementalRefreshDatasetPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlNode, UnlimitedSplitsMetadataProvider metadataProvider){
    super(config, sqlNode, metadataProvider);
    logger.debug("Doing a hive incremental refresh on dataset. Dataset's full path is {}", datasetPath);
    icebergCommandType = IcebergCommandType.INCREMENTAL_METADATA_REFRESH;
    isPartialRefresh = sqlNode.isPartialRefresh();
  }

  @Override
  protected DatasetConfig setupDatasetConfig() {
    DatasetConfig datasetConfig =  metadataProvider.getDatasetConfig();
    return datasetConfig;
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions datasetRetrievalOptions) throws ConnectorException {
    if (isPartialRefresh) {
      RefreshDatasetValidator validator = new RefreshDatasetValidator(metadataProvider);
      validator.validate(sqlNode);
    }
    return super.listPartitionChunks(datasetRetrievalOptions);
  }

  @Override
  public Prel getDirListToFooterReadExchange(Prel child) {
    return getHashToRandomExchangePrel(child);
  }

  @Override
  public void setupMetadataForPlanning(PartitionChunkListing partitionChunkListing, DatasetRetrievalOptions retrievalOptions) throws ConnectorException, InvalidProtocolBufferException {
    super.setupMetadataForPlanning(partitionChunkListing, retrievalOptions);
    validateMetadata();
  }

  @Override
  public Prel getDataFileListingPrel() {
    final FileSystemPlugin<?> metaStoragePlugin = config.getContext().getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
    List<String> paths = isPartialRefresh ? generatePathsForPartialRefresh() : Collections.emptyList();

    return new DirListingInvocationPrel(cluster, cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL),
      table, storagePluginId, refreshExecTableMetadata,
      1.0d, metaStoragePlugin, metadataProvider.getTableUUId(), isPartialRefresh, metadataProvider, paths, x -> getRowCountEstimates("DirList"));
  }

  private void validateMetadata() {
    boolean isValid = validatePartitionSpecEvolution(tableSchema, new ArrayList<>(partitionCols), metadataProvider.getTableSchema(), new ArrayList<>(metadataProvider.getPartitionColumns()));
    if (!isValid) {
      throw UserException.validationError().message("Change in Hive partition definition detected for table '%s', " +
        "re-promote the table to update the partition definition.", tableNSKey.toString()).build(logger);
    }
  }

  private static boolean validatePartitionSpecEvolution(BatchSchema schema, List<String> partitionCols, BatchSchema oldSchema, List<String> oldPartitionCols) {
    Collections.sort(partitionCols);
    Collections.sort(oldPartitionCols);

    if (!partitionCols.equals(oldPartitionCols)) {
      return false;
    }
    else {
      for (String partition : partitionCols) {
        Field partitionField = schema.findField(partition);
        Field oldPartitionField = oldSchema.findField(partition);
        if (!partitionField.equals(oldPartitionField)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public List<String> generatePathsForPartialRefresh() {
    Preconditions.checkNotNull(refreshExecTableMetadata);

    List<String> partitionPaths = new ArrayList<>();
    Iterator<PartitionChunkMetadata> iter = refreshExecTableMetadata.getSplits();
    while (iter.hasNext()) {
      PartitionChunkMetadata split = iter.next();
      for (PartitionProtobuf.DatasetSplit datasetSplit : split.getDatasetSplits()) {
        try {
          DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.parseFrom(datasetSplit.getSplitExtendedProperty().toByteArray());
          partitionPaths.add(dirListInputSplit.getOperatingPath());
        }
        catch (InvalidProtocolBufferException e) {
          throw UserException.parseError(e).buildSilently();
        }
      }
    }
    return partitionPaths;
  }

  @Override
  public boolean updateDatasetConfigWithIcebergMetadataIfNecessary() {
    return repairAndSaveDatasetConfigIfNecessary();
  }
}
