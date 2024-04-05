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
package com.dremio.exec.store.deltalake;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.options.Options;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The this class is responsible for all pre-planning scans on the DeltaTable metadata. It
 * selectively gathers information, that leads to the dataset size, the relevant commit files and
 * the schema
 */
@Options
public class DeltaLakeFormatDatasetAccessor implements FileDatasetHandle {
  private static final Logger logger =
      LoggerFactory.getLogger(DeltaLakeFormatDatasetAccessor.class);
  private final DatasetType type;
  private final NamespaceKey tableSchemaPath;
  private final FileSystem fs;
  private final FileSystemPlugin fsPlugin;
  private final FileSelection fileSelection;
  private final DeltaLakeFormatPlugin formatPlugin;
  private final TimeTravelOption.TimeTravelRequest travelRequest;
  private volatile DeltaLakeTable deltaTable;

  public DeltaLakeFormatDatasetAccessor(
      DatasetType type,
      FileSystem fs,
      FileSystemPlugin fsPlugin,
      FileSelection fileSelection,
      NamespaceKey tableSchemaPath,
      DeltaLakeFormatPlugin formatPlugin,
      TimeTravelOption.TimeTravelRequest travelRequest) {

    this.type = type;
    this.fsPlugin = fsPlugin;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.tableSchemaPath = tableSchemaPath;
    this.formatPlugin = formatPlugin;
    this.travelRequest = travelRequest;
  }

  private void initializeDeltaTableWrapper() throws Exception {
    if (deltaTable == null) {
      synchronized (this) {
        if (deltaTable == null) {
          final SabotContext context = formatPlugin.getContext();
          try (BufferAllocator sampleAllocator =
                  context
                      .getAllocator()
                      .newChildAllocator("delta-lake-metadata-alloc", 0, Long.MAX_VALUE);
              OperatorContextImpl operatorContext =
                  new OperatorContextImpl(
                      context.getConfig(),
                      context.getDremioConfig(),
                      sampleAllocator,
                      context.getOptionManager(),
                      1000,
                      context.getExpressionSplitCache()); ) {
            final FileSystem tableFileSystem =
                fsPlugin.createFS(SYSTEM_USERNAME, operatorContext, true);
            this.deltaTable =
                new DeltaLakeTable(context, tableFileSystem, fileSelection, travelRequest);
          }
        }
      }
    }
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options)
      throws ConnectorException {
    try {
      initializeDeltaTableWrapper();
      final DeltaLogSnapshot snapshot = deltaTable.getConsolidatedSnapshot();

      return new FileConfigMetadata() {

        @Override
        public DatasetStats getDatasetStats() {
          return DatasetStats.of(snapshot.getNetOutputRows(), ScanCostFactor.PARQUET.getFactor());
        }

        @Override
        public DatasetStats getManifestStats() {
          return DatasetStats.of(snapshot.getDataFileEntryCount(), ScanCostFactor.EASY.getFactor());
        }

        @Override
        public Schema getRecordSchema() {
          try {
            Preconditions.checkNotNull(snapshot, "Unable to read commit snapshot");
            boolean mapDataTypeEnabled =
                formatPlugin
                    .getContext()
                    .getOptionManager()
                    .getOption(ExecConstants.ENABLE_MAP_DATA_TYPE);
            boolean columnMappingEnabled =
                formatPlugin
                    .getContext()
                    .getOptionManager()
                    .getOption(ExecConstants.ENABLE_DELTALAKE_COLUMN_MAPPING);
            return DeltaLakeSchemaConverter.newBuilder()
                .withMapEnabled(mapDataTypeEnabled)
                .withColumnMapping(columnMappingEnabled, snapshot.getColumnMappingMode())
                .build()
                .fromSchemaString(snapshot.getSchema());
          } catch (IOException e) {
            logger.error("Error while parsing DeltaLake schema", e);
            throw new RuntimeException(e);
          }
        }

        @Override
        public List<String> getPartitionColumns() {
          return snapshot.getPartitionColumns();
        }

        @Override
        public List<String> getSortColumns() {
          return new ArrayList<>(); // TODO: Implement
        }

        @Override
        public BytesOutput getExtraInfo() {
          final DeltaLakeProtobuf.DeltaLakeDatasetXAttr xAttr = deltaTable.buildDatasetXattr();
          return xAttr::writeTo;
        }

        @Override
        public FileConfig getFileConfig() {
          return PhysicalDatasetUtils.toFileFormat(formatPlugin)
              .asFileConfig()
              .setLocation(fileSelection.getSelectionRoot());
        }
      };
    } catch (Exception e) {
      logger.error("Error while scanning DeltaLake table", e);
      throw new ConnectorException(e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options)
      throws ConnectorException {
    try {
      initializeDeltaTableWrapper();
      final List<PartitionValue> partitions = Collections.emptyList();
      final PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
      deltaTable
          .getAllSplits()
          .forEach(datasetSplit -> partitionChunkListing.put(partitions, datasetSplit));
      return partitionChunkListing;
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    try {
      return deltaTable.readSignature();
    } catch (IOException e) {
      throw new ConnectorException(e);
    }
  }

  @Override
  public boolean metadataValid(
      BytesOutput readSignature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      FileSystem fileSystem) {
    // if readSignature is NOT existing already, treat metadata as stale
    if (readSignature == BytesOutput.NONE) {
      return false;
    }
    try {
      final DeltaLakeProtobuf.DeltaLakeReadSignature deltaLakeReadSignature =
          LegacyProtobufSerializer.parseFrom(
              DeltaLakeProtobuf.DeltaLakeReadSignature.PARSER,
              MetadataProtoUtils.toProtobuf(readSignature));
      initializeDeltaTableWrapper();
      return !deltaTable.checkMetadataStale(deltaLakeReadSignature);
    } catch (Exception e) {
      // Do a refresh in case of exception
      logger.error(
          "Exception occurred while trying to determine delta dataset metadata validity. Dataset path {}. ",
          fileSelection.toString(),
          e);
      return false;
    }
  }
}
