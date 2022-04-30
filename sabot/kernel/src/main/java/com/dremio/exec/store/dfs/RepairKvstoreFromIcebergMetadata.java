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

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableLoader;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteStringUtil;

/**
 * Class responsible for checking if iceberg metadata and dataset config are not in sync. If found that they are not
 * in sync this will issue try to repair the dataset config by bring it in sync with iceberg metadata.
 *
 * After repair INVALID_DATASET_METADATA exception is thrown which results in reattempting the original query.
 */
public class RepairKvstoreFromIcebergMetadata {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepairKvstoreFromIcebergMetadata.class);

  private final int MAX_REPAIR_ATTEMPTS = 3;
  private DatasetConfig datasetConfig;
  private final FileSystemPlugin<?> metaStoragePlugin;
  private final StoragePlugin storagePlugin;
  private final NamespaceService namespaceService;

  private IcebergMetadata oldIcebergMetadata;
  private IcebergModel icebergModel;
  private Table currentIcebergTable;
  private String currentRootPointerFileLocation;
  private Snapshot currentIcebergSnapshot;

  public RepairKvstoreFromIcebergMetadata(DatasetConfig datasetConfig, FileSystemPlugin<?> metaStoragePlugin, NamespaceService namespaceService, StoragePlugin storagePlugin) {
    this.datasetConfig = datasetConfig;
    this.metaStoragePlugin = metaStoragePlugin;
    this.namespaceService = namespaceService;
    this.storagePlugin = storagePlugin;
  }

  public boolean checkAndRepairDatasetWithQueryRetry() {
    return perfromCheckAndRepairDataset(true);
  }

  public boolean checkAndRepairDatasetWithoutQueryRetry() {
    return perfromCheckAndRepairDataset(false);
  }

  private boolean perfromCheckAndRepairDataset(boolean retryQuery) {
    Retryer<Boolean> retryer = new Retryer.Builder()
      .setMaxRetries(MAX_REPAIR_ATTEMPTS)
      .retryOnExceptionFunc(
        ex ->
          ex instanceof ConcurrentModificationException ||
            (ex instanceof UserException &&
                ((UserException)ex).getErrorType() == UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION)
      )
      .build();

    return retryer.call(() -> {
      if(isRepairNeeded()) {
        performRepair(retryQuery);
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    });
  }

  private boolean isRepairNeeded() {
    if(!DatasetHelper.isInternalIcebergTable(datasetConfig)) {
      return false;
    }

    oldIcebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    icebergModel = metaStoragePlugin.getIcebergModel(metaStoragePlugin.getSystemUserFS());
    Path icebergTableRootFolder = Path.of(metaStoragePlugin.getConfig().getPath().toString()).resolve(oldIcebergMetadata.getTableUuid());
    final IcebergTableLoader icebergTableLoader = icebergModel.getIcebergTableLoader(icebergModel.getTableIdentifier(icebergTableRootFolder.toString()));
    currentIcebergTable = icebergTableLoader.getIcebergTable();
    currentIcebergSnapshot = currentIcebergTable.currentSnapshot();
    currentRootPointerFileLocation = ((BaseTable) currentIcebergTable).operations().current().metadataFileLocation();

    if (oldIcebergMetadata.getMetadataFileLocation().equals(currentRootPointerFileLocation)) {
      logger.debug("DatasetConfig of table {} in catalog is up to date with Iceberg metadata.", datasetConfig.getFullPathList());
      return false;
    }

    return true;
  }

  private void performRepair(boolean retryQuery) throws NamespaceException, ConnectorException, InvalidProtocolBufferException {
    logger.info("DatasetConfig of table {} in catalog is not up to date with Iceberg metadata." +
        "Current iceberg table version [Snapshot ID: {}, RootMetadataFile: {}], version in catalog [Snapshot ID: {}, RootMetadataFile: {}]. " +
        "Tyring to restore catalog metadata from iceberg metadata..", datasetConfig.getFullPathList(), currentIcebergSnapshot.snapshotId(), currentRootPointerFileLocation,
      oldIcebergMetadata.getSnapshotId(), oldIcebergMetadata.getMetadataFileLocation());

    repairSchema();
    repairStats();
    repairDroppedAndModifiedColumns();
    repairReadSignature();

    // update iceberg metadata
    com.dremio.service.namespace.dataset.proto.IcebergMetadata newIcebergMetadata =
      new com.dremio.service.namespace.dataset.proto.IcebergMetadata();
    newIcebergMetadata.setMetadataFileLocation(currentRootPointerFileLocation);
    newIcebergMetadata.setSnapshotId(currentIcebergSnapshot.snapshotId());
    newIcebergMetadata.setTableUuid(oldIcebergMetadata.getTableUuid());
    byte[] specs = IcebergSerDe.serializePartitionSpecMap(currentIcebergTable.specs());
    newIcebergMetadata.setPartitionSpecs(ByteStringUtil.wrap(specs));
    String oldPartitionStatsFile = oldIcebergMetadata.getPartitionStatsFile();
    if (oldPartitionStatsFile != null) {
      String partitionStatsFile = IcebergUtils.getPartitionStatsFile(currentRootPointerFileLocation, currentIcebergSnapshot.snapshotId(), metaStoragePlugin.getFsConfCopy(), metaStoragePlugin);
      if (partitionStatsFile != null) {
        newIcebergMetadata.setPartitionStatsFile(partitionStatsFile);
      }
    }

    datasetConfig.getPhysicalDataset().setIcebergMetadata(newIcebergMetadata);
    try {
      namespaceService.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig);
      String message = String.format("Repair operation was successful for dataset %s. " +
        "Iceberg and KV store are both now in sync. Issuing automatic retry of the query", String.join(".", datasetConfig.getFullPathList()));
      logger.debug(message);
      if (retryQuery) {
        throw UserException.invalidMetadataError().addContext(message).build(logger);
      }
    } catch (ConcurrentModificationException e) {
      //reload the dataset config in case of concurrent modification exception
      datasetConfig = namespaceService.getDataset(new NamespaceKey(datasetConfig.getFullPathList()));
      throw e;
    }
  }

  private void repairSchema() {
    BatchSchema newSchemaFromIceberg = new SchemaConverter().fromIceberg(currentIcebergTable.schema());
    datasetConfig.setRecordSchema(ByteStringUtil.wrap(newSchemaFromIceberg.toByteArray()));
    logger.info("Repairing schema. Current KV store schema {}. Schema from iceberg table {}", BatchSchema.deserialize(datasetConfig.getRecordSchema()).toString(), newSchemaFromIceberg);
  }

  private void repairStats() {
    long numRecords = Long.parseLong(currentIcebergSnapshot.summary().getOrDefault("total-records", "0"));
    datasetConfig.getReadDefinition().getScanStats().setRecordCount(numRecords);
    long numDataFiles = Long.parseLong(currentIcebergSnapshot.summary().getOrDefault("total-data-files", "0"));
    datasetConfig.getReadDefinition().getManifestScanStats().setRecordCount(numDataFiles);
    logger.info("Repaired Stats. total-records from iceberg table {} and total-data-files from iceberg table {}", numRecords, numDataFiles);
  }

  private void repairDroppedAndModifiedColumns() {
    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    BatchSchema modifiedColumns, droppedColumns;
    try {
      modifiedColumns = mapper.readValue(currentIcebergTable.properties().getOrDefault(ColumnOperations.DREMIO_UPDATE_COLUMNS, BatchSchema.EMPTY.toJson()), BatchSchema.class);
      droppedColumns = mapper.readValue(currentIcebergTable.properties().getOrDefault(ColumnOperations.DREMIO_DROPPED_COLUMNS, BatchSchema.EMPTY.toJson()), BatchSchema.class);
    } catch (JsonProcessingException e) {
      String message = "Unable to retrieve dropped and modified columns from iceberg" + e.getMessage();
      logger.error(message);
      throw UserException.dataReadError().addContext(message).build(logger);
    }

    UserDefinedSchemaSettings settings = new UserDefinedSchemaSettings();
    settings.setDroppedColumns(droppedColumns.toByteString());
    settings.setModifiedColumns(modifiedColumns.toByteString());
    if(datasetConfig.getPhysicalDataset().getInternalSchemaSettings() != null) {
      settings.setSchemaLearningEnabled(datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getSchemaLearningEnabled());
    }
    logger.info("Repaired Dropped and modified columns. Modified Columns {}. Dropped Columns {}", modifiedColumns, droppedColumns);
  }

  private void repairReadSignature() throws ConnectorException, InvalidProtocolBufferException {
    if (datasetConfig.getReadDefinition().getReadSignature() != null &&
      !datasetConfig.getReadDefinition().getReadSignature().isEmpty()) {
      List<String> datasetPath = ((SupportsInternalIcebergTable) storagePlugin).resolveTableNameToValidPath(datasetConfig.getFullPathList());

      datasetConfig.getReadDefinition().setReadSignature(ByteStringUtil.wrap(
        createReadSignatureFromPartitionStatsFiles(FileSelection.getPathBasedOnFullPath(datasetPath).toString(), currentIcebergTable)));
    }
  }

  private byte[] createReadSignatureFromPartitionStatsFiles(String dataTableRootFolder, Table table) throws ConnectorException, InvalidProtocolBufferException {
    PartitionSpec spec = table.spec();

    Set<IcebergPartitionData> icebergPartitionDataSet = new HashSet<>();
    if (!spec.isUnpartitioned()) {
      PartitionStatsReader partitionStatsReader;
      final String partitionStatsFile = table.currentSnapshot().partitionStatsMetadata().partitionStatsFiles().getFileForSpecId(spec.specId());

      logger.info("Restoring read signature of table {} from partition stats file {} of snapshot {}", datasetConfig.getFullPathList(),
        partitionStatsFile, currentIcebergSnapshot.snapshotId());

      final InputFile inputFile = new DremioFileIO(metaStoragePlugin.getFsConfCopy(), metaStoragePlugin).newInputFile(partitionStatsFile);
      partitionStatsReader = new PartitionStatsReader(inputFile, spec);

      Streams.stream(partitionStatsReader)
        .map(partitionStatsEntry -> IcebergPartitionData.fromStructLike(spec, partitionStatsEntry.getPartition()))
        .forEach(icebergPartitionDataSet::add);
    }

    SupportsInternalIcebergTable plugin = ((SupportsInternalIcebergTable) storagePlugin);

    List<String> partitionPaths = new ArrayList<String>();
    if(plugin.canGetDatasetMetadataInCoordinator()) {
      partitionPaths = generatePartitionPathsForHiveDataset(plugin);
    }

    // creating full read signature provider
    ReadSignatureProvider readSignatureProvider = ((SupportsInternalIcebergTable) storagePlugin).createReadSignatureProvider(null,
      dataTableRootFolder,
      0,
      partitionPaths,
      ipd -> true, // unused
      true, false);

    return readSignatureProvider.compute(
      icebergPartitionDataSet, // all partitions
      Collections.emptySet()).toByteArray();
  }

  private List<String> generatePartitionPathsForHiveDataset(SupportsInternalIcebergTable plugin) throws ConnectorException, InvalidProtocolBufferException {
    List<String> partitionPaths = new ArrayList<>();
    SourceMetadata sourceMetadata = (SourceMetadata) plugin;
    EntityPath entityPath = MetadataObjectsUtils.toEntityPath(new NamespaceKey(datasetConfig.getFullPathList()));
    Optional<DatasetHandle> datasetHandle = sourceMetadata.getDatasetHandle((entityPath));
    if (!datasetHandle.isPresent()) { // dataset is not in the source
      throw new DatasetNotFoundException(entityPath);
    }

    DatasetRetrievalOptions optionsBuilder = DatasetRetrievalOptions.DEFAULT.toBuilder().setRefreshDataset(true).setPartition(Collections.emptyMap()).build();

    PartitionChunkListing listing = sourceMetadata.listPartitionChunks(datasetHandle.get(), optionsBuilder.asListPartitionChunkOptions(datasetConfig));

    for (Iterator<? extends PartitionChunk> it = listing.iterator(); it.hasNext(); ) {
      PartitionChunk chunk = it.next();
      BytesOutput output = chunk.getSplits().iterator().next().getExtraInfo();

      DirListInputSplitProto.DirListInputSplit dirListInputSplit  = LegacyProtobufSerializer.parseFrom(DirListInputSplitProto.DirListInputSplit.PARSER, MetadataProtoUtils.toProtobuf(output));
      partitionPaths.add(dirListInputSplit.getOperatingPath());
    }

    return partitionPaths;
  }
}
