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

import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;

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
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.iceberg.model.IcebergTableLoader;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieTableIdentifier;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieTableOperations;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.io.file.DistStorageMetadataPathRewritingFileSystem;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PrimaryKey;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteStringUtil;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.projectnessie.client.api.NessieApiV2;

/**
 * Class responsible for checking if iceberg metadata and dataset config are not in sync. If found
 * that they are not in sync this will issue try to repair the dataset config by bring it in sync
 * with iceberg metadata.
 *
 * <p>After repair INVALID_DATASET_METADATA exception is thrown which results in reattempting the
 * original query.
 */
public class RepairKvstoreFromIcebergMetadata {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RepairKvstoreFromIcebergMetadata.class);

  private static final int MAX_REPAIR_ATTEMPTS = 3;

  private final boolean isMapDataTypeEnabled;
  private DatasetConfig datasetConfig;
  private final FileSystemPlugin<?> metaStoragePlugin;
  private final StoragePlugin storagePlugin;
  private final OptionManager optionManager;
  private final NamespaceService namespaceService;

  private IcebergMetadata oldIcebergMetadata;
  private IcebergModel icebergModel;
  private Table currentIcebergTable;
  private String currentRootPointerFileLocation;
  private Snapshot currentIcebergSnapshot;

  public RepairKvstoreFromIcebergMetadata(
      DatasetConfig datasetConfig,
      FileSystemPlugin<?> metaStoragePlugin,
      NamespaceService namespaceService,
      StoragePlugin storagePlugin,
      OptionManager optionManager) {
    this.datasetConfig = datasetConfig;
    this.metaStoragePlugin = metaStoragePlugin;
    this.namespaceService = namespaceService;
    this.storagePlugin = storagePlugin;
    this.optionManager = optionManager;
    this.isMapDataTypeEnabled = optionManager.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE);
  }

  @WithSpan
  public boolean checkAndRepairDatasetWithQueryRetry() {
    return performCheckAndRepairDataset(true);
  }

  @WithSpan
  public boolean checkAndRepairDatasetWithoutQueryRetry() {
    return performCheckAndRepairDataset(false);
  }

  @WithSpan
  private boolean performCheckAndRepairDataset(boolean retryQuery) {
    Retryer retryer =
        Retryer.newBuilder()
            .setMaxRetries(MAX_REPAIR_ATTEMPTS)
            .retryOnExceptionFunc(
                ex ->
                    ex instanceof ConcurrentModificationException
                        || (ex instanceof UserException
                            && ((UserException) ex).getErrorType()
                                == UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION))
            .build();

    return retryer.call(
        () -> {
          if (isRepairNeeded()) {
            performRepair(retryQuery);
            return Boolean.TRUE;
          }
          return Boolean.FALSE;
        });
  }

  @WithSpan
  private boolean isRepairNeeded() throws IOException {
    if (!DatasetHelper.isInternalIcebergTable(datasetConfig)) {
      return false;
    }

    oldIcebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    icebergModel = metaStoragePlugin.getIcebergModel();
    Path icebergTableRootFolder = IcebergUtils.getPreviousTableMetadataRoot(oldIcebergMetadata);
    IcebergTableIdentifier icebergTableIdentifier =
        icebergModel.getTableIdentifier(icebergTableRootFolder.toString());

    repairMetadataDesyncIfNecessary(icebergTableIdentifier);

    final IcebergTableLoader icebergTableLoader =
        icebergModel.getIcebergTableLoader(icebergTableIdentifier);
    currentIcebergTable = icebergTableLoader.getIcebergTable();
    currentIcebergSnapshot = currentIcebergTable.currentSnapshot();
    currentRootPointerFileLocation =
        ((BaseTable) currentIcebergTable).operations().current().metadataFileLocation();

    if (oldIcebergMetadata.getMetadataFileLocation().equals(currentRootPointerFileLocation)) {
      logger.debug(
          "DatasetConfig of table {} in catalog is up to date with Iceberg metadata.",
          datasetConfig.getFullPathList());
      return false;
    }

    return true;
  }

  @WithSpan
  private void performRepair(boolean retryQuery)
      throws NamespaceException, ConnectorException, InvalidProtocolBufferException {
    logger.info(
        "DatasetConfig of table {} in catalog is not up to date with Iceberg metadata."
            + "Current iceberg table version [Snapshot ID: {}, RootMetadataFile: {}], version in catalog [Snapshot ID: {}, RootMetadataFile: {}]. "
            + "Tyring to restore catalog metadata from iceberg metadata..",
        datasetConfig.getFullPathList(),
        currentIcebergSnapshot.snapshotId(),
        currentRootPointerFileLocation,
        oldIcebergMetadata.getSnapshotId(),
        oldIcebergMetadata.getMetadataFileLocation());

    String oldPartitionStatsFile = oldIcebergMetadata.getPartitionStatsFile();
    ImmutableDremioFileAttrs newPartitionStatsFileAttrs = null;
    if (oldPartitionStatsFile != null) {
      newPartitionStatsFileAttrs =
          IcebergUtils.getPartitionStatsFileAttrs(
              currentRootPointerFileLocation,
              currentIcebergSnapshot.snapshotId(),
              currentIcebergTable.io());
    }

    repairSchema();
    repairStats();
    repairDroppedAndModifiedColumns();
    String newPartitionStatsFileName =
        newPartitionStatsFileAttrs == null ? null : newPartitionStatsFileAttrs.fileName();
    repairReadSignature(newPartitionStatsFileName);
    repairPrimaryKeys();

    // update iceberg metadata
    com.dremio.service.namespace.dataset.proto.IcebergMetadata newIcebergMetadata =
        new com.dremio.service.namespace.dataset.proto.IcebergMetadata();
    newIcebergMetadata.setMetadataFileLocation(currentRootPointerFileLocation);
    newIcebergMetadata.setSnapshotId(currentIcebergSnapshot.snapshotId());
    newIcebergMetadata.setTableUuid(oldIcebergMetadata.getTableUuid());
    byte[] specs = IcebergSerDe.serializePartitionSpecAsJsonMap(currentIcebergTable.specs());
    newIcebergMetadata.setPartitionSpecsJsonMap(ByteStringUtil.wrap(specs));
    newIcebergMetadata.setJsonSchema(serializedSchemaAsJson(currentIcebergTable.schema()));

    if (newPartitionStatsFileName != null) {
      newIcebergMetadata.setPartitionStatsFile(newPartitionStatsFileAttrs.fileName());
      newIcebergMetadata.setPartitionStatsFileSize(newPartitionStatsFileAttrs.fileLength());
    }

    datasetConfig.getPhysicalDataset().setIcebergMetadata(newIcebergMetadata);
    try {
      namespaceService.addOrUpdateDataset(
          new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig);
      String message =
          String.format(
              "Repair operation was successful for dataset %s. "
                  + "Iceberg and KV store are both now in sync. Issuing automatic retry of the query",
              String.join(".", datasetConfig.getFullPathList()));
      logger.debug(message);
      if (retryQuery) {
        throw UserException.invalidMetadataError().addContext(message).build(logger);
      }
    } catch (ConcurrentModificationException e) {
      // reload the dataset config in case of concurrent modification exception
      datasetConfig =
          namespaceService.getDataset(new NamespaceKey(datasetConfig.getFullPathList()));
      throw e;
    }
  }

  private void repairSchema() {
    BatchSchema newSchemaFromIceberg =
        SchemaConverter.getBuilder()
            .setMapTypeEnabled(isMapDataTypeEnabled)
            .build()
            .fromIceberg(currentIcebergTable.schema());
    datasetConfig.setRecordSchema(ByteStringUtil.wrap(newSchemaFromIceberg.toByteArray()));
    logger.info(
        "Repairing schema. Current KV store schema {}. Schema from iceberg table {}",
        BatchSchema.deserialize(datasetConfig.getRecordSchema()).toString(),
        newSchemaFromIceberg);
  }

  private void repairStats() {
    Map<String, String> summary =
        Optional.ofNullable(currentIcebergSnapshot)
            .map(Snapshot::summary)
            .orElseGet(ImmutableMap::of);
    long numRecords = Long.parseLong(summary.getOrDefault("total-records", "0"));
    datasetConfig.getReadDefinition().getScanStats().setRecordCount(numRecords);
    long numDataFiles = Long.parseLong(summary.getOrDefault("total-data-files", "0"));
    datasetConfig.getReadDefinition().getManifestScanStats().setRecordCount(numDataFiles);
    logger.info(
        "Repaired Stats. total-records from iceberg table {} and total-data-files from iceberg table {}",
        numRecords,
        numDataFiles);
  }

  private void repairDroppedAndModifiedColumns() {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    BatchSchema modifiedColumns, droppedColumns;
    try {
      modifiedColumns =
          mapper.readValue(
              currentIcebergTable
                  .properties()
                  .getOrDefault(ColumnOperations.DREMIO_UPDATE_COLUMNS, BatchSchema.EMPTY.toJson()),
              BatchSchema.class);
      droppedColumns =
          mapper.readValue(
              currentIcebergTable
                  .properties()
                  .getOrDefault(
                      ColumnOperations.DREMIO_DROPPED_COLUMNS, BatchSchema.EMPTY.toJson()),
              BatchSchema.class);
    } catch (JsonProcessingException e) {
      String message = "Unable to retrieve dropped and modified columns from iceberg";
      logger.error(message, e);
      throw UserException.dataReadError(e).addContext(message).build(logger);
    }

    UserDefinedSchemaSettings settings = new UserDefinedSchemaSettings();
    settings.setDroppedColumns(droppedColumns.toByteString());
    settings.setModifiedColumns(modifiedColumns.toByteString());
    if (datasetConfig.getPhysicalDataset().getInternalSchemaSettings() != null) {
      settings.setSchemaLearningEnabled(
          datasetConfig
              .getPhysicalDataset()
              .getInternalSchemaSettings()
              .getSchemaLearningEnabled());
    }
    datasetConfig.getPhysicalDataset().setInternalSchemaSettings(settings);
    logger.info(
        "Repaired Dropped and modified columns. Modified Columns {}. Dropped Columns {}",
        modifiedColumns,
        droppedColumns);
  }

  private void repairReadSignature(String newPartitionStatsFile)
      throws ConnectorException, InvalidProtocolBufferException {
    if (datasetConfig.getReadDefinition().getReadSignature() != null
        && !datasetConfig.getReadDefinition().getReadSignature().isEmpty()) {
      List<String> datasetPath =
          ((SupportsInternalIcebergTable) storagePlugin)
              .resolveTableNameToValidPath(datasetConfig.getFullPathList());

      datasetConfig
          .getReadDefinition()
          .setReadSignature(
              ByteStringUtil.wrap(
                  createReadSignatureFromPartitionStatsFiles(
                      FileSelection.getPathBasedOnFullPath(datasetPath).toString(),
                      currentIcebergTable,
                      newPartitionStatsFile)));
    }
  }

  private void repairPrimaryKeys() {
    ObjectMapper mapper = new ObjectMapper();
    BatchSchema batchSchema;
    try {
      batchSchema =
          mapper.readValue(
              currentIcebergTable
                  .properties()
                  .getOrDefault(
                      PrimaryKeyOperations.DREMIO_PRIMARY_KEY, BatchSchema.EMPTY.toJson()),
              BatchSchema.class);
    } catch (JsonProcessingException e) {
      String error = "Unexpected error occurred while deserializing primary keys";
      logger.error(error, e);
      throw UserException.dataReadError(e).addContext(error).build(logger);
    }
    if (batchSchema.equals(BatchSchema.EMPTY)) {
      datasetConfig.getPhysicalDataset().setPrimaryKey(null);
    } else {
      datasetConfig
          .getPhysicalDataset()
          .setPrimaryKey(
              new PrimaryKey()
                  .setColumnList(
                      batchSchema.getFields().stream()
                          .map(f -> f.getName().toLowerCase(Locale.ROOT))
                          .collect(Collectors.toList())));
    }
    logger.info("Repaired primary keys {}", batchSchema);
  }

  private byte[] createReadSignatureFromPartitionStatsFiles(
      String dataTableRootFolder, Table table, String newPartitionStatsFile)
      throws ConnectorException, InvalidProtocolBufferException {
    PartitionSpec spec = table.spec();

    Set<IcebergPartitionData> icebergPartitionDataSet = new HashSet<>();
    if (!spec.isUnpartitioned() && newPartitionStatsFile != null) {
      logger.info(
          "Restoring read signature of table {} from partition stats file {} of snapshot {}",
          datasetConfig.getFullPathList(),
          newPartitionStatsFile,
          currentIcebergSnapshot.snapshotId());

      FileIO io =
          metaStoragePlugin.createIcebergFileIO(
              metaStoragePlugin.getSystemUserFS(), null, null, null, null);
      final InputFile partitionStatsInputFile = io.newInputFile(newPartitionStatsFile);
      PartitionStatsReader partitionStatsReader =
          new PartitionStatsReader(partitionStatsInputFile, spec);

      Streams.stream(partitionStatsReader)
          .map(
              partitionStatsEntry ->
                  IcebergPartitionData.fromStructLike(spec, partitionStatsEntry.getPartition()))
          .forEach(icebergPartitionDataSet::add);
    }

    SupportsInternalIcebergTable plugin = ((SupportsInternalIcebergTable) storagePlugin);

    List<String> partitionPaths = new ArrayList<String>();
    if (plugin.canGetDatasetMetadataInCoordinator()) {
      partitionPaths = generatePartitionPathsForHiveDataset(plugin);
    }

    // creating full read signature provider
    ReadSignatureProvider readSignatureProvider =
        ((SupportsInternalIcebergTable) storagePlugin)
            .createReadSignatureProvider(
                null,
                dataTableRootFolder,
                0,
                partitionPaths,
                ipd -> true, // unused
                true,
                false);

    return readSignatureProvider
        .compute(
            icebergPartitionDataSet, // all partitions
            Collections.emptySet())
        .toByteArray();
  }

  private List<String> generatePartitionPathsForHiveDataset(SupportsInternalIcebergTable plugin)
      throws ConnectorException, InvalidProtocolBufferException {
    List<String> partitionPaths = new ArrayList<>();
    SourceMetadata sourceMetadata = (SourceMetadata) plugin;
    EntityPath entityPath =
        MetadataObjectsUtils.toEntityPath(new NamespaceKey(datasetConfig.getFullPathList()));
    Optional<DatasetHandle> datasetHandle = sourceMetadata.getDatasetHandle((entityPath));
    if (!datasetHandle.isPresent()) { // dataset is not in the source
      throw new DatasetNotFoundException(entityPath);
    }

    DatasetRetrievalOptions optionsBuilder =
        DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setRefreshDataset(true)
            .setPartition(Collections.emptyMap())
            .build();

    PartitionChunkListing listing =
        sourceMetadata.listPartitionChunks(
            datasetHandle.get(), optionsBuilder.asListPartitionChunkOptions(datasetConfig));

    for (Iterator<? extends PartitionChunk> it = listing.iterator(); it.hasNext(); ) {
      PartitionChunk chunk = it.next();
      BytesOutput output = chunk.getSplits().iterator().next().getExtraInfo();

      DirListInputSplitProto.DirListInputSplit dirListInputSplit =
          LegacyProtobufSerializer.parseFrom(
              DirListInputSplitProto.DirListInputSplit.PARSER,
              MetadataProtoUtils.toProtobuf(output));
      partitionPaths.add(dirListInputSplit.getOperatingPath());
    }

    return partitionPaths;
  }

  /**
   * re-syncs the unlimited splits metadata by frontloading the last-seen metadata.json file within
   * fs dir, to the HEAD of the commit. this commit will be used at reference point to conduct the
   * repair method.
   *
   * @param icebergTableIdentifier: the icebergTableIdentifier used to locate the internal iceberg
   *     table
   */
  private void repairMetadataDesyncIfNecessary(IcebergTableIdentifier icebergTableIdentifier)
      throws IOException {
    FileSystem fs = this.metaStoragePlugin.getSystemUserFS();
    String icebergMetadataFileLocation =
        this.datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    // If feature flag ON, and the file does not exist in the fs, perform metadata re-sync
    if (metaStoragePlugin
            .getContext()
            .getOptionManager()
            .getOption(ExecConstants.ENABLE_UNLIMITED_SPLITS_DISTRIBUTED_STORAGE_RELOCATION)
        && !fs.exists(Path.of(icebergMetadataFileLocation))) {

      logger.warn(
          String.format(
              "metadata file: %s was not found within storage %s. Attempting self-heal to re-sync metadata",
              icebergMetadataFileLocation,
              ((DistStorageMetadataPathRewritingFileSystem) fs).getDistStoragePath()));
      Provider<NessieApiV2> api = ((IcebergNessieModel) icebergModel).getNessieApi();

      // Construct the internal iceberg's TableOperations
      IcebergNessieTableOperations icebergNessieTableOperations =
          new IcebergNessieTableOperations(
              null,
              api,
              metaStoragePlugin.createIcebergFileIO(fs, null, null, null, null),
              (IcebergNessieTableIdentifier) icebergTableIdentifier,
              null,
              optionManager);

      Path metadataPathLocation =
          getLatestMetastoreVersionIfPathExists(Path.of(icebergMetadataFileLocation), fs);

      // Refresh the object to populate the rest of the table operations.
      // fs.open and fs.getFileAttributes will be called during refresh. Calls to the "current
      // metadata location" will
      // be redirected to point to the latest-existing metastore version found within the fs
      // directory.
      // The table metadata returned represents the contents found by the re-written path.
      icebergNessieTableOperations.doRefreshFromPreviousCommit(metadataPathLocation.toString());
      TableMetadata LatestExistingtableMetadata = icebergNessieTableOperations.current();

      // Commit the outdated metadata to Nessie. Committing the latest-existing metadata pushes it
      // to HEAD.
      // The new commit generates a new (temporary) metadata.json file.
      // The new (temporary) commit serves as the "old metadata" reference point during the repair.
      icebergNessieTableOperations.doCommit(null, LatestExistingtableMetadata);
    }
  }

  /**
   * rewrites the iceberg metastore file path to point to the latest existing metastore file within
   * the current distributed storage's metadata directory. ... Table Metastore files are managed by
   * Nessie catalog. This method is designed to recover & frontload a previous metastore table
   * version.
   *
   * @param p: the original metadata.json file's absolute path
   * @param fs: the filesystem of the path
   * @return the absolute path of the metadata.json file with the newest version-ID within the
   *     directory
   */
  private Path getLatestMetastoreVersionIfPathExists(Path p, FileSystem fs) throws IOException {
    logger.info("requested metadata.json file not found. attempting to re-sync metadata");
    Path newDistStore = this.metaStoragePlugin.getConfig().getPath();
    String metadataGuid = p.getParent().getParent().getName();
    Path metadataRootFolder = newDistStore.resolve(metadataGuid);

    // tracker for the latest existing metastore version
    int maxVersionID = -1;
    Path maxVersionInDir = null;

    // parses through all files in  directory. Keeps track of the newest metadata version.
    DirectoryStream<FileAttributes> stream =
        fs.glob(metadataRootFolder.resolve("metadata/*metadata.json"), PathFilters.ALL_FILES);
    for (FileAttributes file : stream) {
      String metadataFile = file.getPath().getName();

      // gets the metadata file's versionID. version ID first sequence of characters in the
      // filename.
      int currVersionID = Integer.parseInt(metadataFile.substring(0, metadataFile.indexOf('-')));
      if (currVersionID > maxVersionID) {
        maxVersionID = currVersionID;
        maxVersionInDir = file.getPath();
      }
    }
    if (maxVersionInDir == null) {
      throw new FileNotFoundException(
          String.format(
              "metadata file %s does not exist in the following directory: %s",
              p.getName(), metadataRootFolder));
    }
    return maxVersionInDir;
  }
}
