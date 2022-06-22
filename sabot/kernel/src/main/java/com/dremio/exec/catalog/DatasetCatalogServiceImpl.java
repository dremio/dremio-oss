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
package com.dremio.exec.catalog;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.catalog.AddOrUpdateDatasetRequest;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.OperationType;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceInvalidStateException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.protostuff.ByteStringUtil;

/**
 * DatasetCatalogService receives catalog change requests from executors and executes corresponding
 * operations on the CatalogService.
 */
public class DatasetCatalogServiceImpl extends DatasetCatalogServiceGrpc.DatasetCatalogServiceImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetCatalogServiceImpl.class);

  private final Provider<CatalogService> catalogServiceProvider;
  private final Provider<NamespaceService.Factory> namespaceServiceFactoryProvider;

  public DatasetCatalogServiceImpl(Provider<CatalogService> catalogServiceProvider,
                                   Provider<NamespaceService.Factory> namespaceServiceFactoryProvider) {
    this.catalogServiceProvider = catalogServiceProvider;
    this.namespaceServiceFactoryProvider = namespaceServiceFactoryProvider;
  }

  /**
   * Adds or updates a dataset.
   *
   * Calls to this method should be preceded with a call to getDataset() to either retrieve the previous version
   * or determine that there is no existing configuration for the dataset.
   *
   * If there is an existing version of the dataset, the incoming request's operation type must be set to UPDATE and
   * the tag of the previous version must be supplied to the request.
   *
   * If there is not an existing version of the dataest, the incoming request's operation type must be set to CREATE.
   *
   * Exceptions cannot escape this method, however the supplied StreamObserver can return gRPC Status.ABORTED to
   * indicate the write failed due to a ConcurrentModificationException.
   */
  @Override
  public void addOrUpdateDataset(AddOrUpdateDatasetRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("Request received: {}", request);
    try {
      Preconditions.checkArgument(!request.getDatasetPathList().isEmpty());
      final DatasetCatalog catalog = catalogServiceProvider.get().getCatalog(MetadataRequestOptions.of(
        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build()));

      final NamespaceKey name = new NamespaceKey(request.getDatasetPathList());

      final NamespaceService namespaceService = namespaceServiceFactoryProvider.get().get(SystemUser.SYSTEM_USERNAME);
      final boolean datasetExists = namespaceService.exists(name, NameSpaceContainer.Type.DATASET);

      if (request.getOperationType() == OperationType.UPDATE && !datasetExists) {
        // Note that if the caller used an UPDATE operation, and the dataset does not exist, and if they previously called
        // getDataset(), this would imply the dataset previously existed but is now gone (in other words, it was deleted).
        // We throw a CME here to indicate that the caller needs to retry to read-update-write sequence again.
        throw new ConcurrentModificationException("Tried to update a dataset that does not exist.");
      } else if (request.getOperationType() == OperationType.CREATE && datasetExists) {
        // Similarly, if the caller used a CREATE operation, and the dataset does exisst, and the caller previously called
        // getDataset()l it implies that the caller previously found that the dataset didn't yet exist but does now (in
        // other words, it got created).
        // We throw a CME here to indicate that the caller needs to retry to read-update-write sequence again.
        throw new ConcurrentModificationException("Tried to create a dataset that already exists.");
      }
      DatasetConfig config = datasetExists ? namespaceService.getDataset(name) : null;

      if (datasetExists && !request.getDatasetConfig().getTag().equals(config.getTag())) {
        throw new ConcurrentModificationException("Tag mismatch when updating dataset.");
      }

      // There are 2 paths.
      // 1. The dataset already exists and the request is updating the read definition and/or the schema.
      // 2. The dataset is new.
      // Short-circuit if the request is just updating the schema on an existing dataset.
      if (!datasetExists) {
        Preconditions.checkArgument(request.getDatasetConfig().hasBatchSchema(), "BatchSchema must be supplied for new datasets.");
        Preconditions.checkArgument(request.getDatasetConfig().hasDatasetType(), "DatasetType must be supplied for new datasets.");

        config = new DatasetConfig();
        config.setId(new EntityId(UUID.randomUUID().toString()));
        config.setName(request.getDatasetPathList().get(request.getDatasetPathList().size() - 1));
      }

      ReadDefinition resultReadDefinition = config.getReadDefinition();
      if (resultReadDefinition == null) {
        resultReadDefinition = new ReadDefinition();
      }

      // Its possible to not have file Format when we are storing hive dataset in v2 metadata flow
      FileConfig fileConfig = null;
      if(request.getDatasetConfig().hasFileFormat()) {
        fileConfig = toProtoStuff(request.getDatasetPathList(), request.getDatasetConfig().getFileFormat());
      }

      if (!datasetExists || config.getPhysicalDataset() == null || config.getPhysicalDataset().getFormatSettings() == null ||
        !Boolean.valueOf(request.getDatasetConfig().getIcebergMetadataEnabled()).equals(config.getPhysicalDataset().getIcebergMetadataEnabled())) {
        final PhysicalDataset physicalDataset = new PhysicalDataset();
        if (fileConfig != null) {
          physicalDataset.setFormatSettings(fileConfig);
        }
        physicalDataset.setIcebergMetadataEnabled(Boolean.TRUE.equals(request.getDatasetConfig().getIcebergMetadataEnabled()));
        config.setPhysicalDataset(physicalDataset);
        config.setType(DatasetType.valueOf(request.getDatasetConfig().getDatasetType().getNumber()));
        config.setFullPathList(request.getDatasetPathList());
      }

      if (request.getDatasetConfig().hasReadDefinition()) {
        config.setReadDefinition(populateReadDefinitionFromProtoBuf(request.getDatasetConfig().getReadDefinition(), resultReadDefinition));
      }

      if (request.getDatasetConfig().hasBatchSchema()) {
        config.setRecordSchema(ByteStringUtil.wrap(request.getDatasetConfig().getBatchSchema().toByteArray()));
      }

      // Update icebergMetadata that contains root pointer and snapshot version
      if (request.getDatasetConfig().getIcebergMetadataEnabled()) {
        Preconditions.checkState(request.getDatasetConfig().hasIcebergMetadata(), "Unexpected state");
        com.dremio.service.namespace.dataset.proto.IcebergMetadata icebergMetadata =
                new com.dremio.service.namespace.dataset.proto.IcebergMetadata();
        DatasetCommonProtobuf.IcebergMetadata metadata = request.getDatasetConfig().getIcebergMetadata();
        icebergMetadata.setMetadataFileLocation(metadata.getMetadataFileLocation());
        icebergMetadata.setSnapshotId(metadata.getSnapshotId());
        icebergMetadata.setTableUuid(metadata.getTableUuid());
        icebergMetadata.setJsonSchema(metadata.getJsonSchema());
        icebergMetadata.setPartitionSpecsJsonMap(ByteStringUtil.wrap(metadata.getPartitionSpecsJsonMap().toByteArray()));

        String partitionStatsFile = metadata.getPartitionStatsFile();
        if (partitionStatsFile != null) {
          icebergMetadata.setPartitionStatsFile(partitionStatsFile);
        }
        config.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
      }
      saveDataset(namespaceService, catalog, request.getDatasetConfig(), name, config);
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      logger.error("IllegalArgumentException", e);
      responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage()).asException());
    } catch (NamespaceNotFoundException e) {
      logger.error("NamespaceNotFoundException", e);
      responseObserver.onError(Status.NOT_FOUND.withCause(e).withDescription(e.getMessage()).asException());
    } catch (NamespaceInvalidStateException e) {
      logger.error("NamespaceInvalidStateException", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    } catch (ConcurrentModificationException e) {
      logger.error("ConcurrentModificationException", e);
      responseObserver.onError(Status.ABORTED.withCause(e).withDescription(e.getMessage()).asException());
    } catch (UserException e) {
      logger.error("UserException", e);
      switch (e.getErrorType()) {
        case CONCURRENT_MODIFICATION:
        case INVALID_DATASET_METADATA:
          responseObserver.onError(Status.ABORTED.withCause(e).withDescription(e.getMessage()).asException());
          break;
        default:
          responseObserver.onError(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
      }
    } catch (Exception e) {
      logger.error("Exception", e);
      responseObserver.onError(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  private void saveDataset(NamespaceService namespaceService, DatasetCatalog catalog, UpdatableDatasetConfigFields datasetConfigFields,
                           NamespaceKey namespaceKey, DatasetConfig config) throws NamespaceException, IOException {
    if (!datasetConfigFields.hasPartitionChunk()) {
      catalog.addOrUpdateDataset(namespaceKey, config);
      return;
    }

    final PartitionProtobuf.PartitionChunk partitionChunkProto = datasetConfigFields.getPartitionChunk();
    final PartitionProtobuf.DatasetSplit splitProto = partitionChunkProto.getDatasetSplit();

    final List<PartitionValue> partition = partitionChunkProto.getPartitionValuesList().stream()
            .map(MetadataProtoUtils::fromProtobuf).collect(Collectors.toList());
    final List<DatasetSplitAffinity> affinities = splitProto.getAffinitiesList().stream()
            .map(a -> DatasetSplitAffinity.of(a.getHost(), a.getFactor())).collect(Collectors.toList());
    final DatasetSplit datasetSplit = DatasetSplit.of(affinities, splitProto.getSize(), splitProto.getRecordCount(),
            splitProto.getSplitExtendedProperty()::writeTo);
    final PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(partition, datasetSplit);
    partitionChunkListing.computePartitionChunks();
    final PartitionChunk partitionChunk = partitionChunkListing.iterator().next();

    // TODO: Get split compression and partition chunks from support options.
    final DatasetMetadataSaver saver = namespaceService.newDatasetMetadataSaver(
            namespaceKey, config.getId(), NamespaceService.SplitCompression.SNAPPY, 500L,
            false);
    saver.saveDatasetSplit(datasetSplit);
    saver.savePartitionChunk(partitionChunk);

    saver.saveDataset(config, false, new NamespaceAttribute[0]);
    logger.info("Saving partition chunk {}", partitionChunk);
  }

  /**
   * Get the metadata for an existing dataset.
   *
   * Exceptions cannot be thrown from this method, however a Grpc Status.NOT_FOUND exception will be written to the
   * StreamObserver if the dataset requested does not exist.
   *
   * The intent of this method is to supply metadata that can be modified and sent back in a call to
   * {@link #addOrUpdateDataset(AddOrUpdateDatasetRequest, StreamObserver)}.
   *
   * If this method returned a NOT_FOUND exception,
   * a subsequent call to {@link #addOrUpdateDataset(AddOrUpdateDatasetRequest, StreamObserver)} should be sent with the
   * CREATE operation type. If this method successfully returned a dataset, then a subsequent call to
   * {@link #addOrUpdateDataset(AddOrUpdateDatasetRequest, StreamObserver)} should be sent with the UPDATE operation type
   * and use the tag that was returned.
   */
  @Override
  public void getDataset(GetDatasetRequest request, StreamObserver<UpdatableDatasetConfigFields> responseObserver) {
    logger.debug("Request received: {}", request);
    try {
      Preconditions.checkArgument(!request.getDatasetPathList().isEmpty());
      final DatasetConfig config = namespaceServiceFactoryProvider.get().get(SystemUser.SYSTEM_USERNAME).getDataset(new NamespaceKey(request.getDatasetPathList()));

      final UpdatableDatasetConfigFields.Builder resultBuilder = UpdatableDatasetConfigFields.newBuilder()
        .setDatasetType(DatasetCommonProtobuf.DatasetType.forNumber(config.getType().getNumber()))
        .setBatchSchema(UnsafeByteOperations.unsafeWrap(config.getRecordSchema().asReadOnlyByteBuffer()))
        .setReadDefinition(toProtoBuf(config.getReadDefinition()))
        .setTag(config.getTag());

      if (config.getPhysicalDataset() != null && config.getPhysicalDataset().getFormatSettings() != null) {
        resultBuilder.setFileFormat(toProtoBuf(config.getPhysicalDataset().getFormatSettings()));
      }

      responseObserver.onNext(resultBuilder.build());
      responseObserver.onCompleted();
    } catch (NamespaceNotFoundException e) {
      logger.error("NamespaceNotFoundException", e);
      responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
    } catch (NamespaceInvalidStateException e) {
      logger.error("NamespaceInvalidStateException", e);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    } catch (Exception e) {
      logger.error("Exception", e);
      responseObserver.onError(Status.UNKNOWN.withCause(e).asException());
    }
  }

  private static FileConfig toProtoStuff(List<String> datasetPath, FileProtobuf.FileConfig newFileConfig) {
    final FileConfig fileConfig = new FileConfig();
    fileConfig
      .setFullPathList(new NamespaceKey(datasetPath).getPathComponents())
      .setName(datasetPath.get(datasetPath.size()-1));

    if (newFileConfig.hasExtendedConfig()) {
      fileConfig.setExtendedConfig(ByteStringUtil.wrap(newFileConfig.getExtendedConfig().toByteArray()));
    }

    if (newFileConfig.hasType()) {
      fileConfig.setType(FileType.valueOf(newFileConfig.getType().getNumber()));
    }

    if (newFileConfig.hasOwner()) {
      fileConfig.setOwner(newFileConfig.getOwner());
    }

    if (newFileConfig.hasTag()) {
      fileConfig.setTag(newFileConfig.getTag());
    }

    if (newFileConfig.hasLocation()) {
      fileConfig.setLocation(newFileConfig.getLocation());
    }

    if (newFileConfig.hasCtime()) {
      fileConfig.setCtime(newFileConfig.getCtime());
    }

    return fileConfig;
  }

  private ReadDefinition populateReadDefinitionFromProtoBuf(DatasetCommonProtobuf.ReadDefinition newReadDefinition,
                                                            ReadDefinition outputReadDefinition) {
    if (newReadDefinition.hasReadSignature()) {
      outputReadDefinition.setReadSignature(ByteStringUtil.wrap(newReadDefinition.getReadSignature().toByteArray()));
    }

    if (newReadDefinition.hasExtendedProperty()) {
      outputReadDefinition.setExtendedProperty(ByteStringUtil.wrap(newReadDefinition.getExtendedProperty().toByteArray()));
    }

    if (newReadDefinition.hasManifestScanStats()) {
      outputReadDefinition.setManifestScanStats(toProtoStuff(newReadDefinition.getManifestScanStats()));
    }

    if (newReadDefinition.hasLastRefreshDate()) {
      outputReadDefinition.setLastRefreshDate(newReadDefinition.getLastRefreshDate());
    }

    if (newReadDefinition.hasScanStats()) {
      outputReadDefinition.setScanStats(toProtoStuff(newReadDefinition.getScanStats()));
    }

    if (newReadDefinition.hasSplitVersion()) {
      outputReadDefinition.setSplitVersion(newReadDefinition.getSplitVersion());
    }

    if (newReadDefinition.getPartitionColumnsCount() > 0) {
      outputReadDefinition.setPartitionColumnsList(newReadDefinition.getPartitionColumnsList());
    }

    if (newReadDefinition.getSortColumnsCount() > 0) {
      outputReadDefinition.setSortColumnsList(newReadDefinition.getSortColumnsList());
    }
    return outputReadDefinition;
  }

  private static FileProtobuf.FileConfig toProtoBuf(FileConfig fileConfig) {
    final FileProtobuf.FileConfig.Builder newFileConfigBuilder = FileProtobuf.FileConfig.newBuilder();

    newFileConfigBuilder.addAllFullPath(fileConfig.getFullPathList());
    newFileConfigBuilder.setType(FileProtobuf.FileType.forNumber(fileConfig.getType().getNumber()));

    if (fileConfig.getExtendedConfig() != null && !fileConfig.getExtendedConfig().isEmpty()) {
      newFileConfigBuilder.setExtendedConfig(UnsafeByteOperations.unsafeWrap(fileConfig.getExtendedConfig().asReadOnlyByteBuffer()));
    }

    if (fileConfig.getOwner() != null) {
      newFileConfigBuilder.setOwner(fileConfig.getOwner());
    }

    if (fileConfig.getTag() != null) {
      newFileConfigBuilder.setTag(fileConfig.getTag());
    }

    if (fileConfig.getLocation() != null) {
      newFileConfigBuilder.setLocation(fileConfig.getLocation());
    }

    if (fileConfig.getCtime() != null) {
      newFileConfigBuilder.setCtime(fileConfig.getCtime());
    }

    return newFileConfigBuilder.build();
  }

  private static ScanStats toProtoStuff(DatasetCommonProtobuf.ScanStats scanStats) {
    final ScanStats result = new ScanStats();

    if (scanStats.hasScanFactor()) {
      result.setScanFactor(scanStats.getScanFactor());
    }

    if (scanStats.hasCpuCost()) {
      result.setCpuCost(scanStats.getCpuCost());
    }

    if (scanStats.hasDiskCost()) {
      result.setDiskCost(scanStats.getDiskCost());
    }

    if (scanStats.hasRecordCount()) {
      result.setRecordCount(scanStats.getRecordCount());
    }

    if (scanStats.hasType()) {
      result.setType(ScanStatsType.valueOf(scanStats.getType().getNumber()));
    }

    return result;
  }

  private DatasetCommonProtobuf.ReadDefinition toProtoBuf(ReadDefinition readDefinition) {
    final DatasetCommonProtobuf.ReadDefinition.Builder builder = DatasetCommonProtobuf.ReadDefinition.newBuilder();

    if (readDefinition.getReadSignature() != null) {
      builder.setReadSignature(UnsafeByteOperations.unsafeWrap(readDefinition.getReadSignature().asReadOnlyByteBuffer()));
    }

    if (readDefinition.getExtendedProperty() != null) {
      builder.setExtendedProperty(UnsafeByteOperations.unsafeWrap(readDefinition.getExtendedProperty().toByteArray()));
    }

    if (readDefinition.getManifestScanStats() != null) {
      builder.setManifestScanStats(toProtoBuf(readDefinition.getManifestScanStats()));
    }

    if (readDefinition.getLastRefreshDate() != null) {
      builder.setLastRefreshDate(readDefinition.getLastRefreshDate());
    }

    if (readDefinition.getScanStats() != null) {
      builder.setScanStats(toProtoBuf(readDefinition.getScanStats()));
    }

    if (readDefinition.getSplitVersion() != null) {
      builder.setSplitVersion(readDefinition.getSplitVersion());
    }

    if (readDefinition.getPartitionColumnsList() != null) {
      builder.addAllPartitionColumns(readDefinition.getPartitionColumnsList());
    }

    if (readDefinition.getSortColumnsList() != null) {
      builder.addAllSortColumns(readDefinition.getSortColumnsList());
    }
    return builder.build();
  }

  private static DatasetCommonProtobuf.ScanStats toProtoBuf(ScanStats scanStats) {
    final DatasetCommonProtobuf.ScanStats.Builder builder = DatasetCommonProtobuf.ScanStats.newBuilder();

    if (scanStats.getType() != null) {
      builder.setType(DatasetCommonProtobuf.ScanStatsType.forNumber(scanStats.getType().getNumber()));
    }

    if (scanStats.getCpuCost() != null) {
      builder.setCpuCost(scanStats.getCpuCost());
    }

    if (scanStats.getScanFactor() != null) {
      builder.setScanFactor(scanStats.getScanFactor());
    }

    if (scanStats.getDiskCost() != null) {
      builder.setDiskCost(scanStats.getDiskCost());
    }

    if (scanStats.getRecordCount() != null) {
      builder.setRecordCount(scanStats.getRecordCount());
    }

    return builder.build();
  }
}
