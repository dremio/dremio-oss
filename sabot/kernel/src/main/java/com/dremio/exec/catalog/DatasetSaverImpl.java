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

import static com.dremio.common.util.DremioStringUtils.firstLine;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshUtils;
import com.dremio.exec.store.metadatarefresh.SupportsUnlimitedSplits;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceOptions;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;

/**
 * Base implementation of DatasetSaver.
 */
public class DatasetSaverImpl implements DatasetSaver {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetSaverImpl.class);
  public static final String unsupportedPartitionListingError = "Found unsupported partition listing in the table";

  private final NamespaceService systemNamespace;
  private final MetadataUpdateListener updateListener;
  private final OptionManager optionManager;

  DatasetSaverImpl(NamespaceService systemNamespace, MetadataUpdateListener updateListener, OptionManager optionManager) {
    this.systemNamespace = systemNamespace;
    this.updateListener = updateListener;
    this.optionManager = optionManager;
  }

  /**
   * Save the dataset to namespace.
   * <p>
   * The dataset is represented by the given config and handle. If plugin supports unlimited splits, runs internal
   * refresh query else the source metadata API is used to fetch metadata, and save the metadata to namespace.
   * The dataset mutator is the invoked before saving the metadata to namespace; this allow the callers to mutate the
   * dataset config before saving.
   *
   * @param datasetConfig      dataset config
   * @param handle             dataset handle
   * @param sourceMetadata     source metadata
   * @param opportunisticSave  if set, will only attempt a save once, without attempting to handle concurrent modifications
   * @param options            options
   * @param datasetMutator     dataset mutator
   * @param attributes         extra attributes
   */
  @Override
  public void save(
    DatasetConfig datasetConfig,
    DatasetHandle handle,
    SourceMetadata sourceMetadata,
    boolean opportunisticSave,
    DatasetRetrievalOptions options,
    Function<DatasetConfig, DatasetConfig> datasetMutator,
    NamespaceAttribute... attributes
  ) {
    final NamespaceKey canonicalKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());

    if (MetadataRefreshUtils.unlimitedSplitsSupportEnabled(optionManager) &&
      sourceMetadata instanceof SupportsUnlimitedSplits) {


      String queryUser = getRefreshQueryUser(options);
      SupportsUnlimitedSplits s = (SupportsUnlimitedSplits) sourceMetadata;
      boolean refreshUsingQuery = s.allowUnlimitedSplits(handle, datasetConfig, queryUser);
      if (refreshUsingQuery) {
        boolean successUsingRefreshQuery = saveUsingInternalRefreshDatasetQuery(datasetConfig, handle,
                options, datasetMutator, s,
                canonicalKey, queryUser);
        if (successUsingRefreshQuery) {
          return;
        }
      }
    }

    Preconditions.checkState(
            datasetConfig.getPhysicalDataset() == null ||
            datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled() == null ||
            !datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled(),
            "Found a dataset with Iceberg metadata in non-Iceberg refresh." +
                    " Please forget metadata for dataset '%s'", canonicalKey);

    saveUsingV1Flow(handle, options, datasetConfig, sourceMetadata,
            opportunisticSave, datasetMutator, canonicalKey, attributes);
  }

  /**
   * @param opportunisticSave  if set, will only attempt a save once, without attempting to handle concurrent modifications
   */
  @Override
  public void save(
    DatasetConfig datasetConfig,
    DatasetHandle handle,
    SourceMetadata sourceMetadata,
    boolean opportunisticSave,
    DatasetRetrievalOptions options,
    NamespaceAttribute... attributes
  ) {
    save(datasetConfig, handle, sourceMetadata, opportunisticSave, options, Function.identity(), attributes);
  }

  @Override
  public void save(DatasetConfig datasetConfig, DatasetHandle handle, SourceMetadata sourceMetadata, boolean opportunisticSave, DatasetRetrievalOptions options, String userName, NamespaceAttribute... attributes
  ) {
    save(datasetConfig, handle, sourceMetadata, opportunisticSave, options, Function.identity(), attributes);
  }

  @WithSpan("metadata-refresh-unlimited-splits")
  private boolean saveUsingInternalRefreshDatasetQuery(DatasetConfig datasetConfig,
                                                        DatasetHandle handle,
                                                        DatasetRetrievalOptions options,
                                                        Function<DatasetConfig, DatasetConfig> datasetMutator,
                                                        SupportsUnlimitedSplits s,
                                                        NamespaceKey canonicalKey,
                                                        String queryUser) {
    try {
      String refreshQuery = getRefreshQuery(handle, options);

      logger.debug("Running internal query to do metadata refresh of table [{}]", String.join(".", handle.getDatasetPath().getComponents()));

      // blocked till query execution finishes
      s.runRefreshQuery(refreshQuery, queryUser);

      // update datasetConfig object will full metadata
      DatasetConfig savedConfig = systemNamespace.getDataset(MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath()));

      if (savedConfig == null || savedConfig.getReadDefinition() == null) {
        throw new IllegalStateException("Refresh query succeeded but catalog was not updated");
      }

      datasetConfig.setId(savedConfig.getId());
      datasetConfig.setRecordSchema(savedConfig.getRecordSchema());
      datasetConfig.setReadDefinition(savedConfig.getReadDefinition());
      datasetConfig.setPhysicalDataset(savedConfig.getPhysicalDataset());
      datasetConfig.setLastModified(savedConfig.getLastModified());
      datasetConfig = datasetMutator.apply(datasetConfig);
      datasetConfig.setTotalNumSplits(savedConfig.getTotalNumSplits());
      datasetConfig.setTag(savedConfig.getTag());

      updateListener.metadataUpdated(canonicalKey);
    } catch (Exception e) {
      UserException uex = (e.getCause() instanceof UserException) ?
              (UserException) e.getCause() : null;
      if (uex == null) {
        throw UserException.refreshDatasetError(e).message(e.getMessage()).build(logger);
      }

      UserBitShared.DremioPBError.ErrorType errorCode = uex.getErrorType();
      switch (errorCode) {
        case CONCURRENT_MODIFICATION:
          logger.error("Internal metadata refresh failed", uex);
          throw UserException.invalidMetadataError()
            .message(e.getMessage())
            .buildSilently();
        case UNSUPPORTED_OPERATION:
          if (uex.getMessage().contains(unsupportedPartitionListingError)) {
            logger.error("REFRESH DATASET query failed. Using old refresh mechanism", uex);
            return false;
          }
          // fall through and throw error
        default:
          throw UserException.refreshDatasetError(uex).message(firstLine(uex.getMessage())).build(logger);
      }
    }

    return true;
  }

  private String getRefreshQuery(DatasetHandle handle,
                                 DatasetRetrievalOptions options) {
    if (options.datasetRefreshQuery().isPresent()) {
      return options.datasetRefreshQuery().get().getQuery();
    }

    return
      "REFRESH DATASET " + handle.getDatasetPath().getComponents().stream()
          .map(component -> {
            if (!component.startsWith("\"")) {
              component = "\"" + component;
            }
            if (!component.endsWith("\"")) {
              component = component + "\"";
            }
            return component;
          })
          .collect(Collectors.joining("."));
  }

  private String getRefreshQueryUser(DatasetRetrievalOptions options) {
    if (options.datasetRefreshQuery().isPresent()) {
      return options.datasetRefreshQuery().get().getUser();
    }
    return SystemUser.SYSTEM_USERNAME;
  }

  @WithSpan("metadata-refresh-old")
  private void saveUsingV1Flow(DatasetHandle handle,
                               DatasetRetrievalOptions options,
                               DatasetConfig datasetConfig,
                               SourceMetadata sourceMetadata,
                               boolean opportunisticSave,
                               Function<DatasetConfig, DatasetConfig> datasetMutator,
                               NamespaceKey canonicalKey,
                               NamespaceAttribute... attributes) {
    if (datasetConfig.getId() == null) {
      // this is a new dataset, otherwise save will fail
      datasetConfig.setId(new EntityId(UUID.randomUUID().toString()));
    }

    NamespaceService.SplitCompression splitCompression = NamespaceService.SplitCompression.valueOf(optionManager.getOption(CatalogOptions.SPLIT_COMPRESSION_TYPE).toUpperCase());
    try (DatasetMetadataSaver saver = systemNamespace.newDatasetMetadataSaver(canonicalKey, datasetConfig.getId(), splitCompression,
            optionManager.getOption(CatalogOptions.SINGLE_SPLIT_PARTITION_MAX), optionManager.getOption(NamespaceOptions.DATASET_METADATA_CONSISTENCY_VALIDATE))) {
      final PartitionChunkListing chunkListing = sourceMetadata.listPartitionChunks(handle,
              options.asListPartitionChunkOptions(datasetConfig));

      final long recordCountFromSplits = saver == null || chunkListing == null ? 0 :
              CatalogUtil.savePartitionChunksInSplitsStores(saver,chunkListing);
      final DatasetMetadata datasetMetadata = sourceMetadata.getDatasetMetadata(handle, chunkListing,
              options.asGetMetadataOptions(datasetConfig));

      Optional<ByteString> readSignature = Optional.empty();
      if (sourceMetadata instanceof SupportsReadSignature) {
        final BytesOutput output = ((SupportsReadSignature) sourceMetadata).provideSignature(handle, datasetMetadata);
        //noinspection ObjectEquality
        if (output != BytesOutput.NONE) {
          readSignature = Optional.of(MetadataObjectsUtils.toProtostuff(output));
        }
      }

      MetadataObjectsUtils.overrideExtended(datasetConfig, datasetMetadata, readSignature,
              recordCountFromSplits, options.maxMetadataLeafColumns());
      datasetConfig = datasetMutator.apply(datasetConfig);

      saver.saveDataset(datasetConfig, opportunisticSave, attributes);
      updateListener.metadataUpdated(canonicalKey);
    } catch (DatasetMetadataTooLargeException e) {
      datasetConfig.setRecordSchema(null);
      datasetConfig.setReadDefinition(null);
      try {
        systemNamespace.addOrUpdateDataset(canonicalKey, datasetConfig);
      } catch (NamespaceException ignored) {
      }
      throw UserException.validationError(e)
              .build(logger);
    } catch (NamespaceException| IOException e) {
      throw UserException.validationError(e)
              .build(logger);
    }
  }
}
