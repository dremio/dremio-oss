/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceService.SplitCompression;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;

import io.protostuff.ByteString;

/**
 * Simple facade around namespace that is responsible for persisting datasets. We do this to ensure
 * that system namespace access doesn't leak into user context except for persistence purposes (we
 * always persist as a system user but we need to retrieve as the query user).
 */
public class DatasetSaver {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetSaver.class);

  private final NamespaceService systemNamespace;
  private final MetadataUpdateListener updateListener;
  private final OptionManager optionManager;

  DatasetSaver(NamespaceService systemNamespace, MetadataUpdateListener updateListener, OptionManager optionManager) {
    this.systemNamespace = systemNamespace;
    this.updateListener = updateListener;
    this.optionManager = optionManager;
  }

  /**
   * Save the dataset to namespace.
   * <p>
   * The dataset is represented by the given config and handle. The source metadata API is used to fetch metadata, and
   * save the metadata to namespace. The dataset mutator is the invoked before saving the metadata to namespace; this
   * allow the callers to mutate the dataset config before saving.
   *
   * @param datasetConfig      dataset config
   * @param handle             dataset handle
   * @param sourceMetadata     source metadata
   * @param opportunisticSave  if set, will only attempt a save once, without attempting to handle concurrent modifications
   * @param options            options
   * @param datasetMutator     dataset mutator
   * @param attributes         extra attributes
   */
  public void save(
      DatasetConfig datasetConfig,
      DatasetHandle handle,
      SourceMetadata sourceMetadata,
      boolean opportunisticSave,
      DatasetRetrievalOptions options,
      Function<DatasetConfig, DatasetConfig> datasetMutator,
      NamespaceAttribute... attributes
  ) {
    if (datasetConfig.getId() == null) {
      // this is a new dataset, otherwise save will fail
      datasetConfig.setId(new EntityId(UUID.randomUUID().toString()));
    }
    final NamespaceKey canonicalKey = MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath());
    SplitCompression splitCompression = NamespaceService.SplitCompression.valueOf(optionManager.getOption(CatalogOptions.SPLIT_COMPRESSION_TYPE).toUpperCase());
    try (DatasetMetadataSaver saver = systemNamespace.newDatasetMetadataSaver(canonicalKey, datasetConfig.getId(), splitCompression)) {
      final PartitionChunkListing chunkListing = sourceMetadata.listPartitionChunks(handle,
          options.asListPartitionChunkOptions(datasetConfig));
      final Iterator<? extends PartitionChunk> chunks = chunkListing.iterator();
      while (chunks.hasNext()) {
        final PartitionChunk chunk = chunks.next();

        final Iterator<? extends DatasetSplit> splits = chunk.getSplits().iterator();
        while (splits.hasNext()) {
          final DatasetSplit split = splits.next();
          saver.saveDatasetSplit(split);
        }
        saver.savePartitionChunk(chunk);
      }

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
          options.maxMetadataLeafColumns());
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
    } catch (Exception e) {
      throw UserException.validationError(e)
          .build(logger);
    }
  }

  /**
   * @param opportunisticSave  if set, will only attempt a save once, without attempting to handle concurrent modifications
   */
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
}
