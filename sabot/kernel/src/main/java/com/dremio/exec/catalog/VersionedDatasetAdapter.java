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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static com.dremio.exec.planner.physical.PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.nessie.NessieConfig.NESSIE_DEFAULT_BRANCH;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.datastore.SearchTypes;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadataImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

import io.protostuff.ByteString;


/**
 * Translates the Iceberg format dataset metadata into internal dremio defined classes
 * using the Filesystem(iceberg format) storage plugin interface. To get a specific version
 * of the tables, metadata, it passes in VersionedDatasetAccessOptions to be passed to Nessie
 * so the right version can be looked up.
 */
public class VersionedDatasetAdapter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VersionedDatasetAdapter.class);

  private String versionedTableKey;
  private Optional<VersionContext> versionContext;
  private final ManagedStoragePlugin fsStoragePlugin;
  private final OptionManager optionManager;
  private DatasetHandle datasetHandle;
  private SplitsPointer splitsPointer;
  private DatasetConfig versionedDatasetConfig;


  private VersionedDatasetAdapter(Builder builder) {
    this.versionedTableKey = builder.versionedTableKey;
    this.versionContext = builder.versionContext;
    this.fsStoragePlugin = builder.fsStoragePlugin;
    this.optionManager = builder.optionManager;
    this.splitsPointer = null;
    versionedDatasetConfig = builder.versionedDatasetConfig;
    datasetHandle = builder.datasetHandle;
  }

  public static VersionedDatasetAdapter.Builder newBuilder() {
    return new VersionedDatasetAdapter.Builder();
  }


  public DremioTable getTable(final String accessUserName) {
    // Figure out the user we want to access the dataplane with.
    // *TBD*  Use the Filesystem(Iceberg) plugin to tell us the configuration/username
    //Similar to SchemaConfig , we need a config for DataPlane
    //TBD check access to the dataset
    checkAccesss(versionedTableKey, versionContext, accessUserName);

    // Retrieve the metadata
    try {
      versionedDatasetConfig = getMutatedVersionedConfig(versionedTableKey, versionContext, versionedDatasetConfig);
    } catch (final ConnectorException e) {
      throw UserException.validationError(e)
        .build(logger);
    }
    // Construct the TableMetadata

    final TableMetadata tableMetadata = new TableMetadataImpl(fsStoragePlugin.getId(),
      versionedDatasetConfig,
      accessUserName,
      splitsPointer);
    //TBD - Need to determine if we return NamespaceTable (just liek the current getTableFromPlugin and
    //why the contract in the interface shows DremioTable.
    return new NamespaceTable(tableMetadata, optionManager.getOption(FULL_NESTED_SCHEMA_SUPPORT));
  }


  private DatasetConfig getMutatedVersionedConfig(String nessieKey,
                                                  Optional<VersionContext> versionContext,
                                                  DatasetConfig datasetConfig) throws ConnectorException {
    final DatasetMetadata datasetMetadata = getMetadata(nessieKey, versionContext, datasetConfig);
    final Optional<ByteString> readSignature = getReadSignature();
    final Function<DatasetConfig, DatasetConfig> datasetMutator = java.util.function.Function.identity();
    MetadataObjectsUtils.overrideExtended(datasetConfig, datasetMetadata, readSignature,
      1, Integer.MAX_VALUE);
    datasetConfig = datasetMutator.apply(datasetConfig);
    return datasetConfig;
  }

  /**
   * Provide a read signature for the dataset. This is invoked only if dataset metadata is available for a
   * dataset.
   *
   * @return read signature, not null
   */
  private Optional<ByteString> getReadSignature() throws ConnectorException {
    Preconditions.checkNotNull(datasetHandle);
    Optional<ByteString> readSignature = Optional.empty();
    final SourceMetadata sourceMetadata = fsStoragePlugin.unwrap(StoragePlugin.class);
    if (sourceMetadata instanceof SupportsReadSignature) {
      final BytesOutput output;
      try {
        output = ((SupportsReadSignature) sourceMetadata).provideSignature(datasetHandle, null);
      } catch (final ConnectorException e) {
        throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", datasetHandle.getDatasetPath())
          .build(logger);
      }
      if (output != BytesOutput.NONE) {
        readSignature = Optional.of(MetadataObjectsUtils.toProtostuff(output));
      }
    }
    return readSignature;
  }

  /**
   * Given a key and VersionContext , return the {@link DatasetMetadata dataset metadata} for the versioned dataset
   * represented by the handle.
   *
   * @param tableKey
   * @param versionContext
   * @param datasetConfig
   * @return Dataset metadata, not null
   */
  private DatasetMetadata getMetadata(final String tableKey,
                                      Optional<VersionContext> versionContext,
                                      DatasetConfig datasetConfig) throws ConnectorException {
    final SourceMetadata sourceMetadata = fsStoragePlugin.unwrap(StoragePlugin.class);
    Preconditions.checkNotNull(datasetHandle);

    final VersionedDatasetAccessOptions versionedDatasetAccessOptions = new VersionedDatasetAccessOptions.Builder()
      .setVersionedTableKeyPath(tableKey)
      .setVersionContext(versionContext)
      .build();

    final DatasetRetrievalOptions retrievalOptions = DatasetRetrievalOptions.DEFAULT.toBuilder()
      .setVersionedDatasetAccessOptions(versionedDatasetAccessOptions).build();
    final PartitionChunkListing chunkListing = sourceMetadata.listPartitionChunks(datasetHandle,
      retrievalOptions.asListPartitionChunkOptions(null));
    final DatasetMetadata datasetMetadata = sourceMetadata.getDatasetMetadata(datasetHandle,
      chunkListing,
      retrievalOptions.asGetMetadataOptions(null));
    // Construct the split pointer from the PartitionChunkListing.
    splitsPointer = constructSplitPointer(chunkListing, datasetConfig);
    return datasetMetadata;
  }

  private SplitsPointer constructSplitPointer(PartitionChunkListing chunkListing, DatasetConfig datasetConfig) {
    final List<PartitionChunk> chunkList = new ArrayList<>();
    chunkListing.iterator().forEachRemaining(chunkList::add);
    Preconditions.checkArgument(chunkList.size() == 1);
    //For a versioned iceberg table there is just one single split  - the manifest file.
    return new AbstractSplitsPointer() {
      @Override
      public double getSplitRatio() {
        return 1.0d; // default - same as DatasetSplit implementation.
      }

      @Override
      public int getSplitsCount() {
        return chunkList.size();
      }

      @Override
      public SplitsPointer prune(SearchTypes.SearchQuery partitionFilterQuery) {
        return this;
      }

      @Override
      public Iterable<PartitionChunkMetadata> getPartitionChunks() {
        final PartitionProtobuf.PartitionChunk.Builder builder = PartitionProtobuf.PartitionChunk.newBuilder()
          .setSize(0)
          .setRowCount(0)
          .setPartitionExtendedProperty(MetadataProtoUtils.toProtobuf(chunkList.get(0).getExtraInfo()))
          .addAllPartitionValues((chunkList.get(0).getPartitionValues().stream().map(MetadataProtoUtils::toProtobuf)
            .collect(Collectors.toList())))
          .setDatasetSplit(MetadataProtoUtils.toProtobuf(chunkList.get(0).getSplits().iterator().next()))
          .setSplitKey("0")
          .setSplitCount(1);

        final PartitionChunkMetadata partitionChunkMetadata = new PartitionChunkMetadataImpl(
          builder.build(),
          PartitionChunkId.of(datasetConfig, builder.build(), 0));
        return FluentIterable.of(partitionChunkMetadata);
      }

      @Override
      public int getTotalSplitsCount() {
        return chunkList.size();
      }

      @Override
      public long getSplitVersion() {
        return 0; // Default value - unused for versioned datasets
      }
    };
  }

  /**
   * TBD We need to figure out how to do the access check with dataplane
   *
   * @param tableKey
   * @param versionContext
   * @param userName
   * @return
   */
  private boolean checkAccesss(final String tableKey, Optional<VersionContext> versionContext, String userName) {
    //Needs to be implemented.
    return true;
  }


  static public class Builder {
    private String versionedTableKey;
    private Optional<VersionContext> versionContext;
    private ManagedStoragePlugin fsStoragePlugin;
    private OptionManager optionManager;
    private DatasetHandle datasetHandle;
    private DatasetConfig versionedDatasetConfig;

    public Builder() {
    }

    public Builder setVersionedTableKey(String key) {
      versionedTableKey = key;
      return this;
    }

    public Builder setVersionContext(Optional<VersionContext> versionContext) {
      this.versionContext = versionContext;
      return this;
    }

    public Builder setStoragePlugin(ManagedStoragePlugin plugin) {
      fsStoragePlugin = plugin;
      return this;
    }

    public Builder setOptionManager(OptionManager optionManager) {
      this.optionManager = optionManager;
      return this;
    }

    public VersionedDatasetAdapter build() {
      Preconditions.checkNotNull(versionedTableKey, "Table name provided in invalid(null)");
      Preconditions.checkNotNull(versionContext, "versionContext is null");

      if (!CatalogUtil.isVersionedDDPEntity(new NamespaceKey(PathUtils.parseFullPath(versionedTableKey)))) {
        throw UserException.validationError()
          .message("Invalid DDP key [%s].", versionedTableKey)
          .build(logger);
      }
      if (!versionContext.isPresent()) {
        versionContext = Optional.of(VersionContext.fromBranchName(NESSIE_DEFAULT_BRANCH));
      }
      versionedTableKey = CatalogUtil.removeVersionedCatalogPrefix(versionedTableKey); //mutate table key to get rid of "DDP" prefix
      versionedDatasetConfig = createShallowIcebergDatasetConfig(versionedTableKey);
      datasetHandle = getHandleToIcebergFormatPlugin(versionedTableKey, fsStoragePlugin, versionedDatasetConfig);

      return new VersionedDatasetAdapter(this);

    }

    /**
     * Helper method that gets a handle to the Filesystem(Iceberg format) storage plugin
     *
     * @param key
     * @return
     */
    private DatasetHandle getHandleToIcebergFormatPlugin(String key, ManagedStoragePlugin plugin, DatasetConfig datasetConfig) {
      final Optional<DatasetHandle> handle;
      FileSystemPlugin fsPlugin = (FileSystemPlugin<?>)plugin.unwrap(StoragePlugin.class);
      Preconditions.checkNotNull(fsPlugin);
      OptionValue option_enable_iceberg = OptionValue.createBoolean(SYSTEM, ENABLE_ICEBERG.getOptionName(), true);
      fsPlugin.getContext().getOptionManager().setOption(option_enable_iceberg);

      try {
        handle = plugin.getDatasetHandle(
          new NamespaceKey(PathUtils.parseFullPath(key)),
          datasetConfig,
          plugin.getDefaultRetrievalOptions());

      } catch (ConnectorException e) {
        throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
      }
      if (!handle.isPresent()) {
        throw UserException.validationError()
          .message("Failure while getting handle to iceberg table from source [%s].", key)
          .build(logger);
      }
      return handle.get();
    }

    /**
     * This is a helper method that creates a shell datasetConfig that populates the format so the Filessystem plugin knowsn what format we will
     * be using to access the Iceberg tables.
     *
     * @param key Namespace key
     * @return DatasetConfig populated with the basic info needed by the Filessystem plugin to match and unwrap to Iceberg format plugin
     */
    private DatasetConfig createShallowIcebergDatasetConfig(String key) {
      DatasetConfig datasetConfig = new DatasetConfig()
        .setId(new EntityId().setId(UUID.randomUUID().toString()))
        .setName(key)
        .setFullPathList(PathUtils.parseFullPath(key))
        //This format setting allows us to pick the Iceberg format explicity
        .setPhysicalDataset(new PhysicalDataset().setFormatSettings(new IcebergFileConfig().asFileConfig()))
        .setLastModified(System.currentTimeMillis());

      return datasetConfig;
    }

  }

}
