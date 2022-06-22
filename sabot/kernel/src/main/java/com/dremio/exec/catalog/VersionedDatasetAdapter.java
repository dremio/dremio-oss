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

import static com.dremio.exec.catalog.VersionedPlugin.EntityType.ICEBERG_TABLE;
import static com.dremio.exec.catalog.VersionedPlugin.EntityType.ICEBERG_VIEW;
import static com.dremio.exec.planner.physical.PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.iceberg.view.ViewVersionMetadata;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.datastore.SearchTypes;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.VersionedDatasetHandle;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadataImpl;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Translates the Iceberg format dataset metadata into internal dremio defined classes
 * using the Filesystem(iceberg format) storage plugin interface. To get a specific version
 * of the tables, metadata, it passes in VersionedDatasetAccessOptions to be passed to Nessie
 * so the right version can be looked up.
 */
public final class VersionedDatasetAdapter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VersionedDatasetAdapter.class);

  private final String versionedTableKey;
  private final ResolvedVersionContext versionContext;
  private final StoragePlugin storagePlugin;
  private final StoragePluginId storagePluginId;
  private final OptionManager optionManager;
  private final DatasetHandle datasetHandle;
  private SplitsPointer splitsPointer;
  private DatasetConfig versionedDatasetConfig;

  private VersionedDatasetAdapter(Builder builder) {
    this.versionedTableKey = builder.versionedTableKey;
    this.versionContext = builder.versionContext;
    this.storagePlugin = builder.storagePlugin;
    this.storagePluginId = builder.storagePluginId;
    this.optionManager = builder.optionManager;
    this.splitsPointer = null;
    this.versionedDatasetConfig = builder.versionedDatasetConfig;
    this.datasetHandle = builder.datasetHandle;
  }

  public static VersionedDatasetAdapter.Builder newBuilder() {
    return new VersionedDatasetAdapter.Builder();
  }

  public DremioTable getTable(final String accessUserName) {
    return datasetHandle.unwrap(VersionedDatasetHandle.class).translateToDremioTable(this, accessUserName);
  }

  public DremioTable translateIcebergView(String accessUserName) {
    Preconditions.checkState(datasetHandle.unwrap(VersionedDatasetHandle.class).getType() == ICEBERG_VIEW);
    final ViewHandle viewHandle = datasetHandle.unwrap(ViewHandle.class);
    final List<String> viewKeyPath = viewHandle.getDatasetPath().getComponents();

    final ViewVersionMetadata viewVersionMetadata = viewHandle.getViewVersionMetadata();
    Preconditions.checkNotNull(viewVersionMetadata);
    final SchemaConverter schemaConverter = new SchemaConverter(String.join(".", viewKeyPath));
    //Convert from Iceberg to Arrow schema
    final BatchSchema batchSchema = schemaConverter.fromIceberg(viewVersionMetadata.definition().schema());

    //The ViewFieldType list returned will contain the Calcite converted fields.
    final List<ViewFieldType> viewFieldTypesList = ViewFieldsHelper.getBatchSchemaFields(batchSchema);

    final DatasetConfig viewConfig = createShallowVirtualDatasetConfig(viewKeyPath,
      viewVersionMetadata,
      viewFieldTypesList);

    viewConfig.setTag(viewHandle.getTag());

    final View view = Views.fieldTypesToView(Iterables.getLast(viewKeyPath),
      viewVersionMetadata.definition().sql(),
      viewFieldTypesList,
      viewKeyPath.subList(0, viewKeyPath.size()-1),
      batchSchema);

    // TODO: DX-48432 View ownership should set the view owner to the view/dataset creator
    // TODO: Note - Passing null for viewOwner makes the ViewTable expland with just the schemapath and ignores the user during expansion
    return new ViewTable(new NamespaceKey(viewKeyPath),
      view,
      null,
      viewConfig, batchSchema);
  }

  private DatasetConfig createShallowVirtualDatasetConfig(List<String> viewKeyPath,
                                                          ViewVersionMetadata viewVersionMetadata,
                                                          List<ViewFieldType> viewFieldTypesList) {
    final VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setContextList(viewKeyPath.subList(0, viewKeyPath.size()-1));
    virtualDataset.setSql(viewVersionMetadata.definition().sql());
    virtualDataset.setVersion(DatasetVersion.newVersion());
    virtualDataset.setCalciteFieldsList(viewFieldTypesList);
    virtualDataset.setSqlFieldsList(viewFieldTypesList);

    versionedDatasetConfig.setName(Iterables.getLast(viewKeyPath));
    //TODO: DX-48432 View ownership should set the view owner to the view/dataset creator
    versionedDatasetConfig.setOwner("dremio");
    versionedDatasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    versionedDatasetConfig.setVirtualDataset(virtualDataset);
    return versionedDatasetConfig;
  }

  public  DremioTable translateIcebergTable(final String accessUserName) {
    // Figure out the user we want to access the dataplane with.
    // *TBD*  Use the Filesystem(Iceberg) plugin to tell us the configuration/username
    //Similar to SchemaConfig , we need a config for DataPlane
    // TODO: check access to the dataset (and use return value or make method throw)
    Preconditions.checkState(datasetHandle.unwrap(VersionedDatasetHandle.class).getType() == ICEBERG_TABLE);
    checkAccess(versionedTableKey, versionContext, accessUserName);

    // Retrieve the metadata
    try {
      versionedDatasetConfig = getMutatedVersionedConfig(versionedTableKey, versionContext, versionedDatasetConfig);
    } catch (final ConnectorException e) {
      throw UserException.validationError(e)
        .build(logger);
    }
    // Construct the TableMetadata

    final TableMetadata tableMetadata = new TableMetadataImpl(storagePluginId,
      versionedDatasetConfig,
      accessUserName,
      splitsPointer);

    return new NamespaceTable(tableMetadata, optionManager.getOption(FULL_NESTED_SCHEMA_SUPPORT));
  }


  private DatasetConfig getMutatedVersionedConfig(String nessieKey,
                                                  ResolvedVersionContext versionContext,
                                                  DatasetConfig datasetConfig) throws ConnectorException {
    final DatasetMetadata datasetMetadata = getMetadata(nessieKey, versionContext, datasetConfig);
    final Optional<ByteString> readSignature = getReadSignature();
    final Function<DatasetConfig, DatasetConfig> datasetMutator = java.util.function.Function.identity();
    MetadataObjectsUtils.overrideExtended(datasetConfig, datasetMetadata, readSignature,
      1, Integer.MAX_VALUE);
    datasetConfig = datasetMutator.apply(datasetConfig);
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    return datasetConfig;
  }

  /**
   * Provide a read signature for the dataset. This is invoked only if dataset metadata is available for a
   * dataset.
   *
   * @return read signature, not null
   */
  private Optional<ByteString> getReadSignature() {
    Preconditions.checkNotNull(datasetHandle);
    Optional<ByteString> readSignature = Optional.empty();

    if (storagePlugin instanceof SupportsReadSignature) {
      final BytesOutput output;
      try {
        output = ((SupportsReadSignature) storagePlugin).provideSignature(datasetHandle, null);
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
   * @return Dataset metadata, not null
   */
  private DatasetMetadata getMetadata(final String tableKey,
                                      ResolvedVersionContext versionContext,
                                      DatasetConfig datasetConfig) throws ConnectorException {
    Preconditions.checkNotNull(datasetHandle);

    final VersionedDatasetAccessOptions versionedDatasetAccessOptions =
        new VersionedDatasetAccessOptions.Builder()
            .setVersionContext(versionContext)
            .build();

    final DatasetRetrievalOptions retrievalOptions = DatasetRetrievalOptions.DEFAULT.toBuilder()
        .setVersionedDatasetAccessOptions(versionedDatasetAccessOptions).build();
    final PartitionChunkListing chunkListing = storagePlugin.listPartitionChunks(datasetHandle,
        retrievalOptions.asListPartitionChunkOptions(null));
    final DatasetMetadata datasetMetadata = storagePlugin.getDatasetMetadata(datasetHandle,
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
   */
  private boolean checkAccess(final String tableKey, ResolvedVersionContext versionContext, String userName) {
    // TODO: Needs to be implemented
    return true;
  }

  public static class Builder {
    private String versionedTableKey;
    private ResolvedVersionContext versionContext;
    private StoragePlugin storagePlugin;
    private StoragePluginId storagePluginId;
    private OptionManager optionManager;
    private DatasetHandle datasetHandle;
    private DatasetConfig versionedDatasetConfig;

    public Builder() {
    }

    public Builder setVersionedTableKey(String key) {
      versionedTableKey = key;
      return this;
    }

    public Builder setVersionContext(ResolvedVersionContext versionContext) {
      this.versionContext = versionContext;
      return this;
    }

    public Builder setStoragePlugin(StoragePlugin plugin) {
      storagePlugin = plugin;
      return this;
    }

    public Builder setStoragePluginId(StoragePluginId pluginId) {
      storagePluginId = pluginId;
      return this;
    }

    public Builder setOptionManager(OptionManager optionManager) {
      this.optionManager = optionManager;
      return this;
    }

    public VersionedDatasetAdapter build() {
      Preconditions.checkNotNull(versionedTableKey);
      Preconditions.checkNotNull(versionContext);
      Preconditions.checkNotNull(storagePlugin);
      Preconditions.checkNotNull(storagePluginId);

      versionedDatasetConfig = createShallowIcebergDatasetConfig(versionedTableKey);

      if (!tryGetHandleToIcebergFormatPlugin(storagePlugin, versionedDatasetConfig)) {
        return null;
      }

      return new VersionedDatasetAdapter(this);
    }

    /**
     * Helper method that gets a handle to the Filesystem(Iceberg format) storage plugin
     */
    private boolean tryGetHandleToIcebergFormatPlugin(StoragePlugin plugin, DatasetConfig datasetConfig) {
      final Optional<DatasetHandle> handle;
      final EntityPath entityPath = new EntityPath(datasetConfig.getFullPathList());

      VersionedDatasetAccessOptions versionedDatasetAccessOptions = new VersionedDatasetAccessOptions.Builder()
        .setVersionContext(versionContext)
        .build();

      try {
        handle = plugin.getDatasetHandle(
          entityPath,
          DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setVersionedDatasetAccessOptions(versionedDatasetAccessOptions)
            .build()
            .asGetDatasetOptions(datasetConfig));

      } catch (ConnectorException e) {
        throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", entityPath)
          .build(logger);
      }
      if (!handle.isPresent()) {
        return false;
        /*throw UserException.validationError()
          .message("Failure while getting handle to iceberg table from source [%s].", entityPath)
          .build(logger);*/
      }
      datasetHandle = handle.get();
      return true;
    }

    /**
     * This is a helper method that creates a shell datasetConfig that populates the format so the Filessystem plugin knowsn what format we will
     * be using to access the Iceberg tables.
     *
     * @param key Namespace key
     * @return DatasetConfig populated with the basic info needed by the Filessystem plugin to match and unwrap to Iceberg format plugin
     */
    private DatasetConfig createShallowIcebergDatasetConfig(String key) {
      return new DatasetConfig()
        .setId(new EntityId().setId(UUID.randomUUID().toString()))
        .setName(key)
        .setFullPathList(PathUtils.parseFullPath(key))
        //This format setting allows us to pick the Iceberg format explicitly
        .setPhysicalDataset(new PhysicalDataset().setFormatSettings(new IcebergFileConfig().asFileConfig()))
        .setLastModified(System.currentTimeMillis());
    }
  }
}
