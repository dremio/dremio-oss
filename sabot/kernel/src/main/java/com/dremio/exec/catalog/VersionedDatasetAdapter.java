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

import static com.dremio.exec.catalog.CatalogOptions.VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED;
import static com.dremio.exec.catalog.VersionedPlugin.EntityType.ICEBERG_TABLE;
import static com.dremio.exec.catalog.VersionedPlugin.EntityType.ICEBERG_VIEW;
import static com.dremio.exec.planner.physical.PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.datastore.SearchTypes;
import com.dremio.exec.ExecConstants;
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
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
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
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;

/**
 * Translates the Iceberg format dataset metadata into internal dremio defined classes using the
 * Filesystem(iceberg format) storage plugin interface. To get a specific version of the tables,
 * metadata, it passes in VersionedDatasetAccessOptions to be passed to Nessie so the right version
 * can be looked up.
 */
public class VersionedDatasetAdapter {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VersionedDatasetAdapter.class);

  private final List<String> versionedTableKey;
  private final ResolvedVersionContext versionContext;
  private final StoragePlugin storagePlugin;
  private final StoragePluginId storagePluginId;
  private final OptionManager optionManager;
  private final DatasetHandle datasetHandle;
  private SplitsPointer splitsPointer;
  private DatasetConfig versionedDatasetConfig;

  protected VersionedDatasetAdapter(
      List<String> versionedTableKey,
      ResolvedVersionContext versionContext,
      StoragePlugin storagePlugin,
      StoragePluginId storagePluginId,
      OptionManager optionManager,
      DatasetHandle datasetHandle,
      DatasetConfig versionedDatasetConfig) {
    this.versionedTableKey = versionedTableKey;
    this.versionContext = versionContext;
    this.storagePlugin = storagePlugin;
    this.storagePluginId = storagePluginId;
    this.optionManager = optionManager;
    this.datasetHandle = datasetHandle;
    this.versionedDatasetConfig = versionedDatasetConfig;

    // This null is fine because until we get the metadata for the entity (which happens later),
    // we cannot build the splitsPointer.
    this.splitsPointer = null;
  }

  @WithSpan
  public DremioTable getTable(final String accessUserName) {
    return datasetHandle
        .unwrap(VersionedDatasetHandle.class)
        .translateToDremioTable(this, accessUserName);
  }

  public DremioTable translateIcebergView(String accessUserName) {
    Preconditions.checkState(
        datasetHandle.unwrap(VersionedDatasetHandle.class).getType() == ICEBERG_VIEW);
    final ViewHandle viewHandle = datasetHandle.unwrap(ViewHandle.class);
    final List<String> viewKeyPath = viewHandle.getDatasetPath().getComponents();

    final ViewVersionMetadata viewVersionMetadata = viewHandle.getViewVersionMetadata();
    Preconditions.checkNotNull(viewVersionMetadata);
    final SchemaConverter schemaConverter =
        SchemaConverter.getBuilder()
            .setMapTypeEnabled(optionManager.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
            .setTableName(String.join(".", viewKeyPath))
            .build();
    // Convert from Iceberg to Arrow schema
    final BatchSchema batchSchema =
        schemaConverter.fromIceberg(viewVersionMetadata.definition().schema());

    // The ViewFieldType list returned will contain the Calcite converted fields.
    final List<ViewFieldType> viewFieldTypesList =
        ViewFieldsHelper.getBatchSchemaFields(batchSchema);

    final DatasetConfig viewConfig =
        createShallowVirtualDatasetConfig(viewKeyPath, viewVersionMetadata, viewFieldTypesList);

    viewConfig.setTag(viewHandle.getUniqueInstanceId());
    VersionedDatasetId versionedDatasetId =
        new VersionedDatasetId(
            versionedTableKey, viewHandle.getContentId(), TableVersionContext.of(versionContext));
    viewConfig.setId(new EntityId(versionedDatasetId.asString()));
    viewConfig.setRecordSchema(batchSchema.toByteString());
    viewConfig.setLastModified(viewVersionMetadata.currentVersion().timestampMillis());

    final View view =
        Views.fieldTypesToView(
            Iterables.getLast(viewKeyPath),
            viewVersionMetadata.definition().sql(),
            viewFieldTypesList,
            viewConfig.getVirtualDataset().getContextList(),
            batchSchema);

    CatalogIdentity catalogIdentity = null;
    if (optionManager.getOption(VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)) {
      try {
        VersionContext versionContext = versionedDatasetId.getVersionContext().asVersionContext();
        catalogIdentity =
            getOwner(
                new EntityPath(versionedTableKey),
                versionContext.getValue(),
                versionContext.getType().name());
        if (catalogIdentity != null) {
          viewConfig.setOwner(catalogIdentity.getName());
        }
      } catch (Exception e) {
        throw UserException.dataReadError(e).buildSilently();
      }
    }

    return new ViewTable(
        new NamespaceKey(viewKeyPath),
        view,
        catalogIdentity,
        viewConfig,
        batchSchema,
        TableVersionContext.of(versionContext).asVersionContext(),
        false);
  }

  private DatasetConfig createShallowVirtualDatasetConfig(
      List<String> viewKeyPath,
      ViewVersionMetadata viewVersionMetadata,
      List<ViewFieldType> viewFieldTypesList) {
    final VirtualDataset virtualDataset = new VirtualDataset();
    List<String> workspaceSchemaPath = viewVersionMetadata.definition().sessionNamespace();
    virtualDataset.setContextList(workspaceSchemaPath);
    virtualDataset.setSql(viewVersionMetadata.definition().sql());
    virtualDataset.setVersion(DatasetVersion.newVersion());
    virtualDataset.setSqlFieldsList(viewFieldTypesList);

    if (viewVersionMetadata.properties().containsKey("enable_default_reflection")) {
      final boolean enableDefaultReflection =
          Boolean.parseBoolean(viewVersionMetadata.properties().get("enable_default_reflection"));
      virtualDataset.setDefaultReflectionEnabled(enableDefaultReflection);
    }

    versionedDatasetConfig.setName(Iterables.getLast(viewKeyPath));
    versionedDatasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    versionedDatasetConfig.setVirtualDataset(virtualDataset);
    return versionedDatasetConfig;
  }

  @WithSpan
  public DremioTable translateIcebergTable(final String accessUserName, Table table) {
    // Figure out the user we want to access the dataplane with.
    // *TBD*  Use the Filesystem(Iceberg) plugin to tell us the configuration/username
    // Similar to SchemaConfig , we need a config for DataPlane
    // TODO: check access to the dataset (and use return value or make method throw)

    VersionedDatasetHandle versionedDatasetHandle =
        datasetHandle.unwrap(VersionedDatasetHandle.class);
    Preconditions.checkState(versionedDatasetHandle.getType() == ICEBERG_TABLE);
    checkAccess(versionedTableKey, versionContext, accessUserName);

    // Retrieve the metadata
    try {
      versionedDatasetConfig = getMutatedVersionedConfig(versionContext, versionedDatasetConfig);
    } catch (final ConnectorException e) {
      throw UserException.validationError(e).build(logger);
    }
    VersionedDatasetId versionedDatasetId =
        new VersionedDatasetId(
            versionedTableKey,
            versionedDatasetHandle.getContentId(),
            TableVersionContext.of(versionContext));

    versionedDatasetConfig.setId(new EntityId(versionedDatasetId.asString()));
    setIcebergTableUUID(versionedDatasetConfig, versionedDatasetHandle.getUniqueInstanceId());
    if (optionManager.getOption(VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)) {
      try {
        VersionContext versionContext = versionedDatasetId.getVersionContext().asVersionContext();
        CatalogIdentity catalogIdentity =
            getOwner(
                new EntityPath(versionedTableKey),
                versionContext.getValue(),
                versionContext.getType().name());
        versionedDatasetConfig.setOwner(catalogIdentity != null ? catalogIdentity.getName() : null);
      } catch (Exception e) {
        throw UserException.dataReadError(e).buildSilently();
      }
    }

    final TableMetadata tableMetadata =
        new TableMetadataImpl(
            storagePluginId,
            versionedDatasetConfig,
            accessUserName,
            splitsPointer,
            IcebergUtils.getPrimaryKey(table, versionedDatasetConfig)) {
          @Override
          public TableVersionContext getVersionContext() {
            return TableVersionContext.of(versionContext);
          }
        };

    // A versioned dataset is immutable and therefore always considered complete and up-to-date.
    final DatasetMetadataState metadataState =
        DatasetMetadataState.builder()
            .setIsComplete(true)
            .setIsExpired(false)
            .setLastRefreshTimeMillis(System.currentTimeMillis())
            .build();

    return new NamespaceTable(
        tableMetadata, metadataState, optionManager.getOption(FULL_NESTED_SCHEMA_SUPPORT));
  }

  public CatalogIdentity getOwner(EntityPath entityPath, String refValue, String refType)
      throws Exception {
    return null;
  }

  protected StoragePlugin getStoragePlugin() {
    return this.storagePlugin;
  }

  private DatasetConfig getMutatedVersionedConfig(
      ResolvedVersionContext versionContext, DatasetConfig datasetConfig)
      throws ConnectorException {
    final DatasetMetadata datasetMetadata = getMetadata(versionContext, datasetConfig);
    final Optional<ByteString> readSignature = getReadSignature();
    final Function<DatasetConfig, DatasetConfig> datasetMutator =
        java.util.function.Function.identity();
    MetadataObjectsUtils.overrideExtended(
        datasetConfig, datasetMetadata, readSignature, 1, Integer.MAX_VALUE);
    datasetConfig = datasetMutator.apply(datasetConfig);
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    return datasetConfig;
  }

  /**
   * Provide a read signature for the dataset. This is invoked only if dataset metadata is available
   * for a dataset.
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
   * Given a key and VersionContext , return the {@link DatasetMetadata dataset metadata} for the
   * versioned dataset represented by the handle.
   *
   * @return Dataset metadata, not null
   */
  private DatasetMetadata getMetadata(
      ResolvedVersionContext versionContext, DatasetConfig datasetConfig)
      throws ConnectorException {
    Preconditions.checkNotNull(datasetHandle);

    final VersionedDatasetAccessOptions versionedDatasetAccessOptions =
        new VersionedDatasetAccessOptions.Builder().setVersionContext(versionContext).build();

    final DatasetRetrievalOptions retrievalOptions =
        DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setVersionedDatasetAccessOptions(versionedDatasetAccessOptions)
            .build();
    final PartitionChunkListing chunkListing =
        storagePlugin.listPartitionChunks(
            datasetHandle, retrievalOptions.asListPartitionChunkOptions(null));
    final DatasetMetadata datasetMetadata =
        storagePlugin.getDatasetMetadata(
            datasetHandle, chunkListing, retrievalOptions.asGetMetadataOptions(null));
    // Construct the split pointer from the PartitionChunkListing.
    splitsPointer = constructSplitPointer(chunkListing, datasetConfig);
    return datasetMetadata;
  }

  private SplitsPointer constructSplitPointer(
      PartitionChunkListing chunkListing, DatasetConfig datasetConfig) {
    final List<PartitionChunk> chunkList = new ArrayList<>();
    chunkListing.iterator().forEachRemaining(chunkList::add);
    Preconditions.checkArgument(chunkList.size() == 1);
    // For a versioned iceberg table there is just one single split  - the manifest file.
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
        final PartitionProtobuf.PartitionChunk.Builder builder =
            PartitionProtobuf.PartitionChunk.newBuilder()
                .setSize(0)
                .setRowCount(0)
                .setPartitionExtendedProperty(
                    MetadataProtoUtils.toProtobuf(chunkList.get(0).getExtraInfo()))
                .addAllPartitionValues(
                    (chunkList.get(0).getPartitionValues().stream()
                        .map(MetadataProtoUtils::toProtobuf)
                        .collect(Collectors.toList())))
                .setDatasetSplit(
                    MetadataProtoUtils.toProtobuf(chunkList.get(0).getSplits().iterator().next()))
                .setSplitKey("0")
                .setSplitCount(1);

        final PartitionChunkMetadata partitionChunkMetadata =
            new PartitionChunkMetadataImpl(
                builder.build(), PartitionChunkId.of(datasetConfig, builder.build(), 0));
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

  /** TBD We need to figure out how to do the access check with dataplane */
  private boolean checkAccess(
      final List<String> tableKey, ResolvedVersionContext versionContext, String userName) {
    // TODO: Needs to be implemented
    return true;
  }

  private void setIcebergTableUUID(DatasetConfig datasetConfig, String tableUUID) {
    PhysicalDataset pds = datasetConfig.getPhysicalDataset();
    Preconditions.checkNotNull(pds);
    Preconditions.checkNotNull(pds.getIcebergMetadata());
    IcebergMetadata icebergMetadata = pds.getIcebergMetadata();
    icebergMetadata.setTableUuid(tableUUID);
    pds.setIcebergMetadata(icebergMetadata);
    datasetConfig.setPhysicalDataset(pds);
  }
}
