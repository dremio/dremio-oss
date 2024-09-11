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
package com.dremio.exec.store.mfunctions;

import static com.dremio.exec.catalog.MetadataObjectsUtils.toProtostuff;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.tablefunctions.MFunctionTranslatableTable;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/** Metadata provider for table functions as table_files */
public class TableFilesMFunctionTranslatableTableImpl extends MFunctionTranslatableTable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TableFilesMFunctionTranslatableTableImpl.class);

  /*
   * Underlying Table config. It should provide info like icebergMetadata, tablePath
   */
  private final DatasetConfig underlyingTableConfig;
  private final Supplier<Optional<DatasetHandle>> handleSupplier;
  private final StoragePlugin plugin;
  private final DatasetRetrievalOptions options;
  private static final long DEFAULT_RECORD_COUNT = 100_000L;

  public TableFilesMFunctionTranslatableTableImpl(
      MFunctionCatalogMetadata catalogMetadata,
      DatasetConfig underlyingTableConfig,
      Supplier<Optional<DatasetHandle>> handleSupplier,
      StoragePlugin plugin,
      DatasetRetrievalOptions options,
      String user) {
    super(catalogMetadata, user, true);
    this.underlyingTableConfig = underlyingTableConfig;
    this.handleSupplier = handleSupplier;
    this.plugin = plugin;
    this.options = options;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final Supplier<PartitionChunkListing> partitionChunkListing =
        Suppliers.memoize(this::getPartitionChunkListing);
    DatasetConfig config = getDatasetConfig(partitionChunkListing);
    long splitVersion =
        Optional.of(config)
            .map(DatasetConfig::getReadDefinition)
            .map(ReadDefinition::getSplitVersion)
            .orElse(0L);
    SplitsPointer splitsPointer =
        MaterializedSplitsPointer.oldObsoleteOf(
            splitVersion, toPartitionChunks(partitionChunkListing), 1);
    return new ScanCrel(
        context.getCluster(),
        context.getCluster().traitSetOf(Convention.NONE),
        catalogMetadata.getStoragePluginId(),
        new TableFilesFunctionTableMetadata(
            catalogMetadata.getStoragePluginId(),
            config,
            user,
            catalogMetadata.getBatchSchema(),
            splitsPointer),
        null,
        1.0d,
        ImmutableList.of(),
        false,
        false);
  }

  /**
   * It provides datasetConfig depends upon plugins. For nessie underlyingTableConfig would be null,
   * so it construct with dataset handle and partition chunks. For Filesystemplugin it constructs
   * with underlyingTableConfig using icebergMetadata and stats. For Hive/Glue it needs extrainfo
   * for readerType.
   *
   * @param partitionChunkListing
   * @return
   */
  private DatasetConfig getDatasetConfig(Supplier<PartitionChunkListing> partitionChunkListing) {
    if (underlyingTableConfig == null) {
      return createDatasetConfig(partitionChunkListing);
    }
    DatasetConfig config = createDatasetConfigUsingUnderlyingConfig();
    if (!(plugin instanceof FileSystemPlugin)) {
      config
          .getReadDefinition()
          .setExtendedProperty(
              toProtostuff(getDatasetMetadata(partitionChunkListing).getExtraInfo()));
    }
    return config;
  }

  /**
   * PartitionChunks for FileSystemPlugin can throw error if it tried to load iceberg table using
   * incorrect catalog. So constructing at raw.
   *
   * @return
   */
  private PartitionChunkListing getPartitionChunkListing() {
    try {
      if (plugin instanceof FileSystemPlugin && underlyingTableConfig != null) {
        String splitPath =
            underlyingTableConfig
                .getPhysicalDataset()
                .getIcebergMetadata()
                .getMetadataFileLocation();
        List<PartitionValue> partition = Collections.emptyList();
        IcebergProtobuf.IcebergDatasetSplitXAttr splitExtended =
            IcebergProtobuf.IcebergDatasetSplitXAttr.newBuilder().setPath(splitPath).build();
        List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
        DatasetSplit datasetSplit = DatasetSplit.of(splitAffinities, 0, 0, splitExtended::writeTo);
        PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
        partitionChunkListing.put(partition, datasetSplit);
        return partitionChunkListing;
      }
      Preconditions.checkArgument(handleSupplier.get().isPresent());
      return plugin.listPartitionChunks(
          handleSupplier.get().get(), options.asListPartitionChunkOptions(underlyingTableConfig));
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  private List<PartitionProtobuf.PartitionChunk> toPartitionChunks(
      Supplier<PartitionChunkListing> listingSupplier) {
    final Iterator<? extends PartitionChunk> chunks = listingSupplier.get().iterator();

    final List<PartitionProtobuf.PartitionChunk> toReturn = new ArrayList<>();
    int i = 0;
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();
      toReturn.addAll(
          MetadataObjectsUtils.newPartitionChunk(i + "-", chunk).collect(Collectors.toList()));
      i++;
    }

    return toReturn;
  }

  /**
   * @return new dataset config with existing icebergMetadata and FileConfig
   */
  private DatasetConfig createDatasetConfig(Supplier<PartitionChunkListing> listingSupplier) {
    Preconditions.checkArgument(handleSupplier.get().isPresent());
    final DatasetHandle handle = handleSupplier.get().get();
    final DatasetConfig toReturn = MetadataObjectsUtils.newShallowConfig(handle);
    toReturn.setName(
        "table_files"
            + "("
            + (handle.getDatasetPath().getName() + ")")); // eg -> table_files('table')
    final DatasetMetadata datasetMetadata = getDatasetMetadata(listingSupplier);

    MetadataObjectsUtils.overrideExtended(
        toReturn,
        datasetMetadata,
        Optional.empty(),
        DEFAULT_RECORD_COUNT,
        options.maxMetadataLeafColumns());
    // For iceberg, data files contents fetched from Manifest file which is of AVRO type.
    // To understand the datasetConfig correctly, it should be set to correct fileType instead of
    // native iceberg.
    if (toReturn.getPhysicalDataset() == null
        || toReturn.getPhysicalDataset().getIcebergMetadata() == null) {
      PhysicalDataset physicalDataset = new PhysicalDataset();
      physicalDataset.setIcebergMetadata(
          underlyingTableConfig.getPhysicalDataset().getIcebergMetadata());
      toReturn.setPhysicalDataset(physicalDataset);
    }
    if (toReturn.getPhysicalDataset().getFormatSettings() != null) {
      toReturn.getPhysicalDataset().getFormatSettings().setType(FileType.AVRO);
    }
    // Set recordCount with DataFiles, which is present with manifestStats
    if (datasetMetadata.getManifestStats() != null) {
      final long datasetRecordCount = datasetMetadata.getManifestStats().getRecordCount();
      toReturn
          .getReadDefinition()
          .setScanStats(
              new ScanStats()
                  .setScanFactor(datasetMetadata.getManifestStats().getScanFactor())
                  .setType(
                      datasetMetadata.getManifestStats().isExactRecordCount()
                          ? ScanStatsType.EXACT_ROW_COUNT
                          : ScanStatsType.NO_EXACT_ROW_COUNT)
                  .setRecordCount(
                      datasetRecordCount >= 0 ? datasetRecordCount : DEFAULT_RECORD_COUNT));
    } else {
      toReturn
          .getReadDefinition()
          .setScanStats(
              new ScanStats()
                  .setScanFactor(ScanCostFactor.EASY.getFactor())
                  .setType(ScanStatsType.NO_EXACT_ROW_COUNT)
                  .setRecordCount(DEFAULT_RECORD_COUNT));
    }

    return toReturn;
  }

  private DatasetMetadata getDatasetMetadata(Supplier<PartitionChunkListing> listingSupplier) {
    try {
      return plugin.getDatasetMetadata(
          handleSupplier.get().get(), listingSupplier.get(), options.asGetMetadataOptions(null));
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  /**
   * Here we are using the information that is already present in the underlying configuration,
   * which in turn is present in kv store. This is being used only for internal iceberg tables.
   *
   * @return
   */
  public DatasetConfig createDatasetConfigUsingUnderlyingConfig() {
    final DatasetConfig shallowConfig = new DatasetConfig();

    shallowConfig.setId(new EntityId().setId(UUID.randomUUID().toString()));
    shallowConfig.setCreatedAt(System.currentTimeMillis());
    shallowConfig.setName(underlyingTableConfig.getName());
    shallowConfig.setFullPathList(underlyingTableConfig.getFullPathList());
    shallowConfig.setType(underlyingTableConfig.getType());
    shallowConfig.setSchemaVersion(underlyingTableConfig.getSchemaVersion());
    final BatchSchema batchSchema = IcebergMetadataFunctionsSchema.getTableFilesRecordSchema();
    shallowConfig.setRecordSchema(batchSchema.toByteString());

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setScanStats(underlyingTableConfig.getReadDefinition().getManifestScanStats());
    readDefinition.setManifestScanStats(
        underlyingTableConfig.getReadDefinition().getManifestScanStats());
    shallowConfig.setReadDefinition(readDefinition);

    shallowConfig.setPhysicalDataset(underlyingTableConfig.getPhysicalDataset());

    if (shallowConfig.getPhysicalDataset() != null
        && shallowConfig.getPhysicalDataset().getFormatSettings() != null) {
      shallowConfig.getPhysicalDataset().getFormatSettings().setType(FileType.AVRO);
    }

    if (options.getTimeTravelRequest() instanceof TimeTravelOption.TimestampRequest) {
      throw UserException.validationError()
          .message(
              "TIMESTAMP syntax is not supported on internal iceberg table with table_files function.")
          .buildSilently();
    }

    if (options.getTimeTravelRequest() instanceof TimeTravelOption.SnapshotIdRequest) {
      Long snapshotId =
          Long.parseLong(
              ((TimeTravelOption.SnapshotIdRequest) options.getTimeTravelRequest())
                  .getSnapshotId());
      shallowConfig.getPhysicalDataset().getIcebergMetadata().setSnapshotId(snapshotId);
    }
    return shallowConfig;
  }
}
