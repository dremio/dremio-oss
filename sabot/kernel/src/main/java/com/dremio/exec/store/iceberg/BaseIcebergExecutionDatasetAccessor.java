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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetMetadataVerifyResult;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.extensions.SupportsIcebergMetadata;
import com.dremio.connector.metadata.options.MetadataVerifyRequest;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.MetadataVerifyHandle;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergDatasetXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetXAttr;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;

/** Base iceberg metadata accessor. */
public abstract class BaseIcebergExecutionDatasetAccessor
    implements FileDatasetHandle, MetadataVerifyHandle {

  private final EntityPath entityPath;
  private final Supplier<Table> tableSupplier;
  private final Configuration configuration;
  private final TableSnapshotProvider tableSnapshotProvider;
  private final MutablePlugin plugin;
  private final TableSchemaProvider tableSchemaProvider;
  private final OptionResolver optionResolver;

  protected BaseIcebergExecutionDatasetAccessor(
      EntityPath entityPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      TableSnapshotProvider tableSnapshotProvider,
      MutablePlugin plugin,
      TableSchemaProvider tableSchemaProvider,
      OptionResolver optionResolver) {
    this.entityPath = entityPath;
    this.tableSupplier = tableSupplier;
    this.configuration = configuration;
    this.tableSnapshotProvider = tableSnapshotProvider;
    this.plugin = plugin;
    this.tableSchemaProvider = tableSchemaProvider;
    this.optionResolver = optionResolver;
  }

  protected String getMetadataLocation() {
    final Table table = tableSupplier.get();
    Preconditions.checkArgument(table instanceof HasTableOperations);
    return ((HasTableOperations) table).operations().current().metadataFileLocation();
  }

  @Override
  public EntityPath getDatasetPath() {
    return entityPath;
  }

  @Override
  public DatasetType getDatasetType() {
    return PHYSICAL_DATASET_SOURCE_FOLDER;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options) {
    final Table table = tableSupplier.get();
    final Snapshot snapshot = tableSnapshotProvider.apply(table);

    logger.debug("Getting Iceberg snapshot {}", snapshot);

    Map<String, String> summary =
        Optional.ofNullable(snapshot).map(Snapshot::summary).orElseGet(ImmutableMap::of);
    long numRecords = Long.parseLong(summary.getOrDefault("total-records", "0"));
    long numDataFiles = Long.parseLong(summary.getOrDefault("total-data-files", "0"));
    long numPositionDeletes = Long.parseLong(summary.getOrDefault("total-position-deletes", "0"));
    long numEqualityDeletes = Long.parseLong(summary.getOrDefault("total-equality-deletes", "0"));
    long numDeleteFiles = Long.parseLong(summary.getOrDefault("total-delete-files", "0"));
    long lastModTime = snapshot != null ? snapshot.timestampMillis() : 0L;

    if (numEqualityDeletes > 0
        && !optionResolver.getOption(
            ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE)) {
      throw UserException.unsupportedError()
          .message("Iceberg V2 tables with equality deletes are not supported.")
          .buildSilently();
    }

    final FileConfig fileConfig = getFileConfig();
    final DatasetStats datasetStats =
        DatasetStats.of(numRecords, true, ScanCostFactor.PARQUET.getFactor());
    final DatasetStats manifestStats =
        DatasetStats.of(numDataFiles, ScanCostFactor.EASY.getFactor());
    final DatasetStats deleteStats =
        DatasetStats.of(
            numPositionDeletes + numEqualityDeletes, ScanCostFactor.PARQUET.getFactor());
    final DatasetStats equalityDeleteStats =
        DatasetStats.of(numEqualityDeletes, ScanCostFactor.PARQUET.getFactor());
    final DatasetStats deleteManifestStats =
        DatasetStats.of(numDeleteFiles, ScanCostFactor.EASY.getFactor());

    final SchemaConverter schemaConverter =
        SchemaConverter.getBuilder()
            .setTableName(table.name())
            .setMapTypeEnabled(optionResolver.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
            .build();
    org.apache.iceberg.Schema schema = tableSchemaProvider.apply(table, snapshot);
    final BatchSchema batchSchema = schemaConverter.fromIceberg(schema);

    final List<String> partitionColumns = schemaConverter.getPartitionColumns(table);

    final ParquetDatasetXAttr.Builder builder = ParquetDatasetXAttr.newBuilder();
    builder.setSelectionRoot(table.location());

    final IcebergDatasetXAttr.Builder icebergDatasetBuilder = IcebergDatasetXAttr.newBuilder();
    icebergDatasetBuilder.setParquetDatasetXAttr(builder.build());
    final Map<String, Integer> schemaNameIDMap =
        IcebergUtils.getIcebergColumnNameToIDMap(table.schema());
    schemaNameIDMap.forEach(
        (k, v) ->
            icebergDatasetBuilder.addColumnIds(
                IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath(k).setId(v).build()));

    if (snapshot != null && !table.spec().isUnpartitioned()) {
      ImmutableDremioFileAttrs partitionStatsFileAttrs =
          IcebergUtils.getPartitionStatsFileAttrs(
              getMetadataLocation(), snapshot.snapshotId(), table.io());
      if (partitionStatsFileAttrs.fileName() != null) {
        icebergDatasetBuilder.setPartitionStatsFile(partitionStatsFileAttrs.fileName());
        icebergDatasetBuilder.setPartitionStatsFileSize(partitionStatsFileAttrs.fileLength());
      }
    }
    final BytesOutput extraInfo = icebergDatasetBuilder.build()::writeTo;

    Map<Integer, PartitionSpec> specsMap = table.specs();
    specsMap = IcebergUtils.getPartitionSpecMapBySchema(specsMap, schema);
    final byte[] specs = IcebergSerDe.serializePartitionSpecAsJsonMap(specsMap);
    SortOrder sortOrder = table.sortOrder();
    final String sortOrderJson = IcebergSerDe.serializeSortOrderAsJson(sortOrder);
    final String icebergSchema = serializedSchemaAsJson(schema);
    final BytesOutput partitionSpecs = os -> os.write(specs);
    final Map<String, String> tableProperties = table.properties();

    final String metadataFileLocation = getMetadataLocation();
    final long snapshotId = snapshot != null ? snapshot.snapshotId() : -1;

    return new DatasetMetadataImpl(
        fileConfig,
        datasetStats,
        manifestStats,
        deleteStats,
        equalityDeleteStats,
        deleteManifestStats,
        batchSchema,
        partitionColumns,
        extraInfo,
        metadataFileLocation,
        snapshotId,
        partitionSpecs,
        sortOrderJson,
        icebergSchema,
        lastModTime,
        tableProperties,
        table.spec().specId());
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) {
    String splitPath = getMetadataLocation();
    List<PartitionValue> partition = Collections.emptyList();
    IcebergProtobuf.IcebergDatasetSplitXAttr splitExtended =
        IcebergProtobuf.IcebergDatasetSplitXAttr.newBuilder().setPath(splitPath).build();
    List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
    DatasetSplit datasetSplit = DatasetSplit.of(splitAffinities, 0, 0, splitExtended::writeTo);
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(partition, datasetSplit);
    return partitionChunkListing;
  }

  @Nonnull
  @Override
  public Optional<DatasetMetadataVerifyResult> verifyMetadata(
      MetadataVerifyRequest metadataVerifyRequest) {
    return IcebergMetadataVerifyProcessors.verify(metadataVerifyRequest, tableSupplier.get());
  }

  @Override
  public abstract BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException;

  protected abstract FileConfig getFileConfig();

  private static class DatasetMetadataImpl implements FileConfigMetadata, SupportsIcebergMetadata {

    private final FileConfig fileConfig;
    private final DatasetStats datasetStats;
    private final DatasetStats manifestStats;
    private final DatasetStats deleteStats;
    private final DatasetStats equalityDeleteStats;
    private final DatasetStats deleteManifestStats;
    private final org.apache.arrow.vector.types.pojo.Schema batchSchema;
    private final List<String> partitionColumns;
    private final BytesOutput extraInfo;
    private final String metadataFileLocation;
    private final long snapshotId;
    private final BytesOutput partitionSpecs;
    private final String sortOrder;
    private final String icebergSchema;
    private final long modificationTime;
    private final Map<String, String> tableProperties;
    private final int defaultPartitionSpecId;

    private DatasetMetadataImpl(
        FileConfig fileConfig,
        DatasetStats datasetStats,
        DatasetStats manifestStats,
        DatasetStats deleteStats,
        DatasetStats equalityDeleteStats,
        DatasetStats deleteManifestStats,
        Schema batchSchema,
        List<String> partitionColumns,
        BytesOutput extraInfo,
        String metadataFileLocation,
        long snapshotId,
        BytesOutput partitionSpecs,
        String sortOrder,
        String icebergSchema,
        long modificationTime,
        Map<String, String> tableProperties,
        int defaultPartitionSpecId) {
      this.fileConfig = fileConfig;
      this.datasetStats = datasetStats;
      this.manifestStats = manifestStats;
      this.deleteStats = deleteStats;
      this.equalityDeleteStats = equalityDeleteStats;
      this.deleteManifestStats = deleteManifestStats;
      this.batchSchema = batchSchema;
      this.partitionColumns = partitionColumns;
      this.extraInfo = extraInfo;
      this.metadataFileLocation = metadataFileLocation;
      this.snapshotId = snapshotId;
      this.partitionSpecs = partitionSpecs;
      this.sortOrder = sortOrder;
      this.icebergSchema = icebergSchema;
      this.modificationTime = modificationTime;
      this.tableProperties = tableProperties;
      this.defaultPartitionSpecId = defaultPartitionSpecId;
    }

    @Override
    public FileConfig getFileConfig() {
      return fileConfig;
    }

    @Override
    public DatasetStats getDatasetStats() {
      return datasetStats;
    }

    @Override
    public DatasetStats getManifestStats() {
      return manifestStats;
    }

    @Override
    public DatasetStats getDeleteStats() {
      return deleteStats;
    }

    @Override
    public DatasetStats getEqualityDeleteStats() {
      return equalityDeleteStats;
    }

    @Override
    public DatasetStats getDeleteManifestStats() {
      return deleteManifestStats;
    }

    @Override
    public long getMtime() {
      return modificationTime;
    }

    @Override
    public org.apache.arrow.vector.types.pojo.Schema getRecordSchema() {
      return batchSchema;
    }

    @Override
    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    @Override
    public BytesOutput getExtraInfo() {
      return extraInfo;
    }

    @Override
    public String getMetadataFileLocation() {
      return metadataFileLocation;
    }

    @Override
    public long getSnapshotId() {
      return snapshotId;
    }

    @Override
    public BytesOutput getPartitionSpecs() {
      return partitionSpecs;
    }

    @Override
    public String getSortOrder() {
      return sortOrder;
    }

    @Override
    public String getIcebergSchema() {
      return icebergSchema;
    }

    @Override
    public Map<String, String> getTableProperties() {
      return tableProperties;
    }

    @Override
    public int getDefaultPartitionSpecId() {
      return defaultPartitionSpecId;
    }
  }
}
