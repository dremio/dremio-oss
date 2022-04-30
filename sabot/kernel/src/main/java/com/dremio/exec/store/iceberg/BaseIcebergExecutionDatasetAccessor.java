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

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.extensions.SupportsIcebergMetadata;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergDatasetXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetXAttr;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;

/**
 * Base iceberg metadata accessor.
 */
public abstract class BaseIcebergExecutionDatasetAccessor implements FileDatasetHandle {

  private final EntityPath entityPath;
  private final Supplier<Table> tableSupplier;
  private final Configuration configuration;
  private final TableSnapshotProvider tableSnapshotProvider;
  private final MutablePlugin plugin;

  protected BaseIcebergExecutionDatasetAccessor(
      EntityPath entityPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      TableSnapshotProvider tableSnapshotProvider,
      MutablePlugin plugin
  ) {
    this.entityPath = entityPath;
    this.tableSupplier = tableSupplier;
    this.configuration = configuration;
    this.tableSnapshotProvider = tableSnapshotProvider;
    this.plugin = plugin;
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

    long numRecords = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-records", "0")) : 0L;
    long numDataFiles = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-data-files", "0")) : 0L;
    long numDeleteFiles = snapshot != null ?
      Long.parseLong(snapshot.summary().getOrDefault("total-delete-files", "0")) : 0L;

    // DX-41522, throw exception when DeleteFiles are present.
    // TODO: remove this throw when we get full support for handling the deletes correctly.
    if (numDeleteFiles > 0) {
      throw UserException.unsupportedError()
        .message("Iceberg V2 tables with delete files are not supported")
        .buildSilently();
    }

    final FileConfig fileConfig = getFileConfig();
    final DatasetStats datasetStats = DatasetStats.of(numRecords, true, ScanCostFactor.PARQUET.getFactor());
    final DatasetStats manifestStats = DatasetStats.of(numDataFiles, ScanCostFactor.EASY.getFactor());

    final SchemaConverter schemaConverter = new SchemaConverter(table.name());
    final BatchSchema batchSchema = schemaConverter.fromIceberg(table.schema());

    final List<String> partitionColumns = schemaConverter.getPartitionColumns(table);

    final ParquetDatasetXAttr.Builder builder = ParquetDatasetXAttr.newBuilder();
    builder.setSelectionRoot(table.location());

    final IcebergDatasetXAttr.Builder icebergDatasetBuilder = IcebergDatasetXAttr.newBuilder();
    icebergDatasetBuilder.setParquetDatasetXAttr(builder.build());
    final Map<String, Integer> schemaNameIDMap = IcebergUtils.getIcebergColumnNameToIDMap(table.schema());
    schemaNameIDMap.forEach((k, v) -> icebergDatasetBuilder.addColumnIds(
        IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath(k).setId(v).build()
    ));

    if (snapshot != null && !table.spec().isUnpartitioned()) {
      String partitionStatsFile = IcebergUtils.getPartitionStatsFile(
          getMetadataLocation(),
          snapshot.snapshotId(),
          new Configuration(configuration), plugin);
      if (partitionStatsFile != null) {
        icebergDatasetBuilder.setPartitionStatsFile(partitionStatsFile);
      }
    }
    final BytesOutput extraInfo = icebergDatasetBuilder.build()::writeTo;

    final Map<Integer, PartitionSpec> specsMap = table.specs();
    final byte[] specs = IcebergSerDe.serializePartitionSpecMap(specsMap);
    final BytesOutput partitionSpecs = os -> os.write(specs);

    final String metadataFileLocation = getMetadataLocation();
    final long snapshotId = snapshot != null ? snapshot.snapshotId() : -1;

    return new DatasetMetadataImpl(fileConfig, datasetStats, manifestStats, batchSchema, partitionColumns, extraInfo,
        metadataFileLocation, snapshotId, partitionSpecs);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) {
    String splitPath = getMetadataLocation();
    List<PartitionValue> partition = Collections.emptyList();
    IcebergProtobuf.IcebergDatasetSplitXAttr splitExtended = IcebergProtobuf.IcebergDatasetSplitXAttr.newBuilder()
      .setPath(splitPath)
      .build();
    List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
    DatasetSplit datasetSplit = DatasetSplit.of(
      splitAffinities, 0, 0, splitExtended::writeTo);
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(partition, datasetSplit);
    return partitionChunkListing;
  }

  @Override
  public abstract BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException;

  protected abstract FileConfig getFileConfig();

  private static class DatasetMetadataImpl implements FileConfigMetadata, SupportsIcebergMetadata {

    private final FileConfig fileConfig;
    private final DatasetStats datasetStats;
    private final DatasetStats manifestStats;
    private final org.apache.arrow.vector.types.pojo.Schema batchSchema;
    private final List<String> partitionColumns;
    private final BytesOutput extraInfo;
    private final String metadataFileLocation;
    private final long snapshotId;
    private final BytesOutput partitionSpecs;

    private DatasetMetadataImpl(
        FileConfig fileConfig,
        DatasetStats datasetStats,
        DatasetStats manifestStats,
        Schema batchSchema,
        List<String> partitionColumns,
        BytesOutput extraInfo,
        String metadataFileLocation,
        long snapshotId,
        BytesOutput partitionSpecs
    ) {
      this.fileConfig = fileConfig;
      this.datasetStats = datasetStats;
      this.manifestStats = manifestStats;
      this.batchSchema = batchSchema;
      this.partitionColumns = partitionColumns;
      this.extraInfo = extraInfo;
      this.metadataFileLocation = metadataFileLocation;
      this.snapshotId = snapshotId;
      this.partitionSpecs = partitionSpecs;
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
  }
}
