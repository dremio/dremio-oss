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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

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
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.Options;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Throwables;

/**
 * Dataset handle for Iceberg format that supports Iceberg execution model
 */
@Options
public class IcebergExecutionDatasetAccessor implements FileDatasetHandle {
  private final NamespaceKey tableSchemaPath;
  private final DatasetType type;
  private final FileSystemPlugin<?> fsPlugin;
  private final FileSelection fileSelection;
  private final FormatPlugin formatPlugin;
  private final FileSystem fs;
  public IcebergExecutionDatasetAccessor(DatasetType type,
                                         FileSystem fs,
                                         FormatPlugin formatPlugin,
                                         FileSelection fileSelection,
                                         FileSystemPlugin<?> fsPlugin,
                                         NamespaceKey tableSchemaPath) {
    this.fs = fs;
    this.tableSchemaPath = tableSchemaPath;
    this.type = type;
    this.fsPlugin = fsPlugin;
    this.fileSelection = fileSelection;
    this.formatPlugin = formatPlugin;
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException {
    Table table = (new HadoopTables(this.fsPlugin.getFsConfCopy())).load(fileSelection.getSelectionRoot());
    // TODO: Using iceberg scan schema instead of table schema. Once parquet scan fragment is introduced
    // in the plan, this will change to use table schema.
    //BatchSchema batchSchema = new SchemaConverter().fromIceberg(table.schema());
    BatchSchema batchSchema = RecordReader.SPLIT_GEN_SCAN_SCHEMA;
    long numRecords = Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-records", "0"));

    return new FileConfigMetadata() {

      @Override
      public FileConfig getFileConfig() {
        return PhysicalDatasetUtils.toFileFormat(formatPlugin).asFileConfig().setLocation(fileSelection.getSelectionRoot());
      }

      @Override
      public DatasetStats getDatasetStats() {
        return DatasetStats.of(numRecords, true, ScanCostFactor.PARQUET.getFactor());
      }

      @Override
      public org.apache.arrow.vector.types.pojo.Schema getRecordSchema() {
        return batchSchema;
      }

      @Override
      public List<String> getPartitionColumns() {
        return table
          .spec()
          .fields()
          .stream()
          .map(PartitionField::sourceId)
          .map(table.schema()::findColumnName) // column name from schema
          .collect(Collectors.toList());
      }

      @Override
      public BytesOutput getExtraInfo() {
        IcebergProtobuf.IcebergDatasetXAttr.Builder icebergDatasetBuilder = IcebergProtobuf.IcebergDatasetXAttr.newBuilder();
        ParquetProtobuf.ParquetDatasetXAttr.Builder builder = ParquetProtobuf.ParquetDatasetXAttr.newBuilder();
        builder.setSelectionRoot(fileSelection.getSelectionRoot());
        icebergDatasetBuilder.setParquetDatasetXAttr(builder.build());
        Map<String, Integer> schemaNameIDMap = IcebergUtils.getIcebergColumnNameToIDMap(table.schema());
        schemaNameIDMap.forEach((k, v) -> icebergDatasetBuilder.addColumnIds(
          IcebergProtobuf.IcebergSchemaField.newBuilder().setSchemaPath(k).setId(v).build()
        ));
        return icebergDatasetBuilder.build()::writeTo;
      }
    };
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException {
    List<PartitionValue> partition = Collections.emptyList();
    EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
      .setPath(fileSelection.getSelectionRoot())
      .setStart(0)
      .setLength(0)
      .setUpdateKey(FileProtobuf.FileSystemCachedEntity.newBuilder()
        .setPath(fileSelection.getSelectionRoot())
        .setLastModificationTime(0))
      .build();
    List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
    DatasetSplit datasetSplit = DatasetSplit.of(
      splitAffinities, 0, 0, splitExtended::writeTo);
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(partition, datasetSplit);
    return partitionChunkListing;
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    try {
      Path metaDir = Path.of(fileSelection.getSelectionRoot()).resolve(IcebergFormatMatcher.METADATA_DIR_NAME);
      if (!fs.exists(metaDir) || !fs.isDirectory(metaDir)) {
        throw new IllegalStateException("missing metadata dir for iceberg table");
      }

      final FileAttributes attributes = fs.getFileAttributes(metaDir);
      final FileProtobuf.FileSystemCachedEntity cachedEntity = FileProtobuf.FileSystemCachedEntity
        .newBuilder()
        .setPath(metaDir.toString())
        .setLastModificationTime(attributes.lastModifiedTime().toMillis())
        .setLength(attributes.size())
        .build();

      return FileProtobuf.FileUpdateKey
        .newBuilder()
        .addCachedEntities(cachedEntity)
        .build()::writeTo;
    }
    catch (IOException ioe) {
      Throwables.propagateIfPossible(ioe, ConnectorException.class);
      throw new ConnectorException(ioe);
    }
  }

}
