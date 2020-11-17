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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.proto.EntityId;

import io.protostuff.ByteString;
import io.protostuff.ByteStringUtil;

/**
 * Utility functions to convert from metadata object provided by source connectors to metadata object stored in
 * the catalog.
 */
public final class MetadataObjectsUtils {

  /**
   * Creates a serializable {@link DatasetConfig} for the given handle. The returned object is "shallow", effectively
   * setting only the name of the dataset.
   *
   * @param handle dataset handle
   * @return dataset config
   */
  public static DatasetConfig newShallowConfig(DatasetHandle handle) {
    final DatasetConfig shallowConfig = new DatasetConfig();

    shallowConfig.setId(new EntityId()
        .setId(UUID.randomUUID().toString()));
    shallowConfig.setCreatedAt(System.currentTimeMillis());
    shallowConfig.setName(handle.getDatasetPath().getName());
    shallowConfig.setFullPathList(handle.getDatasetPath().getComponents());
    if (handle instanceof DatasetTypeHandle) {
      shallowConfig.setType(((DatasetTypeHandle) handle).getDatasetType());
    } else {
      shallowConfig.setType(DatasetType.PHYSICAL_DATASET);
    }

    return shallowConfig;
  }

  /**
   * Overrides attributes of the given serializable dataset config with the corresponding values from the given
   * extended dataset metadata. Optionally sets the read signature if one is provided.
   * @param datasetConfig dataset config
   * @param newExtended   extended metadata
   * @param newSignature  optional read signature
   * @param newRecordCount   the number of records for sources that do not support record counts in stats.
   * @param maxLeafFields the number of leaf fields permitted
   */
  public static void overrideExtended(
    DatasetConfig datasetConfig,
    DatasetMetadata newExtended,
    Optional<ByteString> newSignature,
    long newRecordCount,
    int maxLeafFields) {
    // 1. preserve shallow and misc attributes as is
    // no-op

    // 2. override extended metadata attributes
    datasetConfig.setSchemaVersion(datasetConfig.getSchemaVersion());  // TODO: how to detect changes?
    final BatchSchema batchSchema = new BatchSchema(newExtended.getRecordSchema().getFields());
    if (batchSchema.getTotalFieldCount() > maxLeafFields) {
      throw new ColumnCountTooLargeException(maxLeafFields);
    }
    datasetConfig.setRecordSchema(new BatchSchema(newExtended.getRecordSchema().getFields()).toByteString());

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setPartitionColumnsList(newExtended.getPartitionColumns());
    readDefinition.setSortColumnsList(newExtended.getSortColumns()); // TODO: not required from connectors
    readDefinition.setExtendedProperty(toProtostuff(newExtended.getExtraInfo()));
    final DatasetStats datasetStats = newExtended.getDatasetStats();
    final long datasetRecordCount = datasetStats.getRecordCount();
    readDefinition.setScanStats(new ScanStats()
        .setScanFactor(datasetStats.getScanFactor())
        .setType(datasetStats.isExactRecordCount() ? ScanStatsType.EXACT_ROW_COUNT : ScanStatsType.NO_EXACT_ROW_COUNT)
        .setRecordCount(datasetRecordCount >= 0 ? datasetRecordCount : newRecordCount)
    );

    if (newExtended instanceof FileConfigMetadata) {
      PhysicalDataset pds = datasetConfig.getPhysicalDataset();
      if (pds == null) {
        pds = new PhysicalDataset();
      }
      FileConfig fileConfig = ((FileConfigMetadata) newExtended).getFileConfig();
      if (pds.getFormatSettings() != null) {
        fileConfig.setFullPathList(pds.getFormatSettings().getFullPathList());
      }
      pds.setFormatSettings(fileConfig);
      datasetConfig.setPhysicalDataset(pds);
    }

    newSignature.ifPresent(bs -> readDefinition.setReadSignature(ByteStringUtil.wrap(bs.toByteArray())));

    datasetConfig.setReadDefinition(readDefinition);
  }

  /**
   * Converts the given partition chunk into a stream of serializable partition chunks.
   *
   * @param splitPrefix split prefix
   * @param partitionChunk partition chunk
   * @return partition chunk stream
   */
  @Deprecated // this will end up creating a legacy partition chunk metadata impl, which will be handled correctly
  public static Stream<PartitionProtobuf.PartitionChunk>
  newPartitionChunk(String splitPrefix, PartitionChunk partitionChunk) {

    final List<PartitionProtobuf.PartitionValue> values = partitionChunk.getPartitionValues()
        .stream()
        .map(MetadataProtoUtils::toProtobuf)
        .collect(Collectors.toList());
    final Pointer<Long> id = new Pointer<>(0L);
    return StreamSupport.stream(Spliterators.spliterator(partitionChunk.getSplits().iterator(), 0, 0), false)
        .map(datasetSplit -> {
          PartitionProtobuf.PartitionChunk.Builder builder = PartitionProtobuf.PartitionChunk.newBuilder();
          builder.addAllPartitionValues(values);
          builder.addAllAffinities(datasetSplit.getAffinities()
              .stream()
              .map(MetadataProtoUtils::toProtobuf)
              .collect(Collectors.toList()));

          if (datasetSplit.getExtraInfo() != BytesOutput.NONE) {
            builder.setPartitionExtendedProperty(MetadataProtoUtils.toProtobuf(datasetSplit.getExtraInfo()));
          }

          builder.setRowCount(datasetSplit.getRecordCount());
          builder.setSize(datasetSplit.getSizeInBytes());
          builder.setSplitKey(splitPrefix + id.value);
          id.value++;
          return builder.build();
        });
  }

  /**
   * Converts the given bytes output to a serializable byte string.
   *
   * @param out bytes output
   * @return byte string
   */
  static ByteString toProtostuff(BytesOutput out) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      out.writeTo(output);
      return ByteStringUtil.wrap(output.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts the given entity path into a namespace key.
   *
   * @param entityPath entity path
   * @return namespace key
   */
  public static NamespaceKey toNamespaceKey(EntityPath entityPath) {
    return new NamespaceKey(entityPath.getComponents());
  }

  /**
   * Converts the given namespace key into an entity path.
   *
   * @param namespaceKey namespace key
   * @return entity path
   */
  public static EntityPath toEntityPath(NamespaceKey namespaceKey) {
    return new EntityPath(namespaceKey.getPathComponents());
  }

  // prevent instantiation
  private MetadataObjectsUtils() {
  }
}
