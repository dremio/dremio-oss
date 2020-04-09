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
package com.dremio.exec.store.hive.metadata;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.InputFormat;

import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class MetadataAccumulator {
  private final HiveDatasetStats datasetStats;

  private final List<HiveReaderProto.FileSystemPartitionUpdateKey> fileSystemPartitionUpdateKeys;
  private final List<Integer> partitionHashes;

  private final DictionaryBuilder<String> inputFormatDictionary;
  private final DictionaryBuilder<String> serializationLibDictionary;
  private final DictionaryBuilder<String> storageHandlerDictionary;
  private final DictionaryBuilder<Prop> propertyDictionary;

  private boolean allFSBasedPartitions;
  private boolean allowParquetNative;

  public MetadataAccumulator() {
    this.datasetStats = new HiveDatasetStats();
    this.fileSystemPartitionUpdateKeys = new ArrayList<>();
    this.partitionHashes = new ArrayList<>();
    this.allFSBasedPartitions = true;
    this.allowParquetNative = true;

    inputFormatDictionary = new DictionaryBuilder<>(String.class);
    serializationLibDictionary = new DictionaryBuilder<>(String.class);
    storageHandlerDictionary = new DictionaryBuilder<>(String.class);
    propertyDictionary = new DictionaryBuilder<>(Prop.class);
  }

  public void accumulateFileSystemPartitionUpdateKey(HiveReaderProto.FileSystemPartitionUpdateKey fileSystemPartitionUpdateKey) {
    fileSystemPartitionUpdateKeys.add(fileSystemPartitionUpdateKey);
  }

  public PartitionXattr buildPartitionXattrDictionaries(Partition partition, List<Prop> props) {
    final PartitionXattr.Builder partitionXattrBuilder = PartitionXattr.newBuilder();

    if (partition.getSd().getInputFormat() != null) {
      partitionXattrBuilder.setInputFormatSubscript(
        inputFormatDictionary.getOrCreateSubscript(partition.getSd().getInputFormat()));
    }

    String metaTableStorage = partition.getParameters().get(META_TABLE_STORAGE);
    if (metaTableStorage != null) {
      partitionXattrBuilder.setStorageHandlerSubscript(
        storageHandlerDictionary.getOrCreateSubscript(metaTableStorage));
    }

    if (partition.getSd().getSerdeInfo().getSerializationLib() != null) {
      partitionXattrBuilder.setSerializationLibSubscript(
        serializationLibDictionary.getOrCreateSubscript(partition.getSd().getSerdeInfo().getSerializationLib()));
    }

    for (Prop prop : props) {
      partitionXattrBuilder.addPropertySubscript(propertyDictionary.getOrCreateSubscript(prop));
    }

    return partitionXattrBuilder.build();
  }

  public int getTableInputFormatSubscript(String inputFormat) {
    Preconditions.checkNotNull(inputFormat);
    return inputFormatDictionary.getOrCreateSubscript(inputFormat);
  }

  public int getTableStorageHandlerSubscript(String inputFormat) {
    Preconditions.checkNotNull(inputFormat);
    return storageHandlerDictionary.getOrCreateSubscript(inputFormat);
  }

  public int getTableSerializationLibSubscript(String inputFormat) {
    Preconditions.checkNotNull(inputFormat);
    return serializationLibDictionary.getOrCreateSubscript(inputFormat);
  }

  public int getTablePropSubscript(Prop prop) {
    Preconditions.checkNotNull(prop);

    return propertyDictionary.getOrCreateSubscript(prop);
  }

  public void accumulatePartitionHash(Partition partition) {
    partitionHashes.add(HiveMetadataUtils.getHash(partition));
  }

  public void accumulateReaderType(Class<? extends InputFormat> inputFormat) {
    allowParquetNative = HiveMetadataUtils.allowParquetNative(allowParquetNative, inputFormat);
  }

  public void accumulateTotalEstimatedRecords(long splitEstimatedRecords) {
    datasetStats.addRecords(splitEstimatedRecords);
  }

  public void accumulateTotalBytesToScanFactor(long splitEstimatedRecords) {
    datasetStats.addBytesToScanFactor(splitEstimatedRecords);
  }

  public HiveDatasetStats getDatasetStats() {
    return datasetStats;
  }

  public List<HiveReaderProto.FileSystemPartitionUpdateKey> getFileSystemPartitionUpdateKeys() {
    return fileSystemPartitionUpdateKeys;
  }

  public boolean allFSBasedPartitions() {
    return allFSBasedPartitions;
  }

  public HiveReaderProto.ReaderType getReaderType() {
    if (allowParquetNative) {
      return HiveReaderProto.ReaderType.NATIVE_PARQUET;
    } else {
      return HiveReaderProto.ReaderType.BASIC;
    }
  }

  public void setNotAllFSBasedPartitions() {
    this.allFSBasedPartitions = false;
  }

  public int getPartitionHash() {
    if (partitionHashes.isEmpty()) {
      return 0;
    }

    Collections.sort(partitionHashes);
    return Objects.hashCode(partitionHashes);
  }

  public Iterable<String> buildInputFormatDictionary() {
    return inputFormatDictionary.build();
  }

  public Iterable<String> buildSerializationLibDictionary() {
    return serializationLibDictionary.build();
  }

  public Iterable<String> buildStorageHandlerDictionary() {
    return storageHandlerDictionary.build();
  }

  public Iterable<? extends Prop> buildPropertyDictionary() {
    return propertyDictionary.build();
  }
}
