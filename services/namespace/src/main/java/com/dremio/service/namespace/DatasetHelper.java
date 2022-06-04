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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;

import io.protostuff.ByteString;

/**
 * Dataset definition helper.
 */
public final class DatasetHelper {
  public static final int NO_VERSION = 0;
  public static final int CURRENT_VERSION = 1;

  private DatasetHelper(){}

  /**
   * Retrieve the schema bytes for a config. Manages different locations due to legacy storage.
   * @param config DatasetConfig to view.
   * @return ByteString for schema if property found. Otherwise null.
   */
  public static ByteString getSchemaBytes(DatasetConfig config){
    ByteString recordSchema = config.getRecordSchema();
    if(recordSchema == null && config.getPhysicalDataset() != null){
      recordSchema = config.getPhysicalDataset().getDeprecatedDatasetSchema();
    }
    return recordSchema;
  }

  /**
   * @return true if the dataset type is PHYSICAL_*
   */
  public static boolean isPhysicalDataset(DatasetType t) {
    return t == DatasetType.PHYSICAL_DATASET ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
      t == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
      t == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  /**
   * Checks if datafile is from an iceberg dataset
   *
   * @param fileConfig file to check
   * @return true if file is from an iceberg dataset
   */
  public static boolean isIcebergFile(FileConfig fileConfig) {
    if (fileConfig == null) {
      return false;
    }

    return fileConfig.getType() == FileType.ICEBERG;
  }

  /**
   * Checks if datafile is from an DeltaLake dataset
   *
   * @param fileConfig file to check
   * @return true if file is from an DeltaLake dataset
   */
  public static boolean isDeltaLake(FileConfig fileConfig) {
    if (fileConfig == null) {
      return false;
    }

    return fileConfig.getType() == FileType.DELTA;
  }

  /**
   * Checks if dataset is iceberg dataset
   *
   * @param dataset Dataset to check
   * @return true if dataset is an iceberg dataset
   */
  public static boolean isIcebergDataset(DatasetConfig dataset) {
    if (dataset.getPhysicalDataset() == null) {
      return false;
    }

    IcebergMetadata icebergMetadata = dataset.getPhysicalDataset().getIcebergMetadata();
    if (icebergMetadata != null && icebergMetadata.getFileType() != null &&
            icebergMetadata.getFileType() == FileType.ICEBERG) {
      return true;
    }

    return DatasetHelper.isIcebergFile(dataset.getPhysicalDataset().getFormatSettings());
  }

  public static boolean isNativeIcebergTable(DatasetConfig dataset) {
    if (dataset.getPhysicalDataset() == null) {
      return false;
    }
    return DatasetHelper.isIcebergFile(dataset.getPhysicalDataset().getFormatSettings());
  }

  public static boolean isJsonDataset(DatasetConfig dataset) {
    if (dataset.getPhysicalDataset() == null) {
      return false;
    }

    if (dataset.getPhysicalDataset().getFormatSettings().getType() == FileType.JSON) {
      return true;
    }

    return false;
  }

  public static boolean isInternalIcebergTable(DatasetConfig dataset) {
    if (dataset.getPhysicalDataset() == null) {
      return false;
    }

    return (dataset.getPhysicalDataset().getIcebergMetadataEnabled() != null
      && dataset.getPhysicalDataset().getIcebergMetadataEnabled());
  }

  public static boolean isInternalIcebergTableOrJsonTable(DatasetConfig dataset) {
    return isInternalIcebergTable(dataset) || isJsonDataset(dataset);
  }

  /**
   * Checks if dataset is delta lake dataset
   *
   * @param dataset Dataset to check
   * @return true if dataset is an delta lake dataset
   */
  public static boolean isDeltaLakeDataset(DatasetConfig dataset) {
    if (dataset.getPhysicalDataset() == null) {
      return false;
    }

    return DatasetHelper.isDeltaLake(dataset.getPhysicalDataset().getFormatSettings());
  }

  /**
   * Checks if dataset is a converted iceberg dataset
   *
   * @param dataset Dataset to check
   * @return true if dataset is a converted iceberg dataset
   */
  public static boolean isConvertedIcebergDataset(DatasetConfig dataset) {
    if (dataset == null || dataset.getPhysicalDataset() == null) {
      return false;
    }

    return Boolean.TRUE.equals(dataset.getPhysicalDataset().getIcebergMetadataEnabled());
  }

  /**
   * Checks if dataset supports prune filter
   *
   * @param dataset Dataset to check
   * @return true if dataset supports prune filter push down
   */
  public static boolean supportsPruneFilter(DatasetConfig dataset) {
    return isIcebergDataset(dataset) || isDeltaLakeDataset(dataset) || isConvertedIcebergDataset(dataset);
  }

  /**
   * Checks if the data files are of type parquet.
   *
   * @param fileConfig config
   * @return true if data files of type parquet.
   */
  public static boolean hasParquetDataFiles(FileConfig fileConfig) {
    Preconditions.checkNotNull(fileConfig);
    return fileConfig.getType() == FileType.PARQUET;
  }

  public static boolean hasIcebergParquetDataFiles(FileConfig fileConfig) {
    Preconditions.checkNotNull(fileConfig);
    return fileConfig.getType() == FileType.ICEBERG;
  }

  public static boolean hasDeltaLakeParquetDataFiles(FileConfig fileConfig) {
    Preconditions.checkNotNull(fileConfig);
    return fileConfig.getType() == FileType.DELTA;
  }

  public static boolean hasParquetAsDataFiles(FileConfig fileConfig) {
    return fileConfig != null &&
      (hasParquetDataFiles(fileConfig)
        || hasIcebergParquetDataFiles(fileConfig)
        || hasDeltaLakeParquetDataFiles(fileConfig));
  }
}
