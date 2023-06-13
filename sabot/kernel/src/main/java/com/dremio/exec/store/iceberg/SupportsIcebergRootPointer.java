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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Interface for plugins that support reading of iceberg root pointers.
 */
public interface SupportsIcebergRootPointer {

  /**
   *
   * @return A copy of the configuration to use for the plugin.
   */
  Configuration getFsConfCopy();

  /**
   * Checks if a metadata validity check is required.
   */
  default boolean isMetadataValidityCheckRecentEnough(Long lastMetadataValidityCheckTime, Long currentTime, OptionManager optionManager) {
    Configuration conf = getFsConfCopy();
    final long metadataAggressiveExpiryTime = Long.parseLong(conf.get(PlannerSettings.METADATA_EXPIRY_CHECK_INTERVAL_SECS.getOptionName(),
      String.valueOf(optionManager.getOption(PlannerSettings.METADATA_EXPIRY_CHECK_INTERVAL_SECS)))) * 1000;
    return (lastMetadataValidityCheckTime != null && (lastMetadataValidityCheckTime + metadataAggressiveExpiryTime >= currentTime)); // dataset metadata validity was checked too long ago (or never)
  }

  /**
   *
   * @param formatConfig
   * @return Returns format plugin based on the the formatConfig.
   */
  FormatPlugin getFormatPlugin(FormatPluginConfig formatConfig);

  /**
   *
   * @param filePath The filepath for the file system.
   * @param userName The userName for the file system.
   * @param operatorContext The operatorContext for creating the file system.
   * @return The filesystem to use to access the iceberg tables.
   * @throws IOException
   */
  FileSystem createFS(String filePath, String userName, OperatorContext operatorContext) throws IOException;

  /**
   *
   * @param filePath The filepath for the file system.
   * @param userName The userName for the file system.
   * @param operatorContext The operatorContext for creating the file system.
   * @return The filesystem to use to access the iceberg tables.
   * @throws IOException
   */
  FileSystem createFSWithAsyncOptions(String filePath, String userName, OperatorContext operatorContext) throws IOException;

  /**
   *
   * @param filePath The filepath for the file system.
   * @param userName The userName for the file system.
   * @param operatorContext The operatorContext for creating the file system.
   * @return The filesystem without using hadoop fs cache.
   * @throws IOException
   */
  FileSystem createFSWithoutHDFSCache(String filePath, String userName, OperatorContext operatorContext) throws IOException;

  default boolean supportsColocatedReads() { return  true;}

  /**
   * Checks if the Iceberg Metadata present in the kvstore for the given dataset
   * is valid or not.
   */
  boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey key, NamespaceService userNamespaceService);

  /**
   * Based on the source plugin, creates an instance of corresponding Iceberg Catalog table operations.
   */
  TableOperations createIcebergTableOperations(FileSystem fs, String queryUserName, IcebergTableIdentifier tableIdentifier);

  FileIO createIcebergFileIO(FileSystem fs, OperatorContext context, List<String> dataset, String datasourcePluginUID,
      Long fileLength);
}
