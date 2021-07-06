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

import com.dremio.exec.store.StoragePlugin;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * This is an optional interface. When extended by an implementation of {@link StoragePlugin}, this is used to provide
 * information from such storage plugins that supports reading/writing from internally created Iceberg Tables by Dremio.
 */
public interface SupportsInternalIcebergTable {

  /**
   * Creates a new FileSystem instance for a given user using the file path provided.
   *
   * @param filePath file path
   * @param userName user
   * @return file system, not null
   */
  FileSystem createFS(String filePath, String userName, OperatorContext operatorContext) throws IOException;

  /**
   * Indicates that the plugin supports getting dataset metadata (partition spec, partition values, table schema)
   * in the coordinator itself. Eg. for Hive plugin, we can get this info from Hive metastore.
   */
  default boolean canGetDatasetMetadataInCoordinator() {
    return false;
  }

  /**
   * Resolves the table name to a valid path.
   */
  List<String> resolveTableNameToValidPath(List<String> tableSchemaPath);
}
