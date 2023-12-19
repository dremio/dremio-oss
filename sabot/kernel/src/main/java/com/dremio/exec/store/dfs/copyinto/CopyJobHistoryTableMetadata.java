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
package com.dremio.exec.store.dfs.copyinto;

import org.apache.iceberg.Schema;

import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;

/**
 * This class represents the metadata for the "copy_job_history" table stored by {@link SystemIcebergTablesStoragePlugin}
 */
public class CopyJobHistoryTableMetadata extends SystemIcebergTableMetadata {
  /**
   * Constructs an instance of CopyJobHistoryTableMetadata.
   *
   * @param schemaVersion The version of the schema for the table.
   * @param schema        The iceberg schema of the table.
   * @param pluginName    Name of the {@link SystemIcebergTablesStoragePlugin}
   * @param pluginPath    Path of the {@link SystemIcebergTablesStoragePlugin}
   * @param tableName     Name of the iceberg table.
   */
  public CopyJobHistoryTableMetadata(long schemaVersion, Schema schema, String pluginName, String pluginPath, String tableName) {
    super(schemaVersion, schema, pluginName, pluginPath, tableName);
  }
}
