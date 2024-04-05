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
package com.dremio.exec.store.dfs.system;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableMetadata;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableMetadata;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.NotFoundException;

/** Utility class for initializing the table metadata for system iceberg tables. */
public final class SystemIcebergTableMetadataFactory {

  public static final String COPY_JOB_HISTORY_TABLE_NAME = "copy_job_history";
  public static final String COPY_FILE_HISTORY_TABLE_NAME = "copy_file_history";

  public static final List<String> SUPPORTED_TABLES =
      ImmutableList.of(COPY_JOB_HISTORY_TABLE_NAME, COPY_FILE_HISTORY_TABLE_NAME);

  public static SystemIcebergTableMetadata getTableMetadata(
      String pluginName,
      String pluginPath,
      OptionManager optionManager,
      List<String> tableSchemaPath) {
    long schemaVersion =
        optionManager.getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION);
    if (tableSchemaPath.stream().anyMatch(COPY_JOB_HISTORY_TABLE_NAME::equalsIgnoreCase)) {
      Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
      return new CopyJobHistoryTableMetadata(
          schemaVersion, schema, pluginName, pluginPath, COPY_JOB_HISTORY_TABLE_NAME);
    } else if (tableSchemaPath.stream().anyMatch(COPY_FILE_HISTORY_TABLE_NAME::equalsIgnoreCase)) {
      Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
      return new CopyFileHistoryTableMetadata(
          schemaVersion, schema, pluginName, pluginPath, COPY_FILE_HISTORY_TABLE_NAME);
    }
    throw new NotFoundException("Invalid system iceberg table : %s", tableSchemaPath);
  }

  private SystemIcebergTableMetadataFactory() {
    // Not to be instantiated
  }

  public static boolean isSupportedTablePath(List<String> tableSchemaPath) {
    return SUPPORTED_TABLES.stream()
        .anyMatch(
            supportedTableName -> tableSchemaPath.stream().anyMatch(supportedTableName::equals));
  }
}
