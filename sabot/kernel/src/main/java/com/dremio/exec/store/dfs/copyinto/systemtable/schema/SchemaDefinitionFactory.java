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
package com.dremio.exec.store.dfs.copyinto.systemtable.schema;

import com.dremio.exec.store.dfs.copyinto.systemtable.schema.errorshistory.V1ErrorsHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.errorshistory.V2ErrorsHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.errorshistory.V3ErrorsHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V1FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V2FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.filehistory.V3FileHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V1JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V2JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.jobhistory.V3JobHistorySchemaDefinition;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory.SupportedSystemIcebergView;
import java.util.Objects;

public class SchemaDefinitionFactory {
  public static SchemaDefinition getSchemaDefinition(
      long schemaVersion, SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable tableType) {
    switch (tableType) {
      case COPY_JOB_HISTORY:
        if (schemaVersion == 1) {
          return new V1JobHistorySchemaDefinition(schemaVersion);
        } else if (schemaVersion == 2) {
          return new V2JobHistorySchemaDefinition(schemaVersion);
        } else if (schemaVersion == 3) {
          return new V3JobHistorySchemaDefinition(schemaVersion);
        }
        throw newUnsupportedSchemaVersionException(schemaVersion, tableType.getTableName());
      case COPY_FILE_HISTORY:
        if (schemaVersion == 1) {
          return new V1FileHistorySchemaDefinition(schemaVersion);
        } else if (schemaVersion == 2) {
          return new V2FileHistorySchemaDefinition(schemaVersion);
        } else if (schemaVersion == 3) {
          return new V3FileHistorySchemaDefinition(schemaVersion);
        }
        throw newUnsupportedSchemaVersionException(schemaVersion, tableType.getTableName());
      default:
        throw newUnsupportedTableName(tableType.getTableName());
    }
  }

  public static SchemaDefinition getSchemaDefinition(
      long schemaVersion, SystemIcebergViewMetadataFactory.SupportedSystemIcebergView viewType) {
    if (Objects.requireNonNull(viewType) == SupportedSystemIcebergView.COPY_ERRORS_HISTORY) {
      if (schemaVersion == 1) {
        return new V1ErrorsHistorySchemaDefinition(schemaVersion);
      } else if (schemaVersion == 2) {
        return new V2ErrorsHistorySchemaDefinition(schemaVersion);
      } else if (schemaVersion == 3) {
        return new V3ErrorsHistorySchemaDefinition(schemaVersion);
      }
      throw newUnsupportedSchemaVersionException(schemaVersion, viewType.getViewName());
    }
    throw newUnsupportedTableName(viewType.getViewName());
  }

  private static UnsupportedOperationException newUnsupportedTableName(String tableName) {
    return new UnsupportedOperationException("Unknown table name " + tableName);
  }

  private static UnsupportedOperationException newUnsupportedSchemaVersionException(
      long schemaVersion, String tableName) {
    return new UnsupportedOperationException(
        "Unsupported "
            + tableName
            + " schema version: "
            + schemaVersion
            + ". Currently supported schema versions are: [1, 2, 3]");
  }
}
