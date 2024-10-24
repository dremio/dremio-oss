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

import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinition;
import com.dremio.exec.store.dfs.copyinto.systemtable.schema.SchemaDefinitionFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory.SupportedSystemIcebergView;
import org.apache.iceberg.Schema;

/** Provides a schema for the "copy_errors_history" view based on the specified schema version. */
public final class CopyErrorsHistoryViewSchemaProvider {

  private final SchemaDefinition schemaDefinition;

  public CopyErrorsHistoryViewSchemaProvider(long schemaVersion) {
    schemaDefinition =
        SchemaDefinitionFactory.getSchemaDefinition(
            schemaVersion, SupportedSystemIcebergView.COPY_ERRORS_HISTORY);
  }

  /**
   * Get the schema for the "copy_errors_history" view.
   *
   * @return The schema for the "copy_errors_history" view.
   */
  public Schema getSchema() {
    return schemaDefinition.getTableSchema();
  }
}
