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

import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadata;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.iceberg.Schema;

/**
 * {@code CopyErrorsHistoryViewMetadata} represents the metadata for a view of the
 * "copy_errors_history" table in the system schema. It extends {@link SystemIcebergViewMetadata}
 * and includes information about the view's schema, schema version, and table name.
 */
public class CopyErrorsHistoryViewMetadata extends SystemIcebergViewMetadata {
  public CopyErrorsHistoryViewMetadata(Schema schema, long schemaVersion, String tableName) {
    super(schema, schemaVersion, tableName);
  }

  @Override
  public List<String> getViewTables() {
    return ImmutableList.of(
        SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME,
        SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME);
  }
}
