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

package com.dremio.exec.store.deltalake;

import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.file.proto.FileConfig;

public class HiveDeltaScanTableMetadata extends TableMetadataImpl {

  private final TableMetadata tableMetadata;

  public HiveDeltaScanTableMetadata(TableMetadata tableMetadata) {
    super(tableMetadata.getStoragePluginId(), tableMetadata.getDatasetConfig(),
      tableMetadata.getUser(), (SplitsPointer) tableMetadata.getSplitsKey(), tableMetadata.getPrimaryKey());
    this.tableMetadata = tableMetadata;
  }

  @Override
  public FileConfig getFormatSettings() {
    return tableMetadata.getFormatSettings();
  }

  @Override
  public BatchSchema getSchema() {
    return tableMetadata.getSchema();
  }
}
