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
package com.dremio.exec.store.mfunctions;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * table_files requires its own TableMetadata , since its using different relnodes like ScanCrel. A separate TableMetadata helps to identify correct Prel Nodes
 * TableFilesFunctionScanPrule needs an identifier.
 */
public class TableFilesFunctionTableMetadata extends TableMetadataImpl {

  private final BatchSchema schema;

  public TableFilesFunctionTableMetadata(StoragePluginId pluginId, DatasetConfig config, String user, BatchSchema schema,
                                         SplitsPointer splitsPointer) {
    super(pluginId, config, user, splitsPointer);
    this.schema = schema;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

}
