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
package com.dremio.exec.store.metadatarefresh;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public class RefreshExecTableMetadata extends TableMetadataImpl {
    // This is the actual table schema, this maybe null while initializing
    // if the plugin doesn't support fetching schema in the coordinator
    private final BatchSchema tableSchema;

    public RefreshExecTableMetadata(StoragePluginId plugin, DatasetConfig config, String user, SplitsPointer splits, BatchSchema tableSchema) {
        super(plugin, config, user, splits);
        this.tableSchema = tableSchema;
    }

    @Override
    public BatchSchema getSchema() {
        return DirList.OUTPUT_SCHEMA.BATCH_SCHEMA;
        // Schema for the leaf scan operators
    }

    public BatchSchema getTableSchema() {
      return tableSchema;
    }
}
