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
package com.dremio.exec.store.dfs;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * To add primary keys to the iceberg metadata
 */
public class AddPrimaryKey extends PrimaryKeyOperations {

  public AddPrimaryKey(NamespaceKey key,
                       SabotContext context,
                       DatasetConfig datasetConfig,
                       SchemaConfig schemaConfig,
                       IcebergModel model,
                       Path path,
                       StoragePlugin storagePlugin) {
    super(datasetConfig, context, key, schemaConfig, model, path, storagePlugin);
  }

  public void performOperation(List<Field> columns) {
    super.performOperation(columns);
  }
}
