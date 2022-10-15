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

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Snapshot;

import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PrimaryKey;

/**
 * Base class for operations on primary keys
 */
public class PrimaryKeyOperations extends MetadataOperations {
  public static String DREMIO_PRIMARY_KEY = "dremio.primary_key";

  public PrimaryKeyOperations(DatasetConfig datasetConfig,
                              SabotContext context,
                              NamespaceKey table,
                              SchemaConfig schemaConfig,
                              IcebergModel model,
                              Path path,
                              StoragePlugin storagePlugin) {
    super(datasetConfig, context, table, schemaConfig, model, path, storagePlugin);
  }

  protected void performOperation(List<Field> columns) {
    checkAndRepair();

    if (DatasetHelper.isIcebergDataset(datasetConfig)) {
      model.updatePrimaryKey(model.getTableIdentifier(path.toString()), columns);
    } else if (DatasetHelper.isInternalIcebergTable(datasetConfig)) {
      String metadataTableName = getMetadataTableName();

      FileSystemPlugin<?> metaStoragePlugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);

      IcebergModel icebergModel = metaStoragePlugin.getIcebergModel();

      IcebergTableIdentifier tableIdentifier = icebergModel.getTableIdentifier(
        metaStoragePlugin.resolveTablePathToValidPath(metadataTableName).toString());

      IcebergOpCommitter opCommitter = icebergModel.getPrimaryKeyUpdateCommitter(tableIdentifier, columns);
      Snapshot snapshot = opCommitter.commit();
      updateDatasetConfigWithIcebergMetadata(opCommitter.getRootPointer(), snapshot.snapshotId(),
        opCommitter.getCurrentSpecMap(), opCommitter.getCurrentSchema());
    }

    final List<String> primaryKey = columns.stream()
      .map(f -> f.getName().toLowerCase(Locale.ROOT))
      .collect(Collectors.toList());
    saveInKvStore(table, datasetConfig, schemaConfig.getUserName(), storagePlugin, context, primaryKey);
  }

  public static void saveInKvStore(NamespaceKey table,
                                   DatasetConfig datasetConfig,
                                   String userName,
                                   StoragePlugin storagePlugin,
                                   SabotContext context,
                                   List<String> primaryKey) {
    if (!(storagePlugin instanceof VersionedPlugin)) { // Don't store in the namespace for versioned plugins.
      datasetConfig.getPhysicalDataset().setPrimaryKey(new PrimaryKey().setColumnList(primaryKey));
      save(table, datasetConfig, userName, context);
    }
  }
}
