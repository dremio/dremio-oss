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
package com.dremio.exec.store;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.tablefunctions.MetadataFunctionsMacro;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * Metadata Functions TableMetadata interface. This is how a metadata functions table is exposed to the planning environment.
 * table_history,table_snapshot,table_manifests are metadata functions.
 * For query like : select * from table(table_history('call_center'))
 */
public class MFunctionCatalogMetadata {

  private final BatchSchema batchSchema;
  private final NamespaceKey namespaceKey;
  /**
   * Plugin of underlying table.
   * In case of table_history('iceberg_table'). This plugin should be same as of iceberg_table.
   */
  private StoragePluginId storagePluginId;
  /**
   * file type for from which metadata is being read. eg: JSON for iceberg history
   */
  private final FileType fileType;
  /**
   * native table type eg: iceberg, deltalake
   */
  private final FileType underlyingTable;
  /**
   * table function name such as
   * TABLE_HISTORY,TABLE_MANIFESTS, TABLE_SNAPSHOT,TABLE_FILES
   */
  private final MetadataFunctionsMacro.MacroName mFunctionName;

  public MFunctionCatalogMetadata(BatchSchema batchSchema, NamespaceKey namespaceKey, StoragePluginId storagePluginId, MetadataFunctionsMacro.MacroName mFunctionName) {
    this.batchSchema = batchSchema;
    this.namespaceKey = namespaceKey;
    this.storagePluginId = storagePluginId;
    this.underlyingTable = FileType.ICEBERG;
    this.mFunctionName = mFunctionName;
    this.fileType = getMetadataFileType();
  }

  private FileType getMetadataFileType() {
    switch (mFunctionName) {
      case TABLE_HISTORY:
      case TABLE_SNAPSHOT:
        return FileType.JSON;
      case TABLE_FILES:
      case TABLE_MANIFESTS:
        return FileType.AVRO;
      default:
        throw new UnsupportedOperationException(mFunctionName + " is not supported for table: " + namespaceKey.getPathComponents());
    }
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  public NamespaceKey getNamespaceKey() {
    return namespaceKey;
  }

  public StoragePluginId getStoragePluginId() {
    return storagePluginId;
  }

  public FileType getFileType() {
    return fileType;
  }

  public FileType getUnderlyingTable() {
    return underlyingTable;
  }

  public MetadataFunctionsMacro.MacroName getMetadataFunctionName() {
    return mFunctionName;
  }
}
