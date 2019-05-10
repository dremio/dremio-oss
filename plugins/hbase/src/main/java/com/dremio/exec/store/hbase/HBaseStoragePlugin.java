/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class HBaseStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseStoragePlugin.class);

  static final String HBASE_SYSTEM_NAMESPACE = "hbase";

  private final String name;
  private final SabotContext context;
  private final HBaseConf storeConfig;
  private final HBaseConnectionManager connection;

  public HBaseStoragePlugin(HBaseConf storeConfig, SabotContext context, String name) {
    this.context = context;
    this.storeConfig = storeConfig;
    this.name = name;
    this.connection = new HBaseConnectionManager(storeConfig.getHBaseConf());
  }

  public HBaseConf getConfig() {
    return storeConfig;
  }

  public Connection getConnection() {
    return connection.getConnection();
  }

  @VisibleForTesting
  public void closeCurrentConnection() throws IOException {
    connection.close();
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) throws ConnectorException {
    final Admin admin;
    final TableName[] tableNames;
    try {
      admin = connection.getConnection().getAdmin();
      tableNames = admin.listTableNames();
    } catch (IOException e) {
      throw new ConnectorException(e);
    }

    return new DatasetHandleListing() {
      @Override
      public Iterator<? extends DatasetHandle> iterator() {
        return Arrays.stream(tableNames).map(tableName -> {
          final EntityPath entityPath = new EntityPath(ImmutableList.of(name, tableName.getNamespaceAsString(),
              tableName.getQualifierAsString()));
          return new HBaseTableBuilder(entityPath, connection, storeConfig.isSizeCalcEnabled, context);
        }).iterator();
      }

      @Override
      public void close() {
        try {
          admin.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
    if (!(datasetPath.size() == 3 || datasetPath.size() == 2)) {
      return Optional.empty();
    }
    if (datasetPath.getName().indexOf((TableName.NAMESPACE_DELIM)) != -1) {
      // ensure the table name does not have ":"
      return Optional.empty();
    }

    try (Admin admin = connection.getConnection().getAdmin()) {
      if (admin.getTableDescriptor(TableNameGetter.getTableName(datasetPath)) == null) {
        return Optional.empty();
      }
    } catch (IOException e) {
      logger.warn("Failure while checking for HBase table {}.", datasetPath, e);
      return Optional.empty();
    }

    return Optional.of(new HBaseTableBuilder(datasetPath, connection, storeConfig.isSizeCalcEnabled, context));
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) throws ConnectorException {
    final BatchSchema oldSchema = CurrentSchemaOption.getSchema(options);
    return datasetHandle.unwrap(HBaseTableBuilder.class).getDatasetMetadata(oldSchema);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options)
      throws ConnectorException {
    final BatchSchema oldSchema = CurrentSchemaOption.getSchema(options);
    return datasetHandle.unwrap(HBaseTableBuilder.class).listPartitionChunks(oldSchema);
  }

  @Override
  public boolean containerExists(EntityPath key) {
    if(key.size() != 2) {
      return false;
    }
    if (key.getName().equals(HBASE_SYSTEM_NAMESPACE)) {
      // DX-10110: do not allow access to the system namespace of HBase itself. Causes confusion when multiple 'use hbase' statements are processed
      return false;
    }
    try(Admin admin = connection.getConnection().getAdmin()) {
      NamespaceDescriptor descriptor = admin.getNamespaceDescriptor(key.getName());
      return descriptor != null;
    } catch (IOException e) {
      logger.warn("Failure while checking for HBase Namespace {}.", key, e);
    }
    return false;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    try {
      connection.validate();
    } catch(Exception ex) {
      return SourceState.badState(ex);
    }
    return SourceState.GOOD;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return HBaseRulesFactory.class;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public void start() {
    connection.validate();
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

}
