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
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;

public class HBaseStoragePlugin implements StoragePlugin, Service {

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

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    try (Admin admin = connection.getConnection().getAdmin()) {
      return FluentIterable.of(admin.listTableNames()).transform(new Function<TableName, SourceTableDefinition>(){
        @Override
        public SourceTableDefinition apply(TableName input) {
          final NamespaceKey key = new NamespaceKey(ImmutableList.<String>of(name, input.getNamespaceAsString(), input.getQualifierAsString()));
          return new HBaseTableBuilder(key, null, connection, storeConfig.isSizeCalcEnabled, context);
        }});
    }
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
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception {
    if(!datasetExists(datasetPath)) {
      return null;
    }
    return new HBaseTableBuilder(datasetPath, oldDataset, connection, storeConfig.isSizeCalcEnabled, context);
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
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
  public boolean datasetExists(NamespaceKey key) {
    if (!(key.size() == 3 || key.size() == 2)) {
      return false;
    }
    if (key.getLeaf().indexOf((TableName.NAMESPACE_DELIM)) != -1) {
      // ensure the table name does not have ":"
      return false;
    }

    try(Admin admin = connection.getConnection().getAdmin()) {
      HTableDescriptor descriptor = admin.getTableDescriptor(TableNameGetter.getTableName(key));
      return descriptor != null;
    } catch (IOException e) {
      logger.warn("Failure while checking for HBase table {}.", key, e);
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
  public CheckResult checkReadSignature(ByteString key, final DatasetConfig datasetConfig) throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
    if(!datasetExists(namespaceKey)) {
      return CheckResult.DELETED;
    }

    return new CheckResult(){
      @Override
      public UpdateStatus getStatus() {
        return UpdateStatus.CHANGED;
      }

      @Override
      public SourceTableDefinition getDataset() {
        return new HBaseTableBuilder(namespaceKey, datasetConfig, connection, storeConfig.isSizeCalcEnabled, context);
      }};
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
