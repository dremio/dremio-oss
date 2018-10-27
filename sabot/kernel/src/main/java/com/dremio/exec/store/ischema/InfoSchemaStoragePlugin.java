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
package com.dremio.exec.store.ischema;

import java.util.List;

import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

public class InfoSchemaStoragePlugin implements StoragePlugin {
  public static String NAME = "INFORMATION_SCHEMA";

  static final ImmutableSet<String> TABLES = FluentIterable.of(InfoSchemaTable.values()).transform(new Function<InfoSchemaTable, String>(){
    @Override
    public String apply(InfoSchemaTable input) {
      return input.name().toLowerCase();
    }}).toSet();

  static final ImmutableMap<String, InfoSchemaTable> TABLE_MAP = FluentIterable.<InfoSchemaTable>of(InfoSchemaTable.values()).uniqueIndex(new Function<InfoSchemaTable, String>(){
    @Override
    public String apply(InfoSchemaTable input) {
      return input.name().toLowerCase();
    }});

  private final SabotContext context;

  public InfoSchemaStoragePlugin(SabotContext context, String name) {
    Preconditions.checkArgument(NAME.equals(name));
    this.context = context;
  }

  SabotContext getSabotContext() {
    return context;
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, DatasetRetrievalOptions ignored) throws Exception {
    return FluentIterable.of(InfoSchemaTable.values()).transform(new Function<InfoSchemaTable, SourceTableDefinition>(){
      @Override
      public SourceTableDefinition apply(InfoSchemaTable input) {
        return input.asTableDefinition(null);
      }});
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, DatasetRetrievalOptions ignored) throws Exception {
    if(datasetPath.size() != 2) {
      return null;
    }

    InfoSchemaTable table = TABLE_MAP.get(datasetPath.getName().toLowerCase());
    if(table != null) {
      return table.asTableDefinition(oldDataset);
    }

    return null;
  }

  @Override
  public boolean containerExists(NamespaceKey key) {
    return false;
  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    return key.size() == 2 && TABLES.contains(key.getName().toLowerCase());
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return InfoSchemaRulesFactory.class;
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig, DatasetRetrievalOptions ignored) throws Exception {
    return CheckResult.UNCHANGED;
  }

  @Override
  public void close(){
  }

  @Override
  public void start() {
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

}
