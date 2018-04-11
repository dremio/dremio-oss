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
package com.dremio.exec.store.sys;

import java.util.List;

import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

public class SystemStoragePlugin implements StoragePlugin {

  static final ImmutableSet<String> TABLES = FluentIterable.of(SystemTable.values()).transform(new Function<SystemTable, String>(){
    @Override
    public String apply(SystemTable input) {
      return input.getTableName().toLowerCase();
    }}).toSet();

  public static final ImmutableMap<String, SystemTable> TABLE_MAP = FluentIterable.of(SystemTable.values()).uniqueIndex(new Function<SystemTable, String>(){

    @Override
    public String apply(SystemTable input) {
      return input.getTableName().toLowerCase();
    }});

  private final SabotContext context;
  private final Predicate<String> userPredicate;

  public SystemStoragePlugin(SabotContext context, String name) {
    this(context, name, Predicates.<String>alwaysTrue());
  }

  protected SystemStoragePlugin(SabotContext context, String name, Predicate<String> userPredicate) {
    Preconditions.checkArgument("sys".equals(name));
    this.context = context;
    this.userPredicate = userPredicate;
  }

  SabotContext getSabotContext() {
    return context;
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    return FluentIterable.of(SystemTable.values()).transform(new Function<SystemTable, SourceTableDefinition>(){
      @Override
      public SourceTableDefinition apply(SystemTable input) {
        return input.asTableDefinition(null);
      }});
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception {
    if(datasetPath.size() != 2) {
      return null;
    }

    SystemTable table = TABLE_MAP.get(datasetPath.getName().toLowerCase());
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
    return userPredicate.apply(user);
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
    return SystemTableRulesFactory.class;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) throws Exception {
    return CheckResult.UNCHANGED;
  }

  @Override
  public void start() {
  }

  @Override
  public void close() {
  }

}
