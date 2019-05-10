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
package com.dremio.service.reflection.materialization;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.net.URI;
import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Configuration for the MaterializationStoragePluginConfigOTAS plugin
 */
@SourceType( value = "ACCELERATION", configurable = false)
public class AccelerationStoragePluginConfig extends FileSystemConf<AccelerationStoragePluginConfig, AccelerationStoragePlugin> {

  @Tag(1)
  public String connection;

  @Tag(2)
  public String path = "/";

  @Tag(3)
  public boolean enableAsync = true;

  public AccelerationStoragePluginConfig() {
  }

  public AccelerationStoragePluginConfig(URI path, boolean enableAsync) {
    if(path.getAuthority() != null) {
      connection = path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      connection = path.getScheme() + ":///";
    }
    String storagePath = path.getPath();
    if (!isNullOrEmpty(storagePath)) {
      this.path = storagePath;
    }
    this.enableAsync = enableAsync;
  }

  @Override
  public AccelerationStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new AccelerationStoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public boolean isInternal() {
    return true;
  }

  @Override
  public Path getPath() {
    return new Path(path);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public List<Property> getProperties() {
    return ImmutableList.of();
  }

  @Override
  public String getConnection() {
    return connection;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.SYSTEM_TABLE;
  }

  @Override
  public List<String> getConnectionUniqueProperties() {
    return ImmutableList.of();
  }

  public static SourceConfig create(URI path, boolean enableAsync) {
    SourceConfig conf = new SourceConfig();
    AccelerationStoragePluginConfig connection = new AccelerationStoragePluginConfig(path, enableAsync);
    conf.setConnectionConf(connection);
    conf.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf.setName(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);
    return conf;
  }

  @Override
  public boolean createIfMissing() {
    return true;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }
}
