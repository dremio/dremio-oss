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
package com.dremio.dac.service;

import java.util.List;

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Source for test purposes.
 */
@SourceType(value = "MYPRIVATE", configurable = false)
public class APrivateSource extends FileSystemConf<APrivateSource, FileSystemPlugin<APrivateSource>> {

  @Tag(1)
  @Secret
  public String password;

  @Tag(2)
  public String username;

  @Override
  public Path getPath() {
    return Path.of("/");
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
    return "file:///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public FileSystemPlugin<APrivateSource> newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new FileSystemPlugin<>(this, context, name, pluginIdProvider);
  }


}
