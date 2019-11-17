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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.protostuff.Tag;

/**
 * Source type used for internal purposes.
 */
@SourceType(value = "INTERNAL", configurable = false)
public class InternalFileConf extends FileSystemConf<InternalFileConf, FileSystemPlugin<InternalFileConf>>{

  @Tag(1)
  public String connection;

  @Tag(2)
  public String path = "/";

  @Tag(3)
  public boolean enableImpersonation = false;

  @Tag(4)
  public List<Property> propertyList = Lists.newArrayList();

  @Tag(5)
  public SchemaMutability mutability = SchemaMutability.NONE;

  @Tag(6)
  public boolean isInternal = true;

  @Tag(7)
  public boolean enableAsync = true;

  @Override
  public Path getPath() {
    return Path.of(path);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return enableImpersonation;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    return connection;
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return mutability;
  }

  @Override
  public FileSystemPlugin<InternalFileConf> newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new FileSystemPlugin<>(this, context, name, pluginIdProvider);
  }

  public InternalFileConf() {
  }

  InternalFileConf(String connection, String path, boolean enableImpersonation, List<Property> propertyList,
                          SchemaMutability mutability, boolean enableAsync) {
    this.connection = connection;
    this.path = path;
    this.enableImpersonation = enableImpersonation;
    this.propertyList = propertyList;
    this.mutability = mutability;
    this.enableAsync = enableAsync;
  }

  public static SourceConfig create(
      String name,
      URI path,
      SchemaMutability mutability,
      MetadataPolicy policy,
      boolean enableAsync
      ) {
    SourceConfig conf = new SourceConfig();
    final String connection;
    if(path.getAuthority() != null) {
      connection = path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      connection = path.getScheme() + ":///";
    }

    InternalFileConf fc = new InternalFileConf(connection, path.getPath(), false, null, mutability, enableAsync);
    conf.setConnectionConf(fc);
    conf.setMetadataPolicy(policy);
    conf.setName(name);
    return conf;
  }

  @Override
  public boolean createIfMissing() {
    return !connection.startsWith("classpath:");
  }

  @Override
  public boolean isInternal() {
    return isInternal;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }

  private final transient Supplier<URI> uri = new Supplier<URI>() {
    @Override
    public URI get() {
      try {
        return new URI(connection);
      } catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }};

  public boolean isPdfsBased() {
    return uri.get().getScheme().equals("pdfs");
  }
}
