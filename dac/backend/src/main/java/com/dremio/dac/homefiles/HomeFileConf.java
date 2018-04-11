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
package com.dremio.dac.homefiles;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotBlank;

import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Source type used for Home Files purposes.
 */
@SourceType(value = "HOME", configurable = false)
public class HomeFileConf extends FileSystemConf<HomeFileConf, HomeFileSystemStoragePlugin>{

  private static final String UPLOADS = "_uploads";
  private static final String STAGING = "_staging";

  // this is transient since it is actually different for each node (which means it is property based as opposed to defined directly in this configuration).
  private transient String hostname;

  private final transient Supplier<URI> uri = new Supplier<URI>() {
    @Override
    public URI get() {
      try {
        return new URI(location);
      } catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }};

  @NotBlank
  @Tag(1)
  public String location;

  public HomeFileConf() {

  }

  public HomeFileConf(DremioConfig config) {
    this(config.getString(DremioConfig.UPLOADS_PATH_STRING));
    hostname = config.getThisNode();
  }

  public HomeFileConf(String location) {
    this.location = location;
  }

  public HomeFileConf(String location, String hostname) {
    this.location = location;
    this.hostname = hostname;
  }

  @Override
  public Path getPath() {
    return new Path(uri.get().getPath());
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  public boolean isPdfsBased() {
    return uri.get().getScheme().equals("pdfs");
  }

  @Override
  public List<Property> getProperties() {
    return ImmutableList.of();
  }

  @Override
  public String getConnection() {
    URI path = uri.get();
    if(path.getAuthority() != null) {
      return path.getScheme() + "://" + path.getAuthority() + "/";
    } else {
      return path.getScheme() + ":///";
    }

  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_VIEW;
  }

  @Override
  public HomeFileSystemStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new HomeFileSystemStoragePlugin(this, context, name, null, pluginIdProvider);
  }

  public Path getBaseUploadsPath() {
    return getPath();
  }

  public Path getInnerUploads() {
    return new Path(getPath(), UPLOADS);
  }

  public Path getStagingPath(String hostname) {
    return new Path(getPath(), STAGING + "." + hostname);
  }

  public FileSystemWrapper getFilesystemAndCreatePaths(String hostname) throws IOException {
    FileSystemWrapper fs = FileSystemWrapper.get(uri.get(), new Configuration());
    fs.mkdirs(getPath(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
    fs.mkdirs(getInnerUploads(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);

    if(hostname == null) {
      hostname = this.hostname;
    }

    if(hostname != null) {
      fs.mkdirs(getStagingPath(hostname), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
      fs.deleteOnExit(getStagingPath(hostname));
    }
    return fs;
  }

  public boolean isInternal() {
    return true;
  }
}
