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
package com.dremio.dac.homefiles;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.NotBlank;

import org.apache.hadoop.conf.Configuration;

import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
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

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(HomeFileConf.class);

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

  @Tag(2)
  public boolean enableAsync = true;

  public HomeFileConf() {

  }

  public HomeFileConf(DremioConfig config) {
    this(config.getString(DremioConfig.UPLOADS_PATH_STRING));
    final boolean enableAsyncForUploads = !config.hasPath(DremioConfig.DEBUG_UPLOADS_ASYNC_ENABLED)
      || config.getBoolean(DremioConfig.DEBUG_UPLOADS_ASYNC_ENABLED);
    hostname = config.getThisNode();
    enableAsync = enableAsyncForUploads;
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
    return Path.of(uri.get().getPath());
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
    return new HomeFileSystemStoragePlugin(this, context, name, pluginIdProvider);
  }

  public Path getBaseUploadsPath() {
    return getPath();
  }

  public Path getInnerUploads() {
    return getPath().resolve(UPLOADS);
  }

  public Path getStagingPath(String hostname) {
    return getPath().resolve(STAGING + "." + hostname);
  }

  public FileSystem getFilesystemAndCreatePaths(String hostname) throws IOException {
    FileSystem fs = HadoopFileSystem.get(uri.get(), new Configuration(), enableAsync);
    fs.mkdirs(getPath(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
    fs.mkdirs(getInnerUploads(), HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);

    if(hostname == null) {
      hostname = this.hostname;
    }

    if(hostname != null) {
      final Path stagingDir = getStagingPath(hostname);
      fs.mkdirs(stagingDir, HomeFileSystemStoragePlugin.DEFAULT_PERMISSIONS);
      FileSystemUtils.deleteOnExit(fs, stagingDir);
    }
    return fs;
  }

  @Override
  public boolean isInternal() {
    return true;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }
}
