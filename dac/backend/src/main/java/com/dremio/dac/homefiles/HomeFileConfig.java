/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.Objects;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.dremio.config.DremioConfig;
import com.dremio.exec.store.dfs.FileSystemWrapper;

/**
 * Home files configuration.
 */
public final class HomeFileConfig {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HomeFileConfig.class);

  static final FsPermission DEFAULT_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.EXECUTE);

  public static final String HOME_PLUGIN_NAME = "__home";
  private static final String UPLOADS = "_uploads";
  private static final String STAGING = "_staging";

  private final URI location;
  private final Path stagingDir;
  private final Path uploadsDir;
  private final Configuration fsConf;
  // deprecating host param as it causes issues with loading files
  // from one host and StoragePlugin being initialized on another (executor)
  // as hosts do not match we will be trying to create new instances of HomeFileSystemPlugin
  // causing performance issues as well as running as non process user and failing
  @Deprecated
  private final String host;
  private final boolean isPdfsBased;

  @Inject
  public HomeFileConfig(DremioConfig config) throws IOException {
    this(config.getURI(DremioConfig.UPLOADS_PATH_STRING), config.getThisNode());
  }

  public HomeFileConfig(URI location, String hostName) throws IOException {
    this.location = location;
    this.host = hostName;
    this.isPdfsBased = location.getScheme().equals("pdfs");
    this.stagingDir = new Path(location.getPath(), STAGING + "." + hostName);
    this.uploadsDir = new Path(location.getPath(), UPLOADS);
    this.fsConf = new Configuration();

  }

  public String getHost() {
    return host;
  }

  public URI getLocation() {
    return location;
  }

  public Path getStagingDir() {
    return stagingDir;
  }

  public Path getUploadsDir() {
    return uploadsDir;
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  public boolean isPdfsBased() {
    return isPdfsBased;
  }

  public FileSystem createFileSystem() throws IOException {
    FileSystem fs = FileSystemWrapper.get(location, fsConf);
    fs.mkdirs(new Path(location.getPath()), DEFAULT_PERMISSIONS);
    fs.mkdirs(stagingDir, DEFAULT_PERMISSIONS);
    fs.mkdirs(uploadsDir, DEFAULT_PERMISSIONS);
    fs.deleteOnExit(stagingDir);
    return fs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(location);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    HomeFileConfig otherConfig = (HomeFileConfig) obj;

    return Objects.equals(this.getLocation(), otherConfig.getLocation());
  }
}
