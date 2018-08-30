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
package com.dremio.exec.store.dfs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;

/**
 * {@link StoragePluginConfig} for {@link FileSystemPlugin}
 */
@JsonTypeName(FileSystemConfig.NAME)
public class FileSystemConfig extends StoragePluginConfig {
  public static final String NAME = "file_default";

  private final String connection;
  private final String path;
  private final Map<String, String> config;
  private final Map<String, FormatPluginConfig> formats;
  private final boolean impersonationEnabled;
  private final SchemaMutability schemaMutability;

  // used internally
  private final URI uri;

  @JsonCreator
  public FileSystemConfig(
      @JsonProperty("connection") String connection,
      @JsonProperty("path") String path,
      @JsonProperty("config") Map<String, String> config,
      @JsonProperty("formats") Map<String, FormatPluginConfig> formats,
      @JsonProperty(value = "impersonationEnabled", defaultValue = "false") boolean impersonationEnabled,
      @JsonProperty(value = "schemaMutability", defaultValue = "ALL") SchemaMutability schemaMutability) {
    this.connection = connection;
    this.path = path == null ? "/" : path;
    this.config = (config == null) ? null : ImmutableMap.copyOf(config);
    this.formats = (formats == null) ? null : ImmutableMap.copyOf(formats);
    this.impersonationEnabled = impersonationEnabled;
    this.uri = connection == null? null: getURI(connection);
    this.schemaMutability = schemaMutability == null ? SchemaMutability.ALL : schemaMutability;
  }

  public FileSystemConfig(URI uri, SchemaMutability schemaMutability) {
    this(uri.toString(), uri.getPath(), null, null, false, schemaMutability);
  }

  public SchemaMutability getSchemaMutability() {
    return schemaMutability;
  }

  public String getConnection() {
    return connection;
  }

  public String getPath() {
    return path;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public Map<String, FormatPluginConfig> getFormats() {
    return formats;
  }

  public boolean isImpersonationEnabled() {
    return impersonationEnabled;
  }

  protected static URI getURI(String connection) {
    try {
      return new URI(connection);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to parse filesystem URI.", e);
    }
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FileSystemConfig that = (FileSystemConfig) o;
    return impersonationEnabled == that.impersonationEnabled &&
        Objects.equals(connection, that.connection) &&
        Objects.equals(path, that.path) &&
        Objects.equals(config, that.config) &&
        Objects.equals(formats, that.formats) &&
        Objects.equals(schemaMutability, that.schemaMutability);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection, path, config, formats, impersonationEnabled, schemaMutability);
  }
}
