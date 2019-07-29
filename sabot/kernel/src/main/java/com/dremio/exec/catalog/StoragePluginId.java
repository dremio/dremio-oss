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
package com.dremio.exec.catalog;

import java.util.Arrays;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

/**
 * Describes a particular storage plugin instance.
 */
public class StoragePluginId {

  private final ConnectionConf<?, ?> connection;
  private final SourceConfig config;
  private final SourceCapabilities capabilities;
  private final int hashCode;

  @JsonCreator
  public StoragePluginId(
      @JsonProperty("config") SourceConfig config,
      @JsonProperty("capabilities") SourceCapabilities capabilities,
      @JacksonInject ConnectionReader confReader
      ) {
    super();
    this.config = Preconditions.checkNotNull(config);
    this.connection = confReader.getConnectionConf(config);
    this.capabilities = capabilities;
    this.hashCode = Objects.hashCode(config, capabilities);
  }

  public StoragePluginId(
      SourceConfig config,
      ConnectionConf<?, ?> connection,
      SourceCapabilities capabilities
      ) {
    this.config = Preconditions.checkNotNull(config);
    this.connection = connection;
    this.capabilities = capabilities;
    this.hashCode = Objects.hashCode(config, capabilities);
    assert Arrays.equals(connection.toBytes(), config.getConfig().toByteArray());
  }

  @JsonIgnore
  public String getName() {
    return config.getName();
  }

  @SuppressWarnings("unchecked")
  @JsonIgnore
  public <T extends ConnectionConf<?, ?>> T getConnectionConf() {
    return (T) connection;
  }

  @JsonIgnore
  public SourceType getType() {
    return connection.getClass().getAnnotation(SourceType.class);
  }

  public SourceConfig getConfig() {
    return config;
  }

  @JsonIgnore
  public SourceConfig getClonedConfig() {
    byte[] bytes = ProtostuffIOUtil.toByteArray(config, SourceConfig.getSchema(), LinkedBuffer.allocate());
    SourceConfig newConfig = new SourceConfig();
    ProtostuffIOUtil.mergeFrom(bytes, newConfig, SourceConfig.getSchema());
    return newConfig;

  }
  public SourceCapabilities getCapabilities(){
    return capabilities;
  }

  @Override
  public String toString() {
    return String.format("%s (%s)", config.getName(), config.getType());
  }

  public String generateRuleName(String description){
    return description + ":" + config.getName();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof StoragePluginId)) {
      return false;
    }
    StoragePluginId castOther = (StoragePluginId) other;
    return
        Objects.equal(config, castOther.config)
        && Objects.equal(capabilities, castOther.capabilities);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

}
