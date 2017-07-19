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
package com.dremio.service.namespace;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Describes a particular storage plugin instance.
 */
public class StoragePluginId {

  private final String name;
  private final StoragePluginConfig config;
  private final StoragePluginType type;
  private final SourceCapabilities capabilities;
  private final int hashCode;

  @JsonCreator
  public StoragePluginId(
      @JsonProperty("name") String name,
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("capabilities") SourceCapabilities capabilities,
      @JsonProperty("type") StoragePluginType type) {
    super();
    this.name = Preconditions.checkNotNull(name);
    this.config = Preconditions.checkNotNull(config);
    this.type = Preconditions.checkNotNull(type);
    this.capabilities = capabilities;
    this.hashCode = Objects.hashCode(name, config, type, capabilities);
  }

  public StoragePluginId(
      String name,
      StoragePluginConfig config,
      StoragePluginType type) {
    this(name, config, SourceCapabilities.NONE, type);
  }

  public String getName() {
    return name;
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePluginConfig> T getConfig() {
    return (T) config;
  }

  public StoragePluginType getType() {
    return type;
  }

  public SourceCapabilities getCapabilities(){
    return capabilities;
  }

  @Override
  public String toString() {
    return String.format("%s (%s)", name, type);
  }

  public String generateRuleName(String description){
    return description + ":" + name;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof StoragePluginId)) {
      return false;
    }
    StoragePluginId castOther = (StoragePluginId) other;
    return Objects.equal(name, castOther.name) && Objects.equal(config, castOther.config)
        && Objects.equal(type, castOther.type) && Objects.equal(capabilities, castOther.capabilities);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

}
