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
import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * plugin config for user uploaded files.
 */
@JsonTypeName(HomeFileConfig.HOME_PLUGIN_NAME)
public class HomeFileSystemPluginConfig extends FileSystemConfig {

  private final HomeFileConfig config;

  @JsonCreator
  public HomeFileSystemPluginConfig(
      @JsonProperty("connection") String connection,
      @JsonProperty("host") String host,
      @JsonProperty("config") Map<String, String> config,
      @JsonProperty("formats") Map<String, FormatPluginConfig> formats,
      @JsonProperty(value = "impersonationEnabled", defaultValue = "false") boolean impersonationEnabled,
      @JsonProperty(value = "schemaMutability", defaultValue = "USER_VIEW") SchemaMutability schemaMutability) throws ExecutionSetupException, IOException {
    super(connection, host, config, formats, impersonationEnabled, schemaMutability);
    this.config = new HomeFileConfig(getURI(connection), host);
  }

  public HomeFileSystemPluginConfig(HomeFileConfig config) throws ExecutionSetupException {
    super(config.getLocation(), SchemaMutability.USER_VIEW);
    this.config = config;
  }

  public String getHost(){
    return config.getHost();
  }

  @JsonIgnore
  public HomeFileConfig getHomeFileConfig(){
    return config;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((config == null) ? 0 : config.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HomeFileSystemPluginConfig other = (HomeFileSystemPluginConfig) obj;
    if (config == null) {
      if (other.config != null) {
        return false;
      }
    } else if (!config.equals(other.config)) {
      return false;
    }
    return true;
  }


}
