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

package com.dremio.dac.admin;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.store.StoragePluginRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simple wrapper class that adds a name to a storage plugin configuration.
 *
 * In the persistent store, this name is used as the key. This class is used to
 * map the input from the rest API to update the storage plugin, where the name
 * is provided in the payload with the structure defined in this class.
 */
public class PluginConfigWrapper {

  private String name;
  private StoragePluginConfig config;
  private boolean exists;

  @JsonCreator
  public PluginConfigWrapper(@JsonProperty("name") String name, @JsonProperty("config") StoragePluginConfig config) {
    this.name = name;
    this.config = config;
    this.exists = config != null;
  }

  public String getName() {
    return name;
  }

  public StoragePluginConfig getConfig() {
    return config;
  }

  public boolean enabled() {
    return exists;
  }

  public void createOrUpdateInStorage(StoragePluginRegistry storage) throws ExecutionSetupException {
    storage.createOrUpdate(name, config, true);
  }

  public boolean exists() {
    return exists;
  }

  public boolean deleteFromStorage(StoragePluginRegistry storage) {
    if (exists) {
      storage.deletePlugin(name);
      return true;
    }
    return false;
  }
}
