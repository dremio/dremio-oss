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
package com.dremio.exec.store.hive;

import java.util.Map;

import com.dremio.common.logical.StoragePluginConfigBase;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;

@JsonTypeName(HiveStoragePluginConfig.NAME)
public class HiveStoragePluginConfig extends StoragePluginConfigBase {
  public static final String NAME = "hive";

  public final Map<String, String> config;

  /**
   * Create an instance.
   * @param config List of configuration override properties.
   */
  @JsonCreator
  public HiveStoragePluginConfig(@JsonProperty("config") Map<String, String> config) {
    this.config = config == null ? null : ImmutableMap.copyOf(config);
  }

  @Override
  public int hashCode() {
    return 31 + ((config == null) ? 0 : config.hashCode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveStoragePluginConfig that = (HiveStoragePluginConfig) o;

    if (config != null ? !config.equals(that.config) : that.config != null) {
      return false;
    }

    return true;
  }

}
