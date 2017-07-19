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
package com.dremio.service.accelerator.materialization;

import java.util.Objects;

import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Configuration for the MaterializationStoragePluginConfigOTAS plugin
 */
@JsonTypeName(MaterializationStoragePluginConfig.NAME)
public class MaterializationStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "materialization";
  private final boolean enabled;

  @JsonCreator
  public MaterializationStoragePluginConfig(@JsonProperty("enabled") final boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MaterializationStoragePluginConfig that = (MaterializationStoragePluginConfig) o;
    return enabled == that.enabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled);
  }
}
