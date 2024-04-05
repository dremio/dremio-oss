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
package com.dremio.service.coordinator;

import com.dremio.service.coordinator.proto.DataCredentials;
import java.net.URI;

/** Interface to define project config */
public interface ProjectConfig {
  /**
   * Get accelerator config
   *
   * @return config
   */
  DistPathConfig getAcceleratorConfig();

  /**
   * Get scratch config
   *
   * @return config
   */
  DistPathConfig getScratchConfig();

  /**
   * Get uploads config
   *
   * @return config
   */
  DistPathConfig getUploadsConfig();

  /**
   * Get metadata config
   *
   * @return config
   */
  DistPathConfig getMetadataConfig();

  /**
   * Get gandiva cache config
   *
   * @return config
   */
  DistPathConfig getGandivaPersistentCacheConfig();

  /**
   * Get copyintoerrors config
   *
   * @return config
   */
  DistPathConfig getSystemIcebergTablesConfig();

  /**
   * @return
   */
  String getOrgId();

  /** Defines dist path config. */
  public static class DistPathConfig {
    private final URI uri;
    private final DataCredentials dataCredentials;

    public DistPathConfig(URI uri, DataCredentials dataCredentials) {
      this.uri = uri;
      this.dataCredentials = dataCredentials;
    }

    public URI getUri() {
      return uri;
    }

    public DataCredentials getDataCredentials() {
      return dataCredentials;
    }
  }
}
