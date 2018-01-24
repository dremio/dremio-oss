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
package com.dremio.exec.store.sys;

import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A namesake plugin configuration for system tables.
 */
@JsonTypeName("sys")
public class SystemTablePluginConfig extends StoragePluginConfig {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTablePluginConfig.class);

  public static final String NAME = "system-tables";

  public static final SystemTablePluginConfig INSTANCE = new SystemTablePluginConfig();

  public SystemTablePluginConfig() {
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SystemTablePluginConfig;
  }

  @Override
  public int hashCode() {
    return 1;
  }

}
