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
package com.dremio.common.logical;

import java.util.Set;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.store.StoragePluginConfig;


public abstract class StoragePluginConfigBase extends StoragePluginConfig {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginConfigBase.class);

  public static Set<Class<? extends StoragePluginConfig>> getSubTypes(final ScanResult classpathScan) {
    final Set<Class<? extends StoragePluginConfig>> packages = classpathScan.getImplementations(StoragePluginConfig.class);
    logger.debug("Found {} logical operator classes: {}.", packages.size(), packages);
    return packages;
  }

  @Override
  public abstract boolean equals(Object o);

}
