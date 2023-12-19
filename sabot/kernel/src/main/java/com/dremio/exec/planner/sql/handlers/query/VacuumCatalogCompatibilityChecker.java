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
package com.dremio.exec.planner.sql.handlers.query;

import java.lang.reflect.InvocationTargetException;

import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;

/**
 * Helps in identifying the compatibility of VACUUM CATALOG implementation against different varieties of storage plugins.
 */
public interface VacuumCatalogCompatibilityChecker {

  /**
   * Validates if the source is compatible for running VACUUM CATALOG given the support options states.
   */
  void checkCompatibility(StoragePlugin plugin, OptionManager options);

  static VacuumCatalogCompatibilityChecker getInstance(ScanResult scanResult) {
    final VacuumCatalogCompatibilityChecker noOpChecker = (plugin, options) ->
      LoggerFactory.getLogger(VacuumCatalogCompatibilityChecker.class).warn("Skipped plugin compatibility check");

    return scanResult.getImplementations(VacuumCatalogCompatibilityChecker.class).stream().map(impl -> {
        try {
          return impl.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
          return noOpChecker;
        }
      }).findFirst().orElse(noOpChecker);
  }
}
