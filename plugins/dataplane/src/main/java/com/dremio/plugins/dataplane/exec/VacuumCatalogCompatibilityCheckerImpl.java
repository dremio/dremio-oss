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
package com.dremio.plugins.dataplane.exec;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.handlers.query.VacuumCatalogCompatibilityChecker;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vacuum Catalog has selective support against different filesystem sources. This class is used to
 * apply the selection of compatible source types
 */
public class VacuumCatalogCompatibilityCheckerImpl implements VacuumCatalogCompatibilityChecker {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(VacuumCatalogCompatibilityCheckerImpl.class);

  @Override
  public void checkCompatibility(StoragePlugin plugin, OptionManager options) {
    LOGGER.info("Executing compatibility checks, which are applicable for versioned sources.");
    if (!(plugin instanceof DataplanePlugin)) {
      throw UserException.validationError()
          .message("VACUUM CATALOG is only supported on versioned sources.")
          .buildSilently();
    }

    DataplanePlugin dataplanePlugin = (DataplanePlugin) plugin;
    StorageProviderType storageProvider = dataplanePlugin.getConfig().getStorageProvider();
    if (!options.getOption(ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG_ON_AZURE)
        && storageProvider != null
        && StorageProviderType.AZURE.equals(storageProvider)) {
      throw UserException.validationError()
          .message("VACUUM CATALOG is not supported on Azure file storage.")
          .buildSilently();
    }
  }
}
