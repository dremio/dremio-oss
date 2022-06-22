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
package com.dremio.exec.store.metadatarefresh;

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;

/**
 * Metadata refresh utils
 */
public final class MetadataRefreshUtils {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataRefreshUtils.class);
  /**
   * checks if unlimited splits is enabled
   * @param options
   * @return
   */
  public static boolean unlimitedSplitsSupportEnabled(final OptionManager options) {
    return options.getOption(ExecConstants.ENABLE_ICEBERG) &&
      options.getOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT);
  }
  /**
   * checks if metadata source is available and supports iceberg
   * @return
   */
  public static boolean metadataSourceAvailable(final CatalogService catalogService) {
    FileSystemPlugin<?> metadataPlugin = null;
    try {
      metadataPlugin = catalogService.getSource(METADATA_STORAGE_PLUGIN_NAME);
    }
    catch (Exception e) {
      logger.debug("Exception while getting the plugin for the source [{}].", METADATA_STORAGE_PLUGIN_NAME, e);
      return false;
    }
    if (metadataPlugin == null) {
      logger.debug("Source {} could not be found", METADATA_STORAGE_PLUGIN_NAME);
      return false;
    }
    boolean supportsIceberg = metadataPlugin.supportsIcebergTables();
    if (!supportsIceberg) {
      logger.debug("metadata plugin does not supports Iceberg tables");
    }
    return supportsIceberg;
  }

  private MetadataRefreshUtils() {}
}
