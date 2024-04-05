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
package com.dremio.exec.store;

import com.dremio.exec.catalog.StoragePluginId;

public interface StoragePluginResolver {

  /**
   * Get a StoragePlugin according to the provided StoragePluginId. StoragePluginId is used as the
   * key when moving from planning to execution and will generally be exposed by Tables returned by
   * Catalog. Typically, a CatalogService consumer should never create a StoragePluginId.
   *
   * <p>This method will update the storage plugin if the config is newer than the one held locally.
   * This method will not check the kvstore for additional updates, simply trusting the provided
   * PluginId as a canonical source of truth. If the provided StoragePluginId has an older version
   * of configuration than the one currently active on this node, an exception will be thrown.
   *
   * @param pluginId
   * @return A StoragePlugin casted to the expected output.
   */
  <T extends StoragePlugin> T getSource(StoragePluginId pluginId);
}
