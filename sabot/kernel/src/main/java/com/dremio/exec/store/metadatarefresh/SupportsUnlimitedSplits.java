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

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/** Interface to be implemented by StoragePlugins which support unlimited splits */
public interface SupportsUnlimitedSplits {

  /**
   * Whether the plugin allows unlimited splits for the given table. May not check if corresponding
   * support options are enabled or not
   *
   * @param handle
   * @param datasetConfig
   * @param user
   * @return
   */
  default boolean allowUnlimitedSplits(
      DatasetHandle handle, DatasetConfig datasetConfig, String user) {
    return false;
  }

  default void runRefreshQuery(String refreshQuery, String system) throws Exception {}
}
