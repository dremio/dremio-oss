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
package com.dremio.exec.catalog;

import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Interface to perform actions on sources.
 */
public interface SourceCatalog extends PrivilegeCatalog {

  SourceState refreshSourceStatus(NamespaceKey key) throws Exception;

  /**
   * Get a source based on the provided name. If the source doesn't exist, synchronize with the
   * KVStore to confirm creation status.
   *
   * @param name
   * @return A StoragePlugin casted to the expected output.
   */
  <T extends StoragePlugin> T getSource(String name);

  /**
   * Create a source based on the provided configuration. Includes both the creation as well the
   * startup of the source. If the source fails to start, no creation will be done. The provided
   * configuration should have a null version. Failure to create or failure to start with throw an
   * exception. Additionally, if "store.plugin.check_state" is enabled, a plugin that starts but
   * then reveals a bad state, will also result in exception.
   *
   * @param config Configuration for the source.
   * @param attributes Optional namespace attributes to pass to namespace entity creation
   */
  void createSource(SourceConfig config, NamespaceAttribute... attributes);

  /**
   * Update an existing source with the given config. The config version must be the same as the
   * currently active source. If it isn't, this call will fail with an exception.
   *
   * @param config Configuration for the source.
   * @param attributes Optional namespace attributes to pass to namespace entity creation
   */
  void updateSource(SourceConfig config, NamespaceAttribute... attributes);

  /**
   * Delete a source with the provided config. If the source doesn't exist or the config doesn't
   * match, the method with throw an exception.
   *
   * @param config
   */
  void deleteSource(SourceConfig config);
}
