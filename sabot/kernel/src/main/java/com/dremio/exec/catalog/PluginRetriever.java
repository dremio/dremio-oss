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
package com.dremio.exec.catalog;

/**
 * Interface to expose plugin access to children of CatalogServiceImpl
 */
interface PluginRetriever {

  /**
   * Find a plugin with the given name.
   * @param name The name to find.
   * @param synchronizeIfMissing Whether or not to synchronize source metadata if we don't find a plugin.
   * @return Return null if not found locally and !synchronizeIfMissing. Return found plugin or throw exception if using synchronizeOnMissing
   */
  ManagedStoragePlugin getPlugin(String name, boolean synchronizeIfMissing);
}
