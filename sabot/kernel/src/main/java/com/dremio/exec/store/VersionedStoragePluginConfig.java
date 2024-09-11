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

/** Marker interface for plugin configurations that support versioning */
public interface VersionedStoragePluginConfig {

  /** When set to true, privileges set on the sources natively will be used. */
  // TODO: DX-92677: Remove this as this is a dangerous hack.
  default boolean useSourceProvidedPrivileges() {
    return false;
  }

  /** Returns the catalog Id. */
  // TODO: DX-92694: Remove this as not all plugins have a catalog Id.
  default String getCatalogId() {
    return null;
  }
}
