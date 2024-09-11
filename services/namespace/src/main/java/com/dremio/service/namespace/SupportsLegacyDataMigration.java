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
package com.dremio.service.namespace;

/**
 * Interface containing default implementations to support breaking ConnectionConf/Source changes.
 */
public interface SupportsLegacyDataMigration {
  /**
   * Runs on Catalog start up and source creation. Can be overridden per source to remove
   * old/deprecated fields and/or create new fields. Persists changes to the KV store.
   *
   * @return true if changes were made, false if no changes were made
   */
  default boolean migrateLegacyFormat() {
    return false;
  }

  /**
   * Runs on existing source update. Can be overridden per source to remove old/deprecated fields
   * and/or create new fields. Persists changes to the KV store.
   */
  default void migrateLegacyFormat(AbstractConnectionConf conf) {
    migrateLegacyFormat();
  }

  /**
   * Can be overridden per source to temporarily re-add old/deprecated fields. Used to prevent
   * breaking API changes by affecting API responses. Does not persist changes to the KV store.
   */
  default void backFillLegacyFormat() {}
}
