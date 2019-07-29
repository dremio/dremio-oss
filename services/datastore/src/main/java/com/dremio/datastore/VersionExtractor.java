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
package com.dremio.datastore;

import com.dremio.common.AutoCloseables;

/**
 * Retrieves the version of an entity
 * (For Optimistic Concurrency Control)
 * @param <T> the entity type
 */
public interface VersionExtractor<T> {

  /**
   * Retrieves the numeric version.
   *
   * This is deprecated, using set/get tag for string versioning.
   *
   * @param value to extract the version from
   * @return the current version
   */
  @Deprecated
  default Long getVersion(T value) {
    return null;
  }

  /**
   * Sets the numeric version on value
   *
   * This is deprecated, using set/get tag for string versioning.
   *
   * @param value to set version on
   * @param version version to set
   */
  @Deprecated
  default void setVersion(T value, Long version) { }

  /**
   * Called before committing the KV value to the store (and before incrementVersion is called).
   *
   * In the case of an remote KV store, this is called before the version retrieved from the master is set.
   *
   * @param value that is being committed
   * @return a AutoClosable that is called if the commit fails
   */
  default AutoCloseable preCommit(T value) {
    return AutoCloseables.noop();
  }

  /**
   * Retrieves the tag
   *
   * @param value to extract the tag from
   * @return the current tag
   */
  String getTag(T value);

  /**
   * Sets the tag on value
   *
   * @param value to set tag on
   * @param tag tag to set
   */
  void setTag(T value, String tag);
}
