/*
 * Copyright (C) 2017 Dremio Corporation
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

/**
 * Retrieves the version of an entity
 * (For Optimistic Concurrency Control)
 * @param <T> the entity type
 */
public interface VersionExtractor<T> {

  /**
   * @param value to extract the version from
   * @return the current version
   */
  Long getVersion(T value);

  /**
   * version++: increments the version of the value and returns the previous version
   * @param value to extract the version from
   * @return the previous version
   */
  Long incrementVersion(T value);

  /**
   * set version on value
   * @param value to set version on
   * @param version version to set
   */
  void setVersion(T value, Long version);
}
