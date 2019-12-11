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

/**
 * System for monitoring the status of the catalog service. Also allows mocking testing. Can be expanded for monitoring
 * other operations of catalog service in the future.
 */
interface CatalogServiceMonitor {

  /**
   * A default, no-op monitor.
   */
  CatalogServiceMonitor DEFAULT = new CatalogServiceMonitor() {};

  /**
   * Construct an monitor for a particular plugin.
   *
   * The monitor implementation can decide whether to track each plugin separately or track as one.
   *
   * @param name Which plugin to retrieve the monitor for.
   * @return The monitor for the named plugin.
   */
  default CatalogServiceMonitor forPlugin(String name) {return this;}

  default void onWakeup() {}
  default void startAdhocRefreshWithLock() {}
  default void startAdhocRefresh() {}
  default void finishAdhocRefresh() {}
  default void startBackgroundRefresh() {}
  default void startBackgroundRefreshWithLock() {}
  default void finishBackgroundRefresh() {}
}

