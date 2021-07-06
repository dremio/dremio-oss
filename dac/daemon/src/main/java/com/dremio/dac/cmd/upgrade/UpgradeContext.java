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
package com.dremio.dac.cmd.upgrade;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;

/**
 * contains all stores required by all upgrade tasks
 */
public class UpgradeContext {

  private final KVStoreProvider kvStoreProvider;
  private final LegacyKVStoreProvider legacyKVStoreProvider;
  private final LogicalPlanPersistence lpPersistence;
  private final ConnectionReader connectionReader;
  private final ScanResult scanResult;

  UpgradeContext(
    KVStoreProvider kvStoreProvider,
    LegacyKVStoreProvider legacyKVStoreProvider,
    LogicalPlanPersistence lpPersistence,
    ConnectionReader connectionReader,
    ScanResult scanResult
  ) {
    this.kvStoreProvider = kvStoreProvider;
    this.legacyKVStoreProvider = legacyKVStoreProvider;
    this.lpPersistence = lpPersistence;
    this.connectionReader = connectionReader;
    this.scanResult = scanResult;
  }

  public KVStoreProvider getKvStoreProvider() {
    return kvStoreProvider;
  }

  public LegacyKVStoreProvider getLegacyKVStoreProvider() {
    return legacyKVStoreProvider;
  }

  public LogicalPlanPersistence getLpPersistence() {
    return lpPersistence;
  }

  public ConnectionReader getConnectionReader() {
    return connectionReader;
  }

  public ScanResult getScanResult() {
    return scanResult;
  }
}
