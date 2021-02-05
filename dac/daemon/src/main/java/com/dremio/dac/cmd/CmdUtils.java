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
package com.dremio.dac.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Optional;

import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LocalKVStoreProvider;

/**
 * The CmdUtils is responsible to provide the general methods for command line operations utils.
 */
public final class CmdUtils {

  private CmdUtils() {}


  /**
   * Gets the store provider, if data exists.
   *
   * <p>
   * If data not exists, it returns null.
   *
   * @param dremioConfig  a Dremio configuration object to get the database path
   * @param classPathScan the Classpath scanning utility to get the Sabot configuration
   * @return              the store provider, if data exists
   */
  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig, ScanResult classPathScan) {
    return getKVStoreProvider(dremioConfig, classPathScan, true);
  }


  /**
   * Gets the store provider, if data exists.
   *
   * <p>
   * If data not exists, it returns null.
   *
   * @param dremioConfig  a Dremio configuration object to get the database path
   * @param classPathScan the Classpath scanning utility to get the Sabot configuration
   * @param noDBOpenRetry a flag which indicates if it should retry in case of the database can't open
   * @return              the store provider, if data exists
   */
  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig, ScanResult classPathScan, boolean noDBOpenRetry) {
    final String dbDir = dremioConfig.getString(DremioConfig.DB_PATH_STRING);
    final File dbFile = new File(dbDir);

    if (!dbFile.exists()) {
      return Optional.empty();
    }

    String[] listFiles = dbFile.list();
    // An empty array means no file in the directory, so do not try to open it.
    // A null value means dbFile is not a directory. Let the caller handle it.
    if (listFiles != null && listFiles.length == 0) {
      return Optional.empty();
    }

    return Optional.of(
      new LocalKVStoreProvider(classPathScan, dbDir, false, true, noDBOpenRetry));
  }

  /**
   * Gets the store provider, if data exists.
   *
   * @param dremioConfig a Dremio configuration object to get the database path
   * @return             the store provider, if data exists
   */
  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig) {
    return CmdUtils.getKVStoreProvider(dremioConfig, ClassPathScanner.fromPrescan(dremioConfig.getSabotConfig()));
  }
}
