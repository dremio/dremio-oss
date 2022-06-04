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

import javax.inject.Provider;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;

/**
 * cmd utils.
 */
public final class CmdUtils {

  private CmdUtils() {}

  /**
   * returns store provider, if data exists. Null if there is no data.
   * @param dremioConfig
   * @param classPathScan
   * @return store provider
   * @throws FileNotFoundException
   */
  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig, ScanResult classPathScan) {
    return getKVStoreProvider(dremioConfig, classPathScan, true, false);
  }

  /**
   * returns store provider, if data exists. Null if there is no data.
   * @param dremioConfig
   * @param classPathScan
   * @param noDBOpenRetry
   * @return store provider
   */
  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig, ScanResult classPathScan, boolean noDBOpenRetry, boolean noDBMessages) {
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
      new LocalKVStoreProvider(classPathScan, dbDir, false, true, noDBOpenRetry, noDBMessages));
  }

  public static Optional<LocalKVStoreProvider> getKVStoreProvider(DremioConfig dremioConfig) {
    return CmdUtils.getKVStoreProvider(dremioConfig, ClassPathScanner.fromPrescan(dremioConfig.getSabotConfig()));
  }

  /**
   * Returns an {@link OptionManager} provider that uses the provided {@link LocalKVStoreProvider}.
   * @param storeProvider
   * @param dremioConfig
   * @return  {@link OptionManager} provider.
   */
  public static Optional<Provider<OptionManager>> getOptionManager(LocalKVStoreProvider storeProvider, DremioConfig dremioConfig) {
    final ScanResult scanResult = ClassPathScanner.fromPrescan(dremioConfig.getSabotConfig());
    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(dremioConfig.getSabotConfig(), scanResult);
    final OptionValidatorListing optionValidatorListing = new OptionValidatorListingImpl(scanResult);
    final DefaultOptionManager defaultOptionManager = new DefaultOptionManager(optionValidatorListing);

    final SystemOptionManager systemOptionManager;
    final LegacyKVStoreProvider legacyKVStoreProvider = storeProvider.asLegacy();
    systemOptionManager = new SystemOptionManager(optionValidatorListing,
      lpp,
      () -> legacyKVStoreProvider,
      () -> null,
      null,
      false);
    try {
      systemOptionManager.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final OptionManagerWrapper optionManagerWrapper = OptionManagerWrapper.Builder.newBuilder()
      .withOptionValidatorProvider(optionValidatorListing)
      .withOptionManager(defaultOptionManager)
      .withOptionManager(systemOptionManager)
      .build();
    return Optional.of(() -> optionManagerWrapper);
  }
}
