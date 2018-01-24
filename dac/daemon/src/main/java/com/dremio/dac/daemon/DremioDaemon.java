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
package com.dremio.dac.daemon;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.cmd.upgrade.Upgrade.TASKS_GREATEST_MAX_VERSION;
import static com.dremio.dac.cmd.upgrade.Upgrade.UPGRADE_VERSION_ORDERING;
import static com.dremio.dac.util.ClusterVersionUtils.fromClusterVersion;

import java.io.File;
import java.util.logging.LogManager;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.dremio.common.Version;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.cmd.upgrade.Upgrade;
import com.dremio.dac.cmd.upgrade.UpgradeStats;
import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.NASSourceConfigurator;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.dac.server.SourceToStoragePluginConfig;
import com.dremio.dac.sources.AzureDataLakeConfigurator;
import com.dremio.dac.sources.DB2SourceConfigurator;
import com.dremio.dac.sources.ElasticSourceConfigurator;
import com.dremio.dac.sources.HBaseSourceConfigurator;
import com.dremio.dac.sources.HDFSSourceConfigurator;
import com.dremio.dac.sources.HiveSourceConfigurator;
import com.dremio.dac.sources.MSSQLSourceConfigurator;
import com.dremio.dac.sources.MYSQLSourceConfigurator;
import com.dremio.dac.sources.MaprFsSourceConfigurator;
import com.dremio.dac.sources.MongoSourceConfigurator;
import com.dremio.dac.sources.OracleSourceConfigurator;
import com.dremio.dac.sources.PostgresSourceConfigurator;
import com.dremio.dac.sources.RedshiftSourceConfigurator;
import com.dremio.dac.sources.S3SourceConfigurator;
import com.dremio.dac.support.SupportService;
import com.dremio.dac.support.SupportService.SupportStoreCreator;
import com.dremio.dac.util.ClusterVersionUtils;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.util.GuavaPatcher;
import com.google.common.base.Preconditions;


/**
 * Starts the Dremio daemon and inject dependencies
 */
public class DremioDaemon {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioDaemon.class);

  static {
    /*
     * HBase client uses older version of Guava's Stopwatch API,
     * while Dremio ships with 18.x which has changes the scope of
     * these API to 'package', this code make them accessible.
     */
    GuavaPatcher.patch();

    /*
     * Route JUL logging messages to SLF4J.
     */
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public static final String DAEMON_MODULE_CLASS = "dremio.daemon.module.class";

  private static void checkVersion(DACConfig config) throws Exception {
    final String dbDir = config.getConfig().getString(DremioConfig.DB_PATH_STRING);
    final File dbFile = new File(dbDir);

    if (!dbFile.exists()) {
      // New installation. Skipping
      logger.debug("initial setup: no version check");
      return;
    }

    final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
    final ScanResult classpathScan = ClassPathScanner.fromPrescan(sabotConfig);
    try (final KVStoreProvider storeProvider = new LocalKVStoreProvider(classpathScan, dbDir, false, true, false, true)) {
      storeProvider.start();

      final KVStore<String, ClusterIdentity> supportStore = storeProvider.getStore(SupportStoreCreator.class);
      final ClusterIdentity identity = Preconditions.checkNotNull(supportStore.get(SupportService.CLUSTER_ID), "No Cluster Identity found");

      final Version storeVersion = fromClusterVersion(identity.getVersion());
      if (storeVersion == null || UPGRADE_VERSION_ORDERING.compare(storeVersion, TASKS_GREATEST_MAX_VERSION) < 0) {
        // Check if autoupgrade is enabled
        if (!config.isAutoUpgrade()) {
          throw new IllegalStateException("KVStore has an older version than the server, please run the upgrade tool first");
        }

        UpgradeStats upgradeStats = Upgrade.upgrade(sabotConfig, classpathScan, storeProvider);
        System.out.println(upgradeStats);
      } else if (UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) < 0) {
        // No upgrade task required. Safe to continue
        logger.info("Newer version of Dremio detected but no upgrade required. Updating kvstore version from %s to %s.", storeVersion, VERSION);
        identity.setVersion(ClusterVersionUtils.toClusterVersion(VERSION));
        supportStore.put(SupportService.CLUSTER_ID, identity);
      } else if (UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) > 0) {
        // KVStore is newer than running server
        final String msg = String.format("KVStore has a newer version (%s) than running Dremio server (%s)",
          storeVersion.getVersion(), VERSION.getVersion());

        if (!config.allowNewerKVStore) {
          throw new IllegalStateException(msg);
        }

        logger.warn(msg);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      DACConfig config = DACConfig.newConfig();

      if (config.isMaster) {
        // Check version before starting daemon
        checkVersion(config);
      }

      final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
      final DACModule module = sabotConfig.getInstance(DAEMON_MODULE_CLASS, DACModule.class, DACDaemonModule.class);
      final SourceToStoragePluginConfig sourceConfig = SingleSourceToStoragePluginConfig.of(
          new NASSourceConfigurator(),
          new ElasticSourceConfigurator(),
          new HDFSSourceConfigurator(),
          new MaprFsSourceConfigurator(),
          new HBaseSourceConfigurator(),
          new HiveSourceConfigurator(),
          new MongoSourceConfigurator(),
          new MSSQLSourceConfigurator(),
          new MYSQLSourceConfigurator(),
          new OracleSourceConfigurator(),
          new PostgresSourceConfigurator(),
          new S3SourceConfigurator(),
          new DB2SourceConfigurator(),
          new RedshiftSourceConfigurator(),
          new AzureDataLakeConfigurator());
      final DACDaemon daemon = DACDaemon.newDremioDaemon(config, ClassPathScanner.fromPrescan(sabotConfig), module, sourceConfig);
      daemon.init();
      daemon.closeOnJVMShutDown();
    }
  }

}
