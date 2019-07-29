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
package com.dremio.dac.daemon;

import static com.dremio.common.util.DremioVersionInfo.VERSION;

import com.dremio.common.JULBridge;
import com.dremio.common.ProcessExit;
import com.dremio.common.Version;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.cmd.upgrade.Upgrade;
import com.dremio.dac.server.DACConfig;
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
    JULBridge.configure();
  }

  public static final String DAEMON_MODULE_CLASS = "dremio.daemon.module.class";

  private static class AutoUpgrade extends Upgrade {

    public AutoUpgrade(DACConfig dacConfig, ScanResult classPathScan) {
      super(dacConfig, classPathScan, false);
    }

    @Override
    protected void ensureUpgradeSupported(Version storeVersion) {
      // Check if store version is up to date, i.e store version is greater or equal to the greatest version
      // of all upgrade tasks. If not, and if auto upgrade is not enabled, fail.
      if (!getDACConfig().isAutoUpgrade()) {
          Preconditions.checkState(
              UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) >= 0,
              "KVStore has an older version (%s) than the server (%s), please run the upgrade tool first",
              storeVersion.getVersion(), VERSION.getVersion());
      }

      // Check if store version is smaller or equal to the code version.
      // If not, and if not allowed by config, fail.
      // Needed to be able to run multiple versions of Dremio on single version of KVStore
      if (getDACConfig().allowNewerKVStore) {
        if (UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) > 0) {
          logger.warn(String.format(
            "This Dremio version %s is older then KVStore version %s", VERSION.getVersion(), storeVersion.getVersion()));
        }
      } else {
        Preconditions.checkState(
          UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) <= 0,
          "KVStore has a newer version (%s) than running Dremio server (%s)",
          storeVersion.getVersion(), VERSION.getVersion());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      final DACConfig config = DACConfig.newConfig();
      final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
      final ScanResult classPathScan = ClassPathScanner.fromPrescan(sabotConfig);

      if (config.isMaster) {
        // Try autoupgrade before starting daemon
        AutoUpgrade autoUpgrade = new AutoUpgrade(config, classPathScan);
        autoUpgrade.run(false);
      }

      final DACModule module = sabotConfig.getInstance(DAEMON_MODULE_CLASS, DACModule.class, DACDaemonModule.class);
      final DACDaemon daemon = DACDaemon.newDremioDaemon(config, classPathScan, module);
      daemon.init();
      daemon.closeOnJVMShutDown();
    } catch (final Throwable ex) {
      ProcessExit.exit(ex, "Failure while starting services.", 4);
    }
  }

}
