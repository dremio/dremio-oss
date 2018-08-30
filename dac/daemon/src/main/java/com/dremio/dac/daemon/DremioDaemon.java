/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.logging.LogManager;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.dremio.common.CatastrophicFailure;
import com.dremio.common.Version;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.dac.cmd.upgrade.Upgrade;
import com.dremio.dac.cmd.upgrade.UpgradeStats;
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
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public static final String DAEMON_MODULE_CLASS = "dremio.daemon.module.class";

  private static class AutoUpgrade extends Upgrade {

    public AutoUpgrade(DACConfig dacConfig) {
      super(dacConfig, false);
    }

    @Override
    protected void ensureUpgradeSupported(Version storeVersion) {
      // Check if store version is up to date, i.e store version is greater or equal to the greatest version
      // of all upgrade tasks. If not, and if autoupgrade is not enabled, fail.
      if (!getDacConfig().isAutoUpgrade()) {
        Preconditions.checkState(
            UPGRADE_VERSION_ORDERING.compare(storeVersion, TASKS_GREATEST_MAX_VERSION) >= 0,
            "KVStore has an older version (%s) than the server (%s), please run the upgrade tool first",
            storeVersion.getVersion(), VERSION.getVersion());
      }

      // Check if store version is smaller or equal to the code version. If not, and if not allowed by config,
      // fail.
      if (!getDacConfig().allowNewerKVStore) {
        Preconditions.checkState(
            UPGRADE_VERSION_ORDERING.compare(storeVersion, VERSION) <= 0,
            "KVStore has a newer version (%s) than running Dremio server (%s)",
            storeVersion.getVersion(), VERSION.getVersion());
      }

      // Check if store version is greater or equal to the smallest version of all upgrade tasks.
      // If not, fail
      Preconditions.checkState(
          UPGRADE_VERSION_ORDERING.compare(storeVersion, TASKS_SMALLEST_MIN_VERSION) >= 0,
          "Cannot run upgrade tool on versions below %s",
          TASKS_SMALLEST_MIN_VERSION.getVersion());
    }
  }

  public static void main(String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      DACConfig config = DACConfig.newConfig();

      if (config.isMaster) {
        // Try autoupgrade before starting daemon
        AutoUpgrade autoUpgrade = new AutoUpgrade(config);
        UpgradeStats upgradeStats = autoUpgrade.run();
        if (upgradeStats != Upgrade.NO_UPGRADE) {
          System.out.println(upgradeStats);
        }
      }

      final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
      final DACModule module = sabotConfig.getInstance(DAEMON_MODULE_CLASS, DACModule.class, DACDaemonModule.class);
      final DACDaemon daemon = DACDaemon.newDremioDaemon(config, ClassPathScanner.fromPrescan(sabotConfig), module);
      daemon.init();
      daemon.closeOnJVMShutDown();
    } catch (final Throwable ex) {
      CatastrophicFailure.exit(ex, "Failed to start services, daemon exiting.", 1);
    }
  }

}
