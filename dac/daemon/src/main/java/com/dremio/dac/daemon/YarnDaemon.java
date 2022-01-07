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

import static com.dremio.config.DremioConfig.KILL_REATTEMPT_INTERVAL_MS;
import static com.dremio.config.DremioConfig.MAX_KILL_ATTEMPTS;
import static com.dremio.config.DremioConfig.MISSED_POLLS_BEFORE_KILL;
import static com.dremio.config.DremioConfig.POLL_INTERVAL_MS;
import static com.dremio.config.DremioConfig.POLL_TIMEOUT_MS;
import static com.dremio.provision.yarn.YarnWatchdog.YARN_WATCHDOG_LOG_LEVEL;
import static org.apache.twill.internal.Constants.Files.APPLICATION_JAR;
import static org.apache.twill.internal.Constants.Files.TWILL_JAR;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import com.dremio.common.JULBridge;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.LivenessService;
import com.dremio.dac.server.liveness.ClasspathHealthMonitor;
import com.dremio.exec.util.GuavaPatcher;
import com.dremio.provision.yarn.YarnContainerHealthMonitor;
import com.dremio.provision.yarn.YarnWatchdog;
import com.google.common.base.Throwables;


/**
 * Starts the Dremio daemon in a YARN container and inject dependencies
 */
public class YarnDaemon implements Runnable, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(YarnDaemon.class);

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


  private volatile DACDaemon dacDaemon;

  public YarnDaemon(String[] args) {}

  @Override
  public void run() {
    try (TimedBlock b = Timer.time("main")) {

      // create a temporary local write path
      final Path localWritePath = Files.createTempDirectory("dremio-executor");
      final DACConfig config = DACConfig.newConfig()
          .writePath(localWritePath.toString());
      logger.info("Local write path set to '{}'", localWritePath);

      final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
      final DACModule module = sabotConfig.getInstance(DremioDaemon.DAEMON_MODULE_CLASS, DACModule.class, DACDaemonModule.class);
      try (final DACDaemon daemon = DACDaemon.newDremioDaemon(config, ClassPathScanner.fromPrescan(sabotConfig),
          module)) {
        dacDaemon = daemon;
        daemon.init();
        // Start yarn watchdog
        startYarnWatchdog(daemon);
        daemon.closeOnJVMShutDown();
        daemon.awaitClose();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getYarnWatchdogLogLevel() {
    final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(YarnWatchdog.class);
    if (logger.isTraceEnabled()) {
      return "TRACE";
    }
    if (logger.isDebugEnabled()) {
      return "DEBUG";
    }
    if (logger.isInfoEnabled()) {
      return "INFO";
    }
    if (logger.isWarnEnabled()) {
      return "WARN";
    }
    if (logger.isErrorEnabled()) {
      return "ERROR";
    }
    return "INFO";
  }

  private void startYarnWatchdog(final DACDaemon daemon) {
    Process yarnWatchdogProcess;
    final LivenessService livenessService = daemon.getLivenessService();
    if (!livenessService.isLivenessServiceEnabled()) {
      // Do not start watchdog since liveness service is disabled.
      logger.info("Liveness service is disabled, dremio will keep running without yarn watchdog.");
      return;
    }
    final int livenessPort = livenessService.getLivenessPort();
    if (livenessPort <= 0) {
      logger.error("Failed to start liveness service, dremio will exit.");
      System.exit(1);
    }

    livenessService.addHealthMonitor(new YarnContainerHealthMonitor());
    livenessService.addHealthMonitor(ClasspathHealthMonitor.newInstance());

    DremioConfig dremioConfig = daemon.getDACConfig().getConfig();
    final long watchedPID = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
    final long pollTimeoutMs = dremioConfig.getMilliseconds(POLL_TIMEOUT_MS);
    final long pollIntervalMs = dremioConfig.getMilliseconds(POLL_INTERVAL_MS);
    final int missedPollsBeforeKill = dremioConfig.getInt(MISSED_POLLS_BEFORE_KILL);
    final int maxKillAttempts = dremioConfig.getInt(MAX_KILL_ATTEMPTS);
    final long killReattemptIntervalMs = dremioConfig.getMilliseconds(KILL_REATTEMPT_INTERVAL_MS);
    final String classpath = APPLICATION_JAR + "/lib/*:" + TWILL_JAR + "/lib/*";
    final ProcessBuilder yarnWatchdogProcessBuilder = new ProcessBuilder("java",
      "-D" + YARN_WATCHDOG_LOG_LEVEL + "=" + getYarnWatchdogLogLevel(),
      "-cp", classpath, YarnWatchdog.class.getName(),
      Long.toString(watchedPID), Integer.toString(livenessPort), Long.toString(pollTimeoutMs),
      Long.toString(pollIntervalMs), Integer.toString(missedPollsBeforeKill), Integer.toString(maxKillAttempts),
      Long.toString(killReattemptIntervalMs))
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .redirectError(ProcessBuilder.Redirect.INHERIT);
    try {
      yarnWatchdogProcess = yarnWatchdogProcessBuilder.start();
    } catch (IOException e) {
      logger.warn("Failed to start yarn watchdog.", e);
      yarnWatchdogProcess = null;
    }
    if (yarnWatchdogProcess != null) {
      final Process finalYarnWatchdogProcess = yarnWatchdogProcess;
      Thread yarnWatchdogMonitor = new Thread("yarn-watchdog-monitor") {

        @Override
        public void run() {
          while (true) {
            while (finalYarnWatchdogProcess.isAlive()) {
              try {
                sleep(1000);
              } catch (InterruptedException e) {
                // ignore exception
              }
            }
            if (livenessService.getPollCount() > 0) {
              logger.error("Yarn watchdog is not alive, dremio will exit.");
              System.exit(1);
            } else {
              logger.warn("Yarn watchdog terminated without any poll, dremio will keep running without yarn watchdog.");
              return;
            }
          }
        }
      };
      yarnWatchdogMonitor.start();
    }
  }

  @Override
  public void close() throws Exception {
    // Because this is used by YARN, not closing daemon directly
    // but instead notifying the service thread to stop waiting on close and
    // and start cleaning up
    if (dacDaemon != null) {
      synchronized(this) {
        // Make sure to only close once to prevent deadlock
        if (dacDaemon != null) {
          dacDaemon.shutdown();
          dacDaemon = null;
        }
      }
    }
  }

}
