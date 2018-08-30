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

import java.util.logging.LogManager;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.dac.server.DACConfig;
import com.dremio.exec.util.GuavaPatcher;
import com.google.api.client.util.Throwables;


/**
 * Starts the Dremio daemon in a YARN container and inject dependencies
 */
public class YarnDaemon implements Runnable, AutoCloseable {
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


  private volatile AutoCloseable closeable;

  public YarnDaemon(String[] args) {}

  @Override
  public void run() {
    try (TimedBlock b = Timer.time("main")) {
      DACConfig config = DACConfig.newConfig();

      final SabotConfig sabotConfig = config.getConfig().getSabotConfig();
      final DACModule module = sabotConfig.getInstance(DremioDaemon.DAEMON_MODULE_CLASS, DACModule.class, DACDaemonModule.class);
      final DACDaemon daemon = DACDaemon.newDremioDaemon(config, ClassPathScanner.fromPrescan(sabotConfig), module);
      closeable = daemon;
      daemon.init();
      daemon.closeOnJVMShutDown();
      daemon.awaitClose();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (closeable != null) {
      closeable.close();
    }

  }

}
