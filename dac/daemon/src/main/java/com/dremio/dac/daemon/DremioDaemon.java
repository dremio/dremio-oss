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

import java.util.logging.LogManager;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.dac.server.DacConfig;
import com.dremio.dac.server.NASSourceConfigurator;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.dac.server.SourceToStoragePluginConfig;
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
import com.dremio.exec.util.GuavaPatcher;


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

  public static void main(String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      DacConfig config = DacConfig.newConfig();
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
          new RedshiftSourceConfigurator());
      final DACDaemon daemon = DACDaemon.newDremioDaemon(config, ClassPathScanner.fromPrescan(sabotConfig), module, sourceConfig);
      daemon.init();
      daemon.closeOnJVMShutDown();
    }
  }

}
