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

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.perf.Timer;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.DACConfig;
import com.dremio.test.DremioTest;

/**
 * Test for Clean command
 */
public class TestClean extends BaseTestServer {

  private static DACConfig dacConfig = DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG).autoPort(true).allowTestApis(true)
      .addDefaultUser(true).serveUI(false).inMemoryStorage(false) // Need this to be a on-disk
                                                                  // kvstore for tests.
      .clusterMode(DACDaemon.ClusterMode.LOCAL);

  protected static DACConfig getDACConfig() {
    return dacConfig;
  }

  @BeforeClass
  public static void init() throws Exception {
    enableDefaultUser(false);
    Assume.assumeFalse(BaseTestServer.isMultinode());
    try (Timer.TimedBlock b = Timer.time("BaseTestServer.@BeforeClass")) {
      dacConfig = dacConfig.writePath(folder1.newFolder().getAbsolutePath());
      startDaemon();
    }
  }

  @AfterClass
  public static void shutdown() {
    enableDefaultUser(true);
  }

  private static void startDaemon() throws Exception {
    setCurrentDremioDaemon(DACDaemon.newDremioDaemon(dacConfig, DremioTest.CLASSPATH_SCAN_RESULT));
    setMasterDremioDaemon(null);
    getCurrentDremioDaemon().init();
    initClient();
  }

  @Test
  public void runWithOptions() throws Exception {
    getCurrentDremioDaemon().close();
    Clean.go(new String[] {});
    Clean.go(new String[] {"-o", "-i", "-c", "-j=30", "-p"});
    Clean.go(new String[] {"-h"});
  }

}
