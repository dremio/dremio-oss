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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.config.DremioConfig;
import com.dremio.dac.daemon.DACDaemon.ClusterMode;
import com.dremio.dac.server.BaseClientUtils;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.JSONUtil;
import com.dremio.test.DremioTest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** tests for serving the UI */
public class TestUIServer {

  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  private static DACDaemon dremioDaemon;
  private static Client client;
  private static WebTarget rootTarget;

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    try (TimedBlock b = Timer.time("TestUIServer.@BeforeClass")) {
      dremioDaemon =
          DACDaemon.newDremioDaemon(
              DACConfig.newDebugConfig(DremioTest.DEFAULT_SABOT_CONFIG)
                  .autoPort(true)
                  .allowTestApis(true)
                  .writePath(folder.getRoot().getAbsolutePath())
                  .with(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, false)
                  .clusterMode(ClusterMode.LOCAL)
                  .serveUI(true),
              DremioTest.CLASSPATH_SCAN_RESULT);
      dremioDaemon.init();
      client = BaseClientUtils.newClient(JSONUtil.prettyMapper());
      rootTarget = client.target("http://localhost:" + dremioDaemon.getWebServer().getPort());
    }
  }

  @AfterClass
  public static void close() throws Exception {
    try (TimedBlock b = Timer.time("TestUIServer.@AfterClass")) {
      if (dremioDaemon != null) {
        dremioDaemon.close();
      }
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testRoot() throws Exception {
    Response response = rootTarget.request(JSON).get();
    String body = response.readEntity(String.class);
    assertEquals(body, 200, response.getStatus());
    assertTrue(body, body.contains("<title>Dremio</title>"));
  }
}
