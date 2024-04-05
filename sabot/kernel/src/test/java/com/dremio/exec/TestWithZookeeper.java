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
package com.dremio.exec;

import com.dremio.common.config.SabotConfig;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestWithZookeeper extends ExecTest {

  private static TestingServer zk;
  private static volatile SabotConfig config;

  @BeforeClass
  public static void setUp() throws Exception {
    zk = new TestingServer(true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (zk != null) {
      zk.close();
    }
  }

  public static SabotConfig getConfig() {
    if (config == null) {
      synchronized (TestWithZookeeper.class) {
        if (config == null) {
          if (zk == null) {
            throw new IllegalStateException("ZooKeeper server is not running.");
          }
          Properties overrides = new Properties();
          overrides.setProperty(ExecConstants.ZK_CONNECTION, zk.getConnectString());
          config = SabotConfig.create(overrides);
        }
      }
    }
    return config;
  }
}
