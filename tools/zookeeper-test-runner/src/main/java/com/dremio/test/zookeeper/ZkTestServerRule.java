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
package com.dremio.test.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.rules.ExternalResource;



/**
 * Test ZK Serve JUnit ruler.
 */
public class ZkTestServerRule extends ExternalResource {
  private final ZkTestServer testingServer;

  public ZkTestServerRule() {
    testingServer = new ZkTestServer();
  }

  public ZkTestServerRule(String path) {
    testingServer = new ZkTestServer(path);
  }

  @Override
  public void before() throws Throwable {
    testingServer.start();
  }

  @Override
  protected void after() {
    testingServer.close();
  }

  public ZooKeeper getZKClient() throws Exception {
    return testingServer.getZKClient();
  }

  public void restartServer() throws IOException {
    testingServer.restartServer();
  }

  public void closeServer() throws IOException {
    testingServer.closeServer();
  }

  public int getPort() {
    return testingServer.getPort();
  }

  public String getConnectionString() {
    return testingServer.getConnectionString();
  }

}
