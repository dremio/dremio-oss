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
package com.dremio.service.coordinator.zk;

import java.io.IOException;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.rules.ExternalResource;

import com.google.common.base.Throwables;

final class ZooKeeperServerResource extends ExternalResource {
  private TestingServer testingServer;
  private CuratorZookeeperClient zkClient;

  @Override
  protected void before() throws Throwable {
    testingServer = new TestingServer(true);
    zkClient = new CuratorZookeeperClient(testingServer.getConnectString(), 10000, 10000, null, new RetryOneTime(2000));
    zkClient.start();
    zkClient.blockUntilConnectedOrTimedOut();
  }

  @Override
  protected void after() {
    try {
      zkClient.close();
      testingServer.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public ZooKeeper getZKClient() throws Exception {
    return zkClient.getZooKeeper();
  }

  public int getPort() {
    return testingServer.getPort();
  }

  public String getConnectString() {
    return testingServer.getConnectString();
  }

  public void restartServer() throws IOException {
    try {
      testingServer.restart();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  public void closeServer() throws IOException {
    testingServer.close();
  }
}
