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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.IOException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkTestServer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkTestServer.class);

  private final String path;
  private TestingServer testingServer;
  private volatile CuratorZookeeperClient zkClient;

  public ZkTestServer() {
    this("");
  }

  public ZkTestServer(String path) {
    Preconditions.checkNotNull(path, "Path cannot be null");
    this.path = path;
  }

  public void start() throws Throwable {
    testingServer = new TestingServer(true);
    testingServer.start();
    LOGGER.info("ZK Testing Server created");
  }

  @Override
  public void close() {
    LOGGER.info("Closing ZK Client and ZK Testing Server");
    try {
      if (zkClient != null) {
        zkClient.close();
      }
      if (testingServer != null) {
        testingServer.close();
      }
      if (zkClient != null && zkClient.isConnected()) {
        LOGGER.error("ZK Client still connected");
      }
      LOGGER.info("ZK Client and ZK testing server are closed");
    } catch (IOException e) {
      throw new RuntimeException("Failed to stop ZK server.", e);
    }
  }

  public ZooKeeper getZKClient() throws Exception {
    checkServerStarted();
    createClientIfRequired();
    return zkClient.getZooKeeper();
  }

  public void restartServer() throws IOException {
    checkServerStarted();
    try {
      testingServer.restart();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
    }
  }

  public void closeServer() throws IOException {
    checkServerStarted();
    testingServer.close();
  }

  public int getPort() {
    return testingServer.getPort();
  }

  public String getConnectionString() {
    checkServerStarted();
    return (testingServer.getConnectString() + this.path);
  }

  private void checkServerStarted() {
    if (testingServer == null) {
      throw new IllegalStateException("Test ZooKeeper server not started");
    }
  }

  private void createClientIfRequired() throws Exception {
    if (zkClient == null) {
      synchronized (this) {
        if (zkClient == null) {
          zkClient =
              new CuratorZookeeperClient(
                  testingServer.getConnectString(), 10000, 10000, null, new RetryOneTime(2000));
          zkClient.start();
          LOGGER.info("ZK Client started. State {}", zkClient.getZooKeeper().getState());

          if (!zkClient.blockUntilConnectedOrTimedOut()) {
            zkClient.close();
            zkClient = null;
            testingServer.close();
            throw new IllegalStateException(
                "ZK Client not connected." + zkClient.getZooKeeper().getState());
          }
          LOGGER.info("ZK Client connected");
        }
      }
    }
  }
}
