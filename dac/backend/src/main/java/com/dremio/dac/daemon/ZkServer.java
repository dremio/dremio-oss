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

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.service.Service;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.commons.io.file.PathUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/** ZooKeeper server service. */
public class ZkServer implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkServer.class);
  private static final int ZK_SERVER_STARTUP_TIME = 1000;

  private final Path storageDir;
  private final boolean autoPort;

  private int port;
  private Thread zkThread;
  private ZkEmbeddedServer zkEmbeddedServer;

  public ZkServer(String dirPath, int port, final boolean autoPort) {
    if (dirPath != null) {
      // TODO - add basic sanity check that the input value looks like a filesytem path
      storageDir = Paths.get(dirPath);
    } else {
      try {
        storageDir = Files.createTempDirectory("zkServer");
        logger.info("Created temporary storage dir: {}", storageDir.toAbsolutePath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    this.autoPort = autoPort;
    this.port = port;
    // start embedded zookeeper in here in order to initialize port.
  }

  public int getPort() {
    return port;
  }

  @Override
  public void start() throws Exception {
    try (TimedBlock ignored = Timer.time("new ZkServer")) {
      if (!deleteQuietly(storageDir)) {
        logger.warn("Couldn't delete Zookeeper data directory: {}", storageDir);
      }
      init();
    }
  }

  public void init() throws Exception {
    logger.info("Starting Zookeeper");

    final Properties startupProperties = new Properties();
    final Path dir = storageDir.resolve("zookeeper");
    startupProperties.put("dataDir", dir.toAbsolutePath().toString());
    final ServerConfig configuration = new ServerConfig();
    final QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();

    while (true) {
      startupProperties.put("clientPort", port);
      quorumConfiguration.parseProperties(startupProperties);
      configuration.readFrom(quorumConfiguration);
      this.zkEmbeddedServer = new ZkEmbeddedServer(configuration);
      this.zkThread = new Thread(zkEmbeddedServer, "dremio-embedded-zookeeper@" + port);
      zkThread.start();
      // wait for zk server to start cleanly, max wait half a sec
      int maxWait = ZK_SERVER_STARTUP_TIME;
      while (zkEmbeddedServer.error == null && maxWait > 0) {
        try {
          Thread.currentThread().sleep(100);
        } catch (InterruptedException ite) {
          break;
        }
        maxWait -= 100;
      }
      if (zkEmbeddedServer.error != null) {
        zkEmbeddedServer.shutDown();
        zkThread.join();
        if (autoPort && zkEmbeddedServer.error instanceof BindException) {
          logger.info("ZooKeeper failed to start on port {}, trying next port {}", port, port + 1);
          port++;
        } else {
          logger.error("ZooKeeper startup failed", zkEmbeddedServer.error);
          Throwables.propagate(zkEmbeddedServer.error);
          break;
        }
      } else {
        break;
      }
    }

    logger.info("Zookeeper is up at localhost:{}", port);
  }

  @Override
  public void close() throws InterruptedException {
    logger.info("Stopping Zookeeper at localhost:{}", port);
    zkEmbeddedServer.shutDown();
    zkThread.join();
    logger.info("Stopped Zookeeper at localhost:{}", port);
  }

  static final class ZkEmbeddedServer extends ZooKeeperServerMain implements Runnable {
    private final ServerConfig configuration;
    private volatile Throwable error = null;

    public ZkEmbeddedServer(ServerConfig configuration) {
      this.configuration = configuration;
    }

    @Override
    public void run() {
      try {
        runFromConfig(configuration);
      } catch (Throwable t) {
        error = t;
      }
    }

    public Throwable getError() {
      return error;
    }

    public void shutDown() {
      super.shutdown();
    }
  }

  private static boolean deleteQuietly(Path path) {
    if (!Files.exists(path)) {
      return true;
    }
    try {
      PathUtils.delete(path);
    } catch (IOException e) {
      logger.debug("Failed to delete path: {}", path, e);
      return false;
    }
    return true;
  }
}
