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
package com.dremio.service.coordinator.zk;

import static com.dremio.service.coordinator.ClusterCoordinator.Options.CLUSTER_ID;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_CONNECTION;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_RETRY_BASE_DELAY;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_RETRY_MAX_DELAY;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ROOT;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_TIMEOUT;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Provider;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.DistributedSemaphore;

class ZKClusterClient implements com.dremio.service.Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterClient.class);
  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^[^/]*?)/(?:(.*)/)?([^/]*)$");

  private final String clusterId;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private CuratorFramework curator;
  private ServiceDiscovery<NodeEndpoint> discovery;
  private SabotConfig config;
  private final String connect;
  private final Provider<Integer> localPortProvider;
  private volatile boolean closed = false;


  public ZKClusterClient(SabotConfig config, String connect) throws IOException {
    this(config, connect, null);
  }

  public ZKClusterClient(SabotConfig config, Provider<Integer> localPort) throws IOException {
    this(config, null, localPort);
  }

  private ZKClusterClient(SabotConfig config, String connect, Provider<Integer> localPort) throws IOException {
    this.localPortProvider = localPort;
    this.connect = connect;
    this.config = config;
    String clusterId = config.getString(CLUSTER_ID);
    if(connect != null){
      final Matcher m = ZK_COMPLEX_STRING.matcher(connect);
      if(m.matches()) {
        clusterId = m.group(3);
      }
    }
    this.clusterId = clusterId;
  }

  @Override
  public void start() throws Exception {
    String connect;
    if(localPortProvider != null){
      connect = "localhost:" + localPortProvider.get();
    } else if(this.connect == null || this.connect.isEmpty()){
      connect = config.getString(ZK_CONNECTION);
    } else {
      connect = this.connect;
    }

    String zkRoot = config.getString(ZK_ROOT);


    // check if this is a complex zk string.  If so, parse into components.
    if(connect != null){
      Matcher m = ZK_COMPLEX_STRING.matcher(connect);
      if(m.matches()) {
        connect = m.group(1);
        zkRoot = m.group(2);
      }
    }

    logger.debug("Connect: {}, zkRoot: {}, clusterId: {}", connect, zkRoot, clusterId);

    RetryPolicy rp = new BoundedExponentialDelayWithUnlimitedRetry(
      config.getMilliseconds(ZK_RETRY_BASE_DELAY).intValue(),
      config.getMilliseconds(ZK_RETRY_MAX_DELAY).intValue());
    curator = CuratorFrameworkFactory.builder()
      .namespace(zkRoot)
      .connectionTimeoutMs(config.getInt(ZK_TIMEOUT))
      .maxCloseWaitMs(config.getMilliseconds(ZK_RETRY_MAX_DELAY).intValue())
      .retryPolicy(rp)
      .connectString(connect)
      .build();
    curator.getConnectionStateListenable().addListener(new InitialConnectionListener());
    curator.getConnectionStateListenable().addListener(new ConnectionLogger());
    curator.start();
    discovery = newDiscovery(clusterId);

    logger.info("Starting ZKClusterClient");
    discovery.start();

    this.initialConnection.await();
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      logger.info("Stopping ZKClusterClient");
      initialConnection.countDown();
      // discovery attempts to close its caches(ie serviceCache) already. however, being good citizens we make sure to
      // explicitly close serviceCache. Not only that we make sure to close serviceCache before discovery to prevent
      // double releasing and disallowing jvm to spit bothering warnings. simply put, we are great!
      AutoCloseables.close(discovery, curator);
      logger.info("Stopped ZKClusterClient");
    }
  }

  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return new ZkDistributedSemaphore(curator, "/semaphore/" + name, maximumLeases);
  }

  public ZKServiceSet newServiceSet(String name) throws Exception {
    return new ZKServiceSet(name, discovery);
  }

  private ServiceDiscovery<NodeEndpoint> newDiscovery(String clusterId) {
    return ServiceDiscoveryBuilder
      .builder(NodeEndpoint.class)
      .basePath(clusterId)
      .client(curator)
      .serializer(ServiceInstanceHelper.SERIALIZER)
      .build();
  }

  private class InitialConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      if(newState == ConnectionState.CONNECTED) {
        ZKClusterClient.this.initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }
  }

  private class ConnectionLogger implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      logger.info("ZK connection state changed to {}", newState);
    }
  }
}
