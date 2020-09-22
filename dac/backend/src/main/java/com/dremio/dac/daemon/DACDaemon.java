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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.net.ssl.HttpsURLConnection;

import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.AutoCloseables;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.admin.LocalAdmin;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.DremioServer;
import com.dremio.dac.server.LivenessService;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.service.BindingCreator;
import com.dremio.service.BindingProvider;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * The single container for all of the services in Dremio.
 *
 * This daemon currently this manages the Dremio Web UI,
 * launching an embedded Zookeeper instance and launching
 * a SabotNode.
 */
public final class DACDaemon implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DACDaemon.class);

  /**
   * Specify the kind of Dremio cluster
   *
   */
  public enum ClusterMode {
    LOCAL, DISTRIBUTED
  }

  private final Runnable shutdownHook = new Runnable() {
    @Override
    public void run() {
      try {
        close();
      } catch (InterruptedException ignored) {
      } catch (Exception e) {
        logger.error("Failed to close services during shutdown", e);
      }
    }
  };

  private final Set<ClusterCoordinator.Role> roles;

  private final SingletonRegistry bootstrapRegistry;
  private final SingletonRegistry registry;
  private final String thisNode;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private final boolean isExecutor;
  private final boolean isMasterless;

  private final CountDownLatch closed = new CountDownLatch(1);

  private WebServer webServer;
  private final DACConfig dacConfig;


  private DACDaemon(
      DremioConfig incomingConfig,
      final ScanResult scanResult,
      DACModule dacModule,
      DremioServer server
      ) throws IOException {

    // ensure that the zookeeper option for sabot is same as dremio config.
    final DremioConfig config = incomingConfig
        .withSabotValue(ExecConstants.ZK_CONNECTION, incomingConfig.getString(DremioConfig.ZOOKEEPER_QUORUM))
        .withSabotValue(DremioClient.INITIAL_USER_PORT, incomingConfig.getString(DremioConfig.CLIENT_PORT_INT))
        .withSabotValue(ExecConstants.SPILL_DIRS, incomingConfig.getList(DremioConfig.SPILLING_PATH_STRING))
        .withSabotValue(ExecConstants.REGISTRATION_ADDRESS, incomingConfig.getString(DremioConfig.REGISTRATION_ADDRESS))
        .withSabotValue(ExecConstants.ZK_SESSION_TIMEOUT, incomingConfig.getString(DremioConfig.ZK_CLIENT_SESSION_TIMEOUT))
        .withSabotValue(ExecConstants.ZK_RETRY_UNLIMITED, incomingConfig.getString(DremioConfig.ZK_CLIENT_RETRY_UNLIMITED))
        .withSabotValue(ExecConstants.ZK_RETRY_LIMIT, incomingConfig.getString(DremioConfig.ZK_CLIENT_RETRY_LIMIT))
        .withSabotValue(ExecConstants.ZK_INITIAL_TIMEOUT_MS, incomingConfig.getString(DremioConfig.ZK_CLIENT_INITIAL_TIMEOUT_MS));

    // This should be the first thing to do.
    setupHadoopUserUsingKerberosKeytab(config);

    // setup default SSL socket factory
    setupDefaultHttpsSSLSocketFactory();

    this.dacConfig = new DACConfig(config);
    this.isMasterless = config.isMasterlessEnabled();
    // master and masterless are mutually exclusive
    this.isMaster = (dacConfig.isMaster && !isMasterless);
    this.isCoordinator = config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL);
    this.isExecutor = config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL);
    this.thisNode = dacConfig.thisNode;

    this.roles = EnumSet.noneOf(ClusterCoordinator.Role.class);
    if (isMaster) {
      roles.add(ClusterCoordinator.Role.MASTER);
    }
    if (isCoordinator) {
      roles.add(ClusterCoordinator.Role.COORDINATOR);
    }
    if (isExecutor) {
      roles.add(ClusterCoordinator.Role.EXECUTOR);
    }

    StringBuilder sb = new StringBuilder();
    if (!isMasterless) {
      if (isMaster) {
        sb.append("This node is the master node, ");
        sb.append(dacConfig.thisNode);
        sb.append(". ");
      } else {
        sb.append("This node is not master, waiting on master to register in ZooKeeper");
        sb.append(". ");
      }
    }

    // we should not check it only on master - either check everywhere
    // or nowhere
    final String writePath = config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING);
    logger.info("Dremio daemon write path: " + config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING));
    PathUtils.checkWritePath(writePath);

    sb.append("This node acts as ");
    if (isCoordinator && isExecutor) {
      sb.append("both a coordinator and an executor.");
    } else if (isCoordinator) {
      sb.append("a coordinator.");
    } else if (isExecutor) {
      sb.append("an executor.");
      Preconditions.checkArgument(!isMaster, "A master node must also operate as a coordinator.");
    } else {
      throw new IllegalStateException();
    }

    logger.info(sb.toString());

    this.bootstrapRegistry = new SingletonRegistry();
    if (isMaster || isMasterless) {
      registry = new SingletonRegistry();
    } else {
      // retry if service start fails due to master is unavailable.
      registry = new NonMasterSingletonRegistry(bootstrapRegistry.provider(MasterStatusListener.class));
    }

    if (server == null) {
      registry.bind(DremioServer.class, new DremioServer());
    } else {
      registry.bind(DremioServer.class, server);
    }
    dacModule.bootstrap(shutdownHook, bootstrapRegistry, scanResult, dacConfig, isMaster);
    dacModule.build(bootstrapRegistry, registry, scanResult, dacConfig, isMaster);
  }

  @VisibleForTesting
  public void startPreServices() throws Exception {
    bootstrapRegistry.start();
  }

  @VisibleForTesting
  public void startServices() throws Exception {
    registry.start();
  }

  public final void init() throws Exception {
    try (TimedBlock b = Timer.time("init")) {
      startPreServices();
      startServices();
      final String text;
      if (isMaster) {
        text = "master";
      } else if (isCoordinator) {
        text = "coordinator";
      } else {
        text = "worker";
      }
      System.out.println("Dremio Daemon Started as " + text);
      LocalAdmin.getInstance().setDaemon(this);
      if(webServer != null){
        System.out.println(String.format("Webserver available at: %s://%s:%d",
            dacConfig.webSSLEnabled() ? "https" : "http", thisNode, webServer.getPort()));
      }

    }
  }

  /**
   * Set up the current user in {@link UserGroupInformation} using the kerberos principal and keytab file path if
   * present in config. If not present, this method call is a no-op. When communicating with the kerberos enabled
   * Hadoop based filesystem credentials in {@link UserGroupInformation} will be used..
   * @param config
   * @throws IOException
   */
  private void setupHadoopUserUsingKerberosKeytab(final DremioConfig config) throws IOException {
    final String kerberosPrincipal = config.getString(DremioConfig.KERBEROS_PRINCIPAL);
    final String kerberosKeytab = config.getString(DremioConfig.KERBEROS_KEYTAB_PATH);

    if (Strings.isNullOrEmpty(kerberosPrincipal) || Strings.isNullOrEmpty(kerberosKeytab)) {
      return;
    }

    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);

    logger.info("Setup Hadoop user info using kerberos principal {} and keytab file {} successful.",
        kerberosPrincipal, kerberosKeytab);
  }

  /**
   * Trigger method to create the default HTTPS SSL socket factory. Currently we have an issue due to
   * {@link HttpsURLConnection#defaultSSLSocketFactory} being static but not volatile which means we could end up
   * initializing the static variable multiple times in multi-threaded scenario and it could cause issues such as
   * <a href="DX-11543">https://dremio.atlassian.net/browse/DX-11543</a>
   */
  private void setupDefaultHttpsSSLSocketFactory() {
    HttpsURLConnection.getDefaultSSLSocketFactory();
  }

  /**
   * Closes this daemon on JVM shutdown. By invoking this method, the caller is delegating the responsibility of
   * invoking {@link #close} to the JVM. Although this method is idempotent, the method should be called at most once.
   */
  public void closeOnJVMShutDown() {
    // Set shutdown hook after services are initialized
    Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook, "shutdown-thread"));
  }

  @VisibleForTesting
  public DACConfig getDACConfig() {
    return dacConfig;
  }

  @VisibleForTesting
  public WebServer getWebServer() {
    return registry.lookup(WebServer.class);
  }

  public ServerHealthMonitor getServerHealthMonitor() {
    return registry.lookup(ServerHealthMonitor.class);
  }

  public LivenessService getLivenessService() {
    return registry.provider(LivenessService.class).get();
  }

  public void awaitClose() throws InterruptedException {
    closed.await();
  }

  /**
   * Notify that daemon is being shut down.
   * Code waiting on awaitClose() will be released
   */
  public void shutdown() {
    closed.countDown();
  }

  @Override
  public void close() throws Exception {
    try (TimedBlock b = Timer.time("close")) {
      // closes all registered services in reverse order
      // (assumed safe to call close on registry and bootstrapRegistry several times)
      AutoCloseables.close(registry, bootstrapRegistry);
    } finally {
      // Notify that daemon has been closed
      closed.countDown();
    }
  }

  public static DACDaemon newDremioDaemon(DACConfig dacConfig, ScanResult scanResult) throws IOException {
    return newDremioDaemon(dacConfig, scanResult, new DACDaemonModule());
  }

  public static DACDaemon newDremioDaemon(
      DACConfig dacConfig,
      ScanResult scanResult,
      DACModule dacModule,
      DremioServer server) throws IOException {
    try (TimedBlock b = Timer.time("newDaemon")) {
      return new DACDaemon(dacConfig.getConfig(), scanResult, dacModule, server);
    }
  }

  public static DACDaemon newDremioDaemon(
    DACConfig dacConfig,
    ScanResult scanResult,
    DACModule dacModule) throws IOException {
    return newDremioDaemon(dacConfig, scanResult, dacModule, null);
  }

  public static void main(final String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      DACConfig config = DACConfig.newConfig();
      DACDaemon daemon = newDremioDaemon(config, ClassPathScanner.fromPrescan(config.getConfig().getSabotConfig()));
      daemon.init();
      daemon.closeOnJVMShutDown();
    }
  }

  public BindingProvider getBindingProvider(){
    return registry.getBindingProvider();
  }

  public BindingCreator getBindingCreator() {
    return registry.getBindingCreator();
  }

}

