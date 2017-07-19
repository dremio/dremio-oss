/*
 * Copyright Dremio Corporation 2015
 */
package com.dremio.dac.daemon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.AutoCloseables;
import com.dremio.common.CatastrophicFailure;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DacConfig;
import com.dremio.dac.server.NASSourceConfigurator;
import com.dremio.dac.server.SourceToStoragePluginConfig;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.service.exec.MasterStatusListener;
import com.dremio.exec.ExecConstants;
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

  private final Set<ClusterCoordinator.Role> roles;

  private final SingletonRegistry bootstrapRegistry;
  private final SingletonRegistry registry;
  private final String masterNode;
  private final String thisNode;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private final boolean isExecutor;

  private WebServer webServer;
  private final DacConfig dacConfig;


  private DACDaemon(
      DremioConfig incomingConfig,
      final ScanResult scanResult,
      DACModule dacModule,
      final SourceToStoragePluginConfig sourceConfigurator
      ) throws IOException {

    // ensure that the zookeeper option for sabot is same as dremio config.
    final DremioConfig config = incomingConfig
        .withSabotValue(ExecConstants.ZK_CONNECTION, incomingConfig.getString(DremioConfig.ZOOKEEPER_QUORUM))
        .withSabotValue(ExecConstants.INITIAL_USER_PORT, incomingConfig.getString(DremioConfig.CLIENT_PORT_INT))
        .withSabotValue(ExecConstants.SPILL_DIRS, incomingConfig.getList(DremioConfig.SPILLING_PATH_STRING))
        .withSabotValue(ExecConstants.REGISTRATION_ADDRESS, incomingConfig.getString(DremioConfig.REGISTRATION_ADDRESS));

    // This should be the first thing to do.
    setupHadoopUserUsingKerberosKeytab(config);

    this.roles = EnumSet.noneOf(ClusterCoordinator.Role.class);
    if (config.getBoolean(DremioConfig.ENABLE_COORDINATOR_BOOL)) {
      roles.add(ClusterCoordinator.Role.COORDINATOR);
    }
    if (config.getBoolean(DremioConfig.ENABLE_EXECUTOR_BOOL)) {
      roles.add(ClusterCoordinator.Role.EXECUTOR);
    }

    isCoordinator = roles.contains(ClusterCoordinator.Role.COORDINATOR);
    isExecutor = roles.contains(ClusterCoordinator.Role.EXECUTOR);

    this.dacConfig = new DacConfig(config);


    this.masterNode = dacConfig.getMasterNode();

    try (TimedBlock bh = Timer.time("getCanonicalHostName")) {
      this.thisNode = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ex) {
      throw new RuntimeException("Failure retrieving hostname from node. Check hosts file.", ex);
    }

    this.isMaster = !dacConfig.isRemote && NetworkUtil.addressResolvesToThisNode(masterNode);
    StringBuilder sb = new StringBuilder();
    if (isMaster) {
      sb.append("This node is the master node, ");
      sb.append(masterNode);
      sb.append(". ");
    } else {
      sb.append("This node is not master, waiting on ");
      sb.append(masterNode);
      sb.append(". ");
    }

    if (isMaster) {
      final String writePath = config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING);
      logger.info("Dremio daemon write path: " + config.getString(DremioConfig.LOCAL_WRITE_PATH_STRING));
      PathUtils.checkWritePath(writePath);
    }

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
    if (isMaster) {
      registry = new SingletonRegistry();
    } else {
      // retry if service start fails due to master is unavailable.
      registry = new NonMasterSingletonRegistry(bootstrapRegistry.provider(MasterStatusListener.class));
    }

    dacModule.bootstrap(bootstrapRegistry, scanResult, dacConfig, masterNode, isMaster);
    dacModule.build(bootstrapRegistry, registry, scanResult, dacConfig, masterNode, isMaster, sourceConfigurator);
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
      System.out.println("Dremio Daemon Started as " + (isMaster ?  "master" : "slave"));
      if(webServer != null){
        System.out.println(String.format("Webserver available at: %s://%s:%d",
            dacConfig.webSSLEnabled ? "https" : "http", thisNode, webServer.getPort()));
      }

    } catch (final Throwable ex) {
      CatastrophicFailure.exit(ex, "Failed to start services, daemon exiting.", -1);
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
   * Closes this daemon on JVM shutdown. By invoking this method, the caller is delegating the responsibility of
   * invoking {@link #close} to the JVM. Although this method is idempotent, the method should be called at most once.
   */
  public void closeOnJVMShutDown() {
    // Set shutdown hook after services are initialized
    Runtime.getRuntime()
        .addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              close();
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
              logger.error("Failed to close services during shutdown", e);
            }
          }
        });
  }

  @VisibleForTesting
  public DacConfig getDacConfig() {
    return dacConfig;
  }

  @VisibleForTesting
  public WebServer getWebServer() {
    return registry.lookup(WebServer.class);
  }

  @Override
  public void close() throws Exception {
    try (TimedBlock b = Timer.time("close")) {
      // closes all registered services in reverse order
      AutoCloseables.close(registry, bootstrapRegistry);
    }
  }

  public static DACDaemon newDremioDaemon(DacConfig dacConfig, ScanResult scanResult) throws IOException {
    return newDremioDaemon(dacConfig, scanResult, new DACDaemonModule(), new NASSourceConfigurator());
  }

  public static DACDaemon newDremioDaemon(
      DacConfig dacConfig,
      ScanResult scanResult,
      DACModule dacModule,
      SourceToStoragePluginConfig sourceConfig) throws IOException {
    try (TimedBlock b = Timer.time("newDaemon")) {
      return new DACDaemon(dacConfig.getConfig(), scanResult, dacModule, sourceConfig);
    }
  }

  public static void main(final String[] args) throws Exception {
    try (TimedBlock b = Timer.time("main")) {
      DacConfig config = DacConfig.newConfig();
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

