/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.zookeeper.Environment;

import com.dremio.common.StackTrace;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.exec.util.GuavaPatcher;
import com.dremio.exec.work.RunningQueryProvider;
import com.dremio.exec.work.WorkStats;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.sabot.exec.FragmentWorkManager;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.rpc.CoordExecService;
import com.dremio.sabot.rpc.CoordToExecHandler;
import com.dremio.sabot.rpc.ExecToCoordHandler;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.BindingCreator;
import com.dremio.service.BindingProvider;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.UserService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Test class to start execution framework without ui.
 */
public class SabotNode implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotNode.class);

  static {
    /*
     * HBase client uses older version of Guava's Stopwatch API,
     * while Dremio ships with 18.x which has changes the scope of
     * these API to 'package', this code make them accessible.
     */
    GuavaPatcher.patch();
    Environment.logEnv("SabotNode environment: ", logger);
  }

  private boolean isClosed = false;

  private final SingletonRegistry registry = new SingletonRegistry();

  private ShutdownThread shutdownHook;

  public SabotNode(
      final SabotConfig config,
      final ClusterCoordinator clusterCoordinator) throws Exception {
    this(config, clusterCoordinator, ClassPathScanner.fromPrescan(config), true);
  }

  @VisibleForTesting
  public SabotNode(
      final SabotConfig config,
      final ClusterCoordinator clusterCoordinator,
      final ScanResult classpathScan,
      boolean allRoles) throws Exception {
    init(registry, config, Preconditions.checkNotNull(clusterCoordinator), classpathScan, allRoles);
  }

  protected void init(
      SingletonRegistry registry,
      SabotConfig config,
      ClusterCoordinator clusterCoordinator,
      ScanResult classpathScan,
      boolean allRoles) throws Exception {
    final boolean allowPortHunting = true;
    final boolean useIP = false;
    DremioConfig dremioConfig = DremioConfig.create(null, config);
    dremioConfig = dremioConfig.withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, allRoles);

    // eagerly created.
    final BootStrapContext bootstrap = registry.bindSelf(new BootStrapContext(dremioConfig, classpathScan));

    // bind default providers.
    registry.bind(MaterializationDescriptorProvider.class, MaterializationDescriptorProvider.EMPTY);
    registry.bind(QueryObserverFactory.class, QueryObserverFactory.DEFAULT);
    // no authentication
    registry.bind(UserService.class, (UserService) null);
    registry.bind(NamespaceService.Factory.class, NamespaceServiceImpl.Factory.class);

    // cluster coordinator
    registry.bind(ClusterCoordinator.class, clusterCoordinator);

    // KVStore.
    registry.bind(KVStoreProvider.class, new LocalKVStoreProvider(bootstrap.getClasspathScan(), null, true, true));

    registry.bind(PersistentStoreProvider.class,
      new KVPersistentStoreProvider(registry.provider(KVStoreProvider.class)));

    // Fabric Service
    final String address = FabricServiceImpl.getAddress(useIP);
    registry.bind(FabricService.class, new FabricServiceImpl(
        address,
        45678,
        allowPortHunting,
        config.getInt(RpcConstants.BIT_RPC_TIMEOUT),
        config.getInt(ExecConstants.BIT_SERVER_RPC_THREADS),
        bootstrap.getExecutor(),
        bootstrap.getAllocator(),
        0,
        Long.MAX_VALUE));

    // RPC Endpoints.
    registry.bindSelf(new UserServer(bootstrap,
        registry.provider(SabotContext.class),
        registry.provider(UserWorker.class),
        null,
        allowPortHunting));

    registry.bindSelf(new CoordExecService(
        bootstrap.getConfig(),
        bootstrap.getAllocator(),
        registry.getBindingCreator(),
        registry.provider(FabricService.class),
        registry.provider(CoordToExecHandler.class),
        registry.provider(ExecToCoordHandler.class)
        ));

    registry.bind(NamespaceService.class, NamespaceServiceImpl.class);
    registry.bind(DatasetListingService.class, new DatasetListingServiceImpl(
        registry.provider(NamespaceService.Factory.class)));

    registry.bind(AccelerationManager.class, AccelerationManager.NO_OP);

    registry.bind(ContextService.class, new ContextService(
        registry.getBindingCreator(),
        bootstrap,
        registry.provider(ClusterCoordinator.class),
        registry.provider(PersistentStoreProvider.class),
        registry.provider(WorkStats.class),
        registry.provider(KVStoreProvider.class),
        registry.provider(FabricService.class),
        registry.provider(UserServer.class),
        registry.provider(MaterializationDescriptorProvider.class),
        registry.provider(QueryObserverFactory.class),
        registry.provider(RunningQueryProvider.class),
        registry.provider(AccelerationManager.class),
        registry.provider(AccelerationListManager.class),
        registry.provider(NamespaceService.Factory.class),
        registry.provider(DatasetListingService.class),
        registry.provider(UserService.class),
        registry.provider(CatalogService.class),
        null,
        allRoles
        ));

    // Note: corePoolSize param below should be more than 1 to show any multithreading issues
    registry.bind(SchedulerService.class, new LocalSchedulerService(2));

    registry.bindSelf(new SystemTablePluginConfigProvider());

    registry.bind(CatalogService.class, new CatalogServiceImpl(
        registry.provider(SabotContext.class),
        registry.provider(SchedulerService.class),
        registry.provider(SystemTablePluginConfigProvider.class),
        registry.provider(FabricService.class)
        ));

    registry.bind(ResourceAllocator.class,
      new BasicResourceAllocator(registry.provider(ClusterCoordinator.class)));

    registry.bindSelf(new ContextInformationFactory());
    registry.bindSelf(
        new FragmentWorkManager(bootstrap,
            dremioConfig,
            registry.provider(NodeEndpoint.class),
            registry.provider(SabotContext.class),
            registry.provider(FabricService.class),
            registry.provider(CatalogService.class),
            registry.provider(ContextInformationFactory.class),
            registry.getBindingCreator()));

    registry.bindSelf(
        new ForemenWorkManager(
            registry.provider(ClusterCoordinator.class),
            registry.provider(FabricService.class),
            registry.provider(SabotContext.class),
            registry.provider(ResourceAllocator.class),
            registry.getBindingCreator()
            )
        );
    registry.bind(NodeRegistration.class, new NodeRegistration(
        registry.provider(SabotContext.class),
        registry.provider(FragmentWorkManager.class),
        registry.provider(ForemenWorkManager.class),
        registry.provider(ClusterCoordinator.class)
        ));
  }

  public LocalQueryExecutor getLocalQueryExecutor(){
    return registry.lookup(LocalQueryExecutor.class);
  }

  public void run() throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Startup begun.");
    registry.start();
    shutdownHook = new ShutdownThread(this, new StackTrace());
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    logger.info("Startup completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public synchronized void close() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        logger.info("Cannot cancel shutdown hook, VM already exiting", e);
      }
    }
    doClose();
  }

  private void doClose() {
    // avoid complaints about double closing
    if (isClosed) {
      return;
    }
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Shutdown begun.");

    // wait for anything that is running to complete

    try {
      registry.close();
    } catch(Exception e) {
      logger.warn("Failure on close()", e);
    }

    logger.info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    isClosed = true;
  }


  /**
   * Shutdown hook for SabotNode. Closes the node, and reports on errors that
   * occur during closure, as well as the location the node was started from.
   */
  private static class ShutdownThread extends Thread {
    private final static AtomicInteger idCounter = new AtomicInteger(0);
    private final SabotNode node;
    private final StackTrace stackTrace;

    /**
     * Constructor.
     *
     * @param node the node to close down
     * @param stackTrace the stack trace from where the SabotNode was started;
     *   use new StackTrace() to generate this
     */
    public ShutdownThread(final SabotNode node, final StackTrace stackTrace) {
      this.node = node;
      this.stackTrace = stackTrace;
      /*
       * TODO should we try to determine a test class name?
       * See https://blogs.oracle.com/tor/entry/how_to_determine_the_junit
       */

      setName("SabotNode-ShutdownHook#" + idCounter.getAndIncrement());
    }

    @Override
    public void run() {
      logger.info("Received shutdown request.");
      try {
        /*
         * We can avoid metrics deregistration concurrency issues by only closing
         * one node at a time. To enforce that, we synchronize on a convenient
         * singleton object.
         */
        synchronized(idCounter) {
          node.doClose();
        }
      } catch(final Exception e) {
        throw new RuntimeException("Caught exception closing SabotNode started from\n" + stackTrace, e);
      }
    }
  }

  @VisibleForTesting
  public SabotContext getContext() {
    return registry.lookup(SabotContext.class);
  }

  @VisibleForTesting
  public BindingCreator getBindingCreator(){
    return registry.getBindingCreator();
  }

  @VisibleForTesting
  public BindingProvider getBindingProvider(){
    return registry.getBindingProvider();
  }

  public static void main(final String[] cli) throws NodeStartupException {
    final StartupOptions options = StartupOptions.parse(cli);
    start(options);
  }

  public static SabotNode start(final StartupOptions options) throws NodeStartupException {
    return start(SabotConfig.create(options.getConfigLocation()), null);
  }

  public static SabotNode start(final SabotConfig config) throws NodeStartupException {
    return start(config, null);
  }

  public static SabotNode start(final SabotConfig config, final ClusterCoordinator clusterCoordinator)
      throws NodeStartupException {
    return start(config, clusterCoordinator, ClassPathScanner.fromPrescan(config));
  }

  public static SabotNode start(final SabotConfig config, final ClusterCoordinator clusterCoordinator, ScanResult classpathScan)
      throws NodeStartupException {
    logger.debug("Starting new SabotNode.");
    SabotNode bit;
    try {
      bit = new SabotNode(config, clusterCoordinator, classpathScan, true);
    } catch (final Exception ex) {
      throw new NodeStartupException("Failure while initializing values in SabotNode.", ex);
    }

    try {
      bit.run();
    } catch (final Exception e) {
      bit.close();
      throw new NodeStartupException("Failure during initial startup of SabotNode.", e);
    }
    logger.debug("Started new SabotNode.");
    return bit;
  }

}
