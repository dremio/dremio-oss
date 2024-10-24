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
package com.dremio.sabot.exec;

import static com.dremio.telemetry.api.metrics.MeterProviders.newGauge;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.ExpressionSplitCache;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.service.executor.ExecutorServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.work.SafeExit;
import com.dremio.exec.work.WorkStats;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.heap.SpillingOperatorHeapController;
import com.dremio.sabot.exec.rpc.ExecProtocol;
import com.dremio.sabot.exec.rpc.ExecTunnel;
import com.dremio.sabot.exec.rpc.FabricExecTunnel;
import com.dremio.sabot.exec.rpc.InProcessExecTunnel;
import com.dremio.sabot.task.TaskPool;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.jobresults.client.JobResultsClientFactory;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClientFactory;
import com.dremio.service.maestroservice.MaestroClientFactory;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;

/** Service managing fragment execution. */
@Singleton
public class FragmentWorkManager implements Service, SafeExit {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FragmentWorkManager.class);

  private final BootStrapContext context;
  private final Provider<NodeEndpoint> identity;
  private final Provider<SabotContext> dbContext;
  private final Provider<FabricService> fabricServiceProvider;
  private final Provider<CatalogService> sources;
  private final Provider<ContextInformationFactory> contextInformationFactory;
  private final Provider<WorkloadTicketDepot> workloadTicketDepotProvider;
  private final WorkStats workStats;

  private FragmentStatusThread statusThread;
  private ThreadsStatsCollector statsCollectorThread;

  private final Provider<TaskPool> pool;
  private FragmentExecutors fragmentExecutors;
  private MaestroProxy maestroProxy;
  private SabotContext bitContext;
  private BufferAllocator allocator;
  private WorkloadTicketDepot ticketDepot;
  private QueriesClerk clerk;
  private CloseableExecutorService executor;
  private final Provider<MaestroClientFactory> maestroServiceClientFactoryProvider;
  private final Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider;
  private final Provider<JobResultsClientFactory> jobResultsClientFactoryProvider;

  private ExtendedLatch exitLatch =
      null; // This is used to wait to exit when things are still running
  private com.dremio.exec.service.executor.ExecutorService executorService;
  private HeapMonitorManager heapMonitorManager = null;
  private SpillingOperatorHeapController heapLowMemController = null;

  private SabotConfig sabotConfig;

  @Inject
  public FragmentWorkManager(
      final BootStrapContext context,
      final SabotConfig sabotConfig,
      Provider<NodeEndpoint> identity,
      final Provider<SabotContext> dbContext,
      final Provider<FabricService> fabricServiceProvider,
      final Provider<CatalogService> sources,
      final Provider<ContextInformationFactory> contextInformationFactory,
      final Provider<WorkloadTicketDepot> workloadTicketDepotProvider,
      final Provider<TaskPool> taskPool,
      final Provider<MaestroClientFactory> maestroServiceClientFactoryProvider,
      final Provider<JobTelemetryExecutorClientFactory> jobTelemetryClientFactoryProvider,
      final Provider<JobResultsClientFactory> jobResultsClientFactoryProvider) {
    this.context = context;
    this.identity = identity;
    this.sources = sources;
    this.fabricServiceProvider = fabricServiceProvider;
    this.dbContext = dbContext;
    this.contextInformationFactory = contextInformationFactory;
    this.workloadTicketDepotProvider = workloadTicketDepotProvider;
    this.pool = taskPool;
    this.workStats = new WorkStatsImpl();
    this.executorService = new ExecutorServiceImpl.NoExecutorService();
    this.maestroServiceClientFactoryProvider = maestroServiceClientFactoryProvider;
    this.jobTelemetryClientFactoryProvider = jobTelemetryClientFactoryProvider;
    this.jobResultsClientFactoryProvider = jobResultsClientFactoryProvider;
    this.sabotConfig = sabotConfig;
  }

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by {@link SabotNode#close()}.
   */
  @Override
  public void waitToExit() {
    synchronized (this) {
      if (fragmentExecutors == null || fragmentExecutors.size() == 0) {
        return;
      }

      exitLatch = new ExtendedLatch();
    }

    // Wait for at most the configured graceful timeout or until the latch is released.
    exitLatch.awaitUninterruptibly(
        bitContext.getDremioConfig().getLong(DremioConfig.DREMIO_TERMINATION_GRACE_PERIOD_SECONDS)
            * 1000);
  }

  /**
   * Returns the WorkStats instance to use.
   *
   * @return the WorkStats instance
   */
  public WorkStats getWorkStats() {
    return workStats;
  }

  private class WorkStatsImpl implements WorkStats {

    @Override
    public Iterable<TaskPool.ThreadInfo> getSlicingThreads() {
      return pool.get().getSlicingThreads();
    }

    /**
     * @return number of running fragments / max width per node
     */
    private float getClusterLoadImpl(GroupResourceInformation groupResourceInformation) {
      final long maxWidthPerNode =
          groupResourceInformation.getAverageExecutorCores(bitContext.getOptionManager());
      Preconditions.checkState(
          maxWidthPerNode > 0, "No executors are available. Unable to determine cluster load");
      return fragmentExecutors.size() / (maxWidthPerNode * 1.0f);
    }

    @Override
    public float getClusterLoad() {
      return getClusterLoadImpl(bitContext.getClusterResourceInformation());
    }

    @Override
    public float getClusterLoad(GroupResourceInformation groupResourceInformation) {
      return getClusterLoadImpl(groupResourceInformation);
    }

    private double getMaxWidthFactorImpl(GroupResourceInformation groupResourceInformation) {
      final OptionManager options = bitContext.getOptionManager();
      final double loadCutoff = options.getOption(ExecConstants.LOAD_CUT_OFF);
      final double loadReduction = options.getOption(ExecConstants.LOAD_REDUCTION);

      float clusterLoad = getClusterLoad(groupResourceInformation);
      if (clusterLoad < loadCutoff) {
        return 1.0; // no reduction when load is below load.cut_off
      }

      return Math.max(0, 1.0 - clusterLoad * loadReduction);
    }

    @Override
    public double getMaxWidthFactor() {
      return getMaxWidthFactorImpl(bitContext.getClusterResourceInformation());
    }

    @Override
    public double getMaxWidthFactor(GroupResourceInformation groupResourceInformation) {
      return getMaxWidthFactorImpl(groupResourceInformation);
    }

    private class FragmentInfoTransformer implements Function<FragmentExecutor, FragmentInfo> {

      @Override
      public FragmentInfo apply(final FragmentExecutor fragmentExecutor) {
        final FragmentStatus status = fragmentExecutor.getStatus();
        final ExecProtos.FragmentHandle handle = fragmentExecutor.getHandle();
        final MinorFragmentProfile profile = status == null ? null : status.getProfile();
        Long memoryUsed = profile == null ? 0 : profile.getMemoryUsed();
        Long rowsProcessed = profile == null ? 0 : getRowsProcessed(profile);
        Timestamp startTime =
            profile == null ? new Timestamp(0) : new Timestamp(profile.getStartTime());
        return new FragmentInfo(
            dbContext.get().getEndpoint().getAddress(),
            QueryIdHelper.getQueryId(handle.getQueryId()),
            handle.getMajorFragmentId(),
            handle.getMinorFragmentId(),
            memoryUsed,
            rowsProcessed,
            startTime,
            fragmentExecutor.getBlockingStatus(),
            fragmentExecutor.getTaskDescriptor(),
            dbContext.get().getEndpoint().getFabricPort(),
            fragmentExecutor.getMemoryGrant());
      }
    }

    private long getRowsProcessed(MinorFragmentProfile profile) {
      long maxRecords = 0;
      for (OperatorProfile operatorProfile : profile.getOperatorProfileList()) {
        long records = 0;
        for (StreamProfile inputProfile : operatorProfile.getInputProfileList()) {
          if (inputProfile.hasRecords()) {
            records += inputProfile.getRecords();
          }
        }
        maxRecords = Math.max(maxRecords, records);
      }
      return maxRecords;
    }

    @Override
    public Iterator<FragmentInfo> getRunningFragments() {
      return Iterators.transform(fragmentExecutors.iterator(), new FragmentInfoTransformer());
    }

    @Override
    public Integer getCpuTrailingAverage(long id, int seconds) {
      return statsCollectorThread.getCpuTrailingAverage(id, seconds);
    }

    @Override
    public Integer getUserTrailingAverage(long id, int seconds) {
      return statsCollectorThread.getUserTrailingAverage(id, seconds);
    }
  }

  /**
   * If it is safe to exit, and the exitLatch is in use, signals it so that waitToExit() will
   * unblock.
   */
  private void indicateIfSafeToExit() {
    synchronized (this) {
      if (exitLatch != null) {
        if (fragmentExecutors.size() == 0) {
          exitLatch.countDown();
        }
      }
    }
  }

  public interface ExitCallback {
    void indicateIfSafeToExit();
  }

  public com.dremio.exec.service.executor.ExecutorService getExecutorService() {
    return executorService;
  }

  @Override
  public void start() {
    bitContext = dbContext.get();

    this.executor = new CloseableThreadPool("fragment-work-executor-");

    // start the internal rpc layer.
    this.allocator =
        context
            .getAllocator()
            .newChildAllocator(
                "fragment-work-manager",
                context.getConfig().getLong("dremio.exec.rpc.bit.server.memory.data.reservation"),
                context.getConfig().getLong("dremio.exec.rpc.bit.server.memory.data.maximum"));

    this.ticketDepot = workloadTicketDepotProvider.get();
    this.clerk = new QueriesClerk(ticketDepot);

    final ExitCallback callback =
        new ExitCallback() {
          @Override
          public void indicateIfSafeToExit() {
            FragmentWorkManager.this.indicateIfSafeToExit();
          }
        };

    maestroProxy =
        new MaestroProxy(
            maestroServiceClientFactoryProvider,
            jobTelemetryClientFactoryProvider,
            bitContext.getClusterCoordinator(),
            identity,
            bitContext.getOptionManager());

    fragmentExecutors =
        new FragmentExecutors(
            context,
            bitContext.getNodeDebugContext(),
            sabotConfig,
            clerk,
            maestroProxy,
            callback,
            pool.get(),
            bitContext.getOptionManager());

    final ExecConnectionCreator connectionCreator =
        new ExecConnectionCreator(
            fabricServiceProvider
                .get()
                .registerProtocol(
                    new ExecProtocol(bitContext.getConfig(), allocator, fragmentExecutors)),
            bitContext.getOptionManager());

    if (bitContext.isExecutor()) {
      heapLowMemController = SpillingOperatorHeapController.create();
    }

    final FragmentExecutorBuilder builder =
        getFragmentExecutorBuilder(
            clerk,
            fragmentExecutors,
            bitContext.getEndpoint(),
            maestroProxy,
            bitContext.getConfig(),
            bitContext.getDremioConfig(),
            bitContext.getClusterCoordinator(),
            executor,
            bitContext.getOptionManager(),
            connectionCreator,
            new OperatorCreatorRegistry(bitContext.getClasspathScan()),
            bitContext.getPlanReader(),
            bitContext.getNamespaceService(SystemUser.SYSTEM_USERNAME),
            sources.get(),
            contextInformationFactory.get(),
            bitContext.getFunctionImplementationRegistry(),
            bitContext.getDecimalFunctionImplementationRegistry(),
            bitContext.getNodeDebugContext(),
            bitContext.getSpillService(),
            bitContext.getCompiler(),
            ClusterCoordinator.Role.fromEndpointRoles(identity.get().getRoles()),
            jobResultsClientFactoryProvider,
            identity,
            bitContext.getExpressionSplitCache(),
            heapLowMemController);

    executorService = new ExecutorServiceImpl(fragmentExecutors, bitContext, builder);

    statusThread =
        new FragmentStatusThread(
            fragmentExecutors, clerk, maestroProxy, bitContext.getOptionManager());
    statusThread.start();
    Iterable<TaskPool.ThreadInfo> slicingThreads = pool.get().getSlicingThreads();
    Set<Long> slicingThreadIds = Sets.newHashSet();
    for (TaskPool.ThreadInfo slicingThread : slicingThreads) {
      slicingThreadIds.add(slicingThread.threadId);
    }
    statsCollectorThread = new ThreadsStatsCollector(slicingThreadIds);
    statsCollectorThread.start();

    if (bitContext.isExecutor()) {
      FailGreediestQueriesStrategy heapClawBackStrategy =
          new FailGreediestQueriesStrategy(fragmentExecutors, clerk, 25);
      logger.info("Starting heap monitor manager in executor");
      heapMonitorManager =
          new HeapMonitorManager(
              () -> bitContext.getOptionManager(),
              heapClawBackStrategy,
              heapClawBackStrategy,
              ClusterCoordinator.Role.EXECUTOR);
      heapMonitorManager.addLowMemListener(heapLowMemController.getLowMemListener());
      heapMonitorManager.addLowMemListener(heapClawBackStrategy);
      heapMonitorManager.start();
    }

    newGauge("rpc.bit.data_current", allocator::getAllocatedMemory);
    newGauge("rpc.bit.data_peak", allocator::getPeakMemoryAllocation);
  }

  protected FragmentExecutorBuilder getFragmentExecutorBuilder(
      QueriesClerk clerk,
      FragmentExecutors fragmentExecutors,
      NodeEndpoint endpoint,
      MaestroProxy maestroProxy,
      SabotConfig config,
      DremioConfig dremioConfig,
      ClusterCoordinator clusterCoordinator,
      CloseableExecutorService executor,
      OptionManager optionManager,
      ExecConnectionCreator connectionCreator,
      OperatorCreatorRegistry operatorCreatorRegistry,
      PhysicalPlanReader planReader,
      NamespaceService namespaceService,
      CatalogService catalogService,
      ContextInformationFactory contextInformationFactory,
      FunctionImplementationRegistry functionImplementationRegistry,
      FunctionImplementationRegistry decimalFunctionImplementationRegistry,
      NodeDebugContextProvider nodeDebugContext,
      SpillService spillService,
      CodeCompiler compiler,
      Set<Role> roles,
      Provider<JobResultsClientFactory> jobResultsClientFactoryProvider,
      Provider<NodeEndpoint> identity,
      ExpressionSplitCache expressionSplitCache,
      SpillingOperatorHeapController heapLowMemController) {
    return new FragmentExecutorBuilder(
        clerk,
        fragmentExecutors,
        endpoint,
        maestroProxy,
        config,
        dremioConfig,
        clusterCoordinator,
        executor,
        optionManager,
        connectionCreator,
        operatorCreatorRegistry,
        planReader,
        namespaceService,
        catalogService,
        contextInformationFactory,
        functionImplementationRegistry,
        decimalFunctionImplementationRegistry,
        nodeDebugContext,
        spillService,
        compiler,
        roles,
        jobResultsClientFactoryProvider,
        identity,
        expressionSplitCache,
        heapLowMemController);
  }

  public class ExecConnectionCreator {
    private final FabricRunnerFactory factory;
    private final OptionManager options;

    public ExecConnectionCreator(FabricRunnerFactory factory, OptionManager options) {
      super();
      this.factory = factory;
      this.options = options;
    }

    public ExecTunnel getTunnel(NodeEndpoint endpoint) {
      return this.options.getOption(ExecConstants.ENABLE_IN_PROCESS_TUNNEL)
              && isInProcessTarget(endpoint)
          ? new InProcessExecTunnel(fragmentExecutors, allocator)
          : new FabricExecTunnel(
              factory.getCommandRunner(endpoint.getAddress(), endpoint.getFabricPort()));
    }

    public boolean isInProcessTarget(NodeEndpoint endpoint) {
      NodeEndpoint self = identity.get();
      return endpoint.getAddress().equals(self.getAddress())
          && endpoint.getFabricPort() == self.getFabricPort();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(
        statusThread,
        statsCollectorThread,
        heapMonitorManager,
        executor,
        fragmentExecutors,
        maestroProxy,
        allocator,
        heapLowMemController);
  }
}
