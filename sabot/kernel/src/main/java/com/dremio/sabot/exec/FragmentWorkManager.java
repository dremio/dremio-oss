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
package com.dremio.sabot.exec;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.curator.utils.CloseableExecutorService;

import com.codahale.metrics.Gauge;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.work.WorkStats;
import com.dremio.metrics.Metrics;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.rpc.CoordToExecHandlerImpl;
import com.dremio.sabot.exec.rpc.ExecProtocol;
import com.dremio.sabot.exec.rpc.ExecTunnel;
import com.dremio.sabot.rpc.CoordToExecHandler;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.sabot.task.TaskPool;
import com.dremio.sabot.task.TaskPoolFactory;
import com.dremio.sabot.task.TaskPools;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Service managing fragment execution.
 */
public class FragmentWorkManager implements Service {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentWorkManager.class);

  private final BootStrapContext context;
  private final Provider<NodeEndpoint> identity;
  private final Provider<SabotContext> dbContext;
  private final BindingCreator bindingCreator;
  private final Provider<FabricService> fabricServiceProvider;
  private final Provider<StoragePluginRegistry> storagePluginRegistry;

  private FragmentStatusThread statusThread;
  private ThreadsStatsCollector statsCollectorThread;

  private TaskPool pool;
  private FragmentExecutors fragmentExecutors;
  private SabotContext bitContext;
  private BufferAllocator allocator;
  private QueriesClerk clerk;
  private ExecutorService executor;
  private CloseableExecutorService closeableExecutor;

  private ExtendedLatch exitLatch = null; // This is used to wait to exit when things are still running


  public FragmentWorkManager(
      final BootStrapContext context,
      Provider<NodeEndpoint> identity,
      final Provider<SabotContext> dbContext,
      final Provider<FabricService> fabricServiceProvider,
      final Provider<StoragePluginRegistry> storagePluginRegistry,
      final BindingCreator bindingCreator
      ) {
    this.context = context;
    this.identity = identity;
    this.storagePluginRegistry = storagePluginRegistry;
    this.fabricServiceProvider = fabricServiceProvider;
    this.dbContext = dbContext;
    this.bindingCreator = bindingCreator;
  }

  public WorkStats getWorkStats() {
    return new WorkStatsImpl();
  }

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by {@link com.dremio.exec.server.SabotNode#close()}.</p>
   */
  public void waitToExit() {
    synchronized(this) {
      if (fragmentExecutors == null || fragmentExecutors.size() == 0) {
        return;
      }

      exitLatch = new ExtendedLatch();
    }

    // Wait for at most 5 seconds or until the latch is released.
    exitLatch.awaitUninterruptibly(5000);
  }

  private class WorkStatsImpl implements WorkStats {
    /**
     * @return number of running fragments / max width per node
     */
    @Override
    public float getClusterLoad() {
      final float maxWidthPerNode = bitContext.getOptionManager().getOption(ExecConstants.MAX_WIDTH_PER_NODE);
      return fragmentExecutors.size() / maxWidthPerNode;
    }

    @Override
    public double getMaxWidthFactor() {
      final OptionManager options = bitContext.getOptionManager();
      final double loadCutoff = options.getOption(ExecConstants.LOAD_CUT_OFF);
      final double loadReduction = options.getOption(ExecConstants.LOAD_REDUCTION);

      float clusterLoad = getClusterLoad();
      if (clusterLoad < loadCutoff) {
        return 1.0; // no reduction when load is below load.cut_off
      }

      return Math.max(0, 1.0 - clusterLoad * loadReduction);
    }

    private class FragmentInfoTransformer implements Function<FragmentExecutor, FragmentInfo>{

      @Override
      public FragmentInfo apply(final FragmentExecutor fragmentExecutor) {
        final FragmentStatus status = fragmentExecutor.getStatus();
        final ExecProtos.FragmentHandle handle = fragmentExecutor.getHandle();
        final MinorFragmentProfile profile = status == null ? null : status.getProfile();
        Long memoryUsed = profile == null ? 0 : profile.getMemoryUsed();
        Long rowsProcessed = profile == null ? 0 : getRowsProcessed(profile);
        Timestamp startTime = profile == null ? new Timestamp(0) : new Timestamp(profile.getStartTime());
        return new FragmentInfo(dbContext.get().getEndpoint().getAddress(),
          QueryIdHelper.getQueryId(handle.getQueryId()),
          handle.getMajorFragmentId(),
          handle.getMinorFragmentId(),
          memoryUsed,
          rowsProcessed,
          startTime,
          fragmentExecutor.getBlockingStatus(),
          fragmentExecutor.getTaskDescriptor());
      }

    }

    private long getRowsProcessed(MinorFragmentProfile profile) {
      long maxRecords = 0;
      for (OperatorProfile operatorProfile : profile.getOperatorProfileList()) {
        long records = 0;
        for (StreamProfile inputProfile :operatorProfile.getInputProfileList()) {
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
    synchronized(this) {
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

  @Override
  public void start() {

    bitContext = dbContext.get();

    final OptionManager options = bitContext.getOptionManager();

    final TaskPoolFactory factory = TaskPools.newFactory(context.getConfig());
    this.pool = factory.newInstance(options);

    this.executor = Executors.newCachedThreadPool();
    this.closeableExecutor = new CloseableExecutorService(executor);

    // start the internal rpc layer.
    this.allocator = context.getAllocator().newChildAllocator(
        "fragment-work-manager",
        context.getConfig().getLong("dremio.exec.rpc.bit.server.memory.data.reservation"),
        context.getConfig().getLong("dremio.exec.rpc.bit.server.memory.data.maximum"));

    this.clerk = new QueriesClerk(context.getAllocator(), context.getConfig());

    final ExecToCoordTunnelCreator creator = new ExecToCoordTunnelCreator(fabricServiceProvider.get().getProtocol(Protocols.COORD_TO_EXEC));

    final ExitCallback callback = new ExitCallback() {
      @Override
      public void indicateIfSafeToExit() {
        FragmentWorkManager.this.indicateIfSafeToExit();
      }
    };

    fragmentExecutors = new FragmentExecutors(creator, callback, pool, bitContext.getOptionManager());

    final ExecConnectionCreator connectionCreator = new ExecConnectionCreator(fabricServiceProvider.get().registerProtocol(new ExecProtocol(bitContext.getConfig(), allocator, fragmentExecutors)));

    final FragmentExecutorBuilder builder = new FragmentExecutorBuilder(
        clerk,
        bitContext.getConfig(),
        bitContext.getClusterCoordinator(),
        executor,
        bitContext.getOptionManager(),
        creator,
        connectionCreator,
        bitContext.getClasspathScan(),
        bitContext.getPlanReader(),
        bitContext.getNamespaceService(SystemUser.SYSTEM_USERNAME),
        storagePluginRegistry.get(),
        ClusterCoordinator.Role.fromEndpointRoles(identity.get().getRoles()));

    // register coord/exec message handling.
    bindingCreator.replace(CoordToExecHandler.class, new CoordToExecHandlerImpl(identity.get(), fragmentExecutors, builder));
    bindingCreator.bind(WorkStats.class, new WorkStatsImpl());

    statusThread = new FragmentStatusThread(fragmentExecutors, creator);
    statusThread.start();
    statsCollectorThread = new ThreadsStatsCollector();
    statsCollectorThread.start();

    final String prefix = "rpc";
    Metrics.registerGauge(prefix + "bit.data.current", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return allocator.getAllocatedMemory();
      }
    });
    Metrics.registerGauge(prefix + "bit.data.peak", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return allocator.getPeakMemoryAllocation();
      }
    });
  }

  public class ExecConnectionCreator {
    private final FabricRunnerFactory factory;

    public ExecConnectionCreator(FabricRunnerFactory factory) {
      super();
      this.factory = factory;
    }

    public ExecTunnel getTunnel(NodeEndpoint endpoint) {
      return new ExecTunnel(factory.getCommandRunner(endpoint.getAddress(), endpoint.getFabricPort()));
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(statusThread, statsCollectorThread, closeableExecutor, pool, fragmentExecutors, clerk, allocator);
  }

}
