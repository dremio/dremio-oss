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
package com.dremio.sabot.exec.fragment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.record.NamespaceUpdater;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.server.options.FragmentOptionManager;
import com.dremio.exec.server.options.OptionList;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.driver.SchemaChangeListener;
import com.dremio.sabot.exec.EventProvider;
import com.dremio.sabot.exec.ExecToCoordTunnelCreator;
import com.dremio.sabot.exec.FragmentWorkManager.ExecConnectionCreator;
import com.dremio.sabot.exec.QueriesClerk;
import com.dremio.sabot.exec.QueriesClerk.FragmentTicket;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.context.StatusHandler;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Preconditions;

/**
 * Singleton utility to help in constructing a FragmentExecutor.
 */
public class FragmentExecutorBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutorBuilder.class);

  static final String PIPELINE_RES_GRP = "pipeline";
  static final String WORK_QUEUE_RES_GRP = "work-queue";

  private final QueriesClerk clerk;
  private final SabotConfig config;
  private final ClusterCoordinator coord;
  private final ExecutorService executorService;
  private final SystemOptionManager optionManager;
  private final ExecToCoordTunnelCreator execToCoord;
  private final ExecConnectionCreator dataCreator;
  private final NamespaceService namespace;

  private final OperatorCreatorRegistry opCreator;
  private final FunctionImplementationRegistry funcRegistry;
  private final CodeCompiler compiler;
  private final PhysicalPlanReader planReader;
  private final SchemaChangeListener schemaUpdater;
  private final Set<ClusterCoordinator.Role> roles;
  private final StoragePluginRegistry storagePluginRegistry;
  private final ContextInformationFactory contextInformationFactory;
  private final NodeDebugContextProvider nodeDebugContextProvider;

  public FragmentExecutorBuilder(
      QueriesClerk clerk,
      SabotConfig config,
      ClusterCoordinator coord,
      ExecutorService executorService,
      SystemOptionManager optionManager,
      ExecToCoordTunnelCreator execToCoord,
      ExecConnectionCreator dataCreator,
      ScanResult scanResult,
      PhysicalPlanReader planReader,
      NamespaceService namespace,
      StoragePluginRegistry storagePluginRegistry,
      ContextInformationFactory contextInformationFactory,
      FunctionImplementationRegistry functions,
      NodeDebugContextProvider nodeDebugContextProvider,
      Set<ClusterCoordinator.Role> roles) {
    this.clerk = clerk;
    this.config = config;
    this.coord = coord;
    this.executorService = executorService;
    this.optionManager = optionManager;
    this.execToCoord = execToCoord;
    this.dataCreator = dataCreator;
    this.namespace = namespace;
    this.planReader = planReader;
    this.opCreator = new OperatorCreatorRegistry(scanResult);
    this.funcRegistry = functions;
    this.compiler = new CodeCompiler(config, optionManager);
    this.schemaUpdater = new NamespaceUpdater(namespace);
    this.roles = roles;
    this.storagePluginRegistry = storagePluginRegistry;
    this.contextInformationFactory = contextInformationFactory;
    this.nodeDebugContextProvider = nodeDebugContextProvider;
  }

  public FragmentExecutor build(PlanFragment fragment, EventProvider eventProvider) throws Exception {

    final AutoCloseableList services = new AutoCloseableList();

    try(RollbackCloseable commit = new RollbackCloseable(services)){
      final OptionList list;
      if (!fragment.hasOptionsJson() || fragment.getOptionsJson().isEmpty()) {
        list = new OptionList();
      } else {
        try {
          list = planReader.readOptionList(fragment.getOptionsJson(), fragment.getFragmentCodec());
        } catch (final Exception e) {
          throw new ExecutionSetupException("Failure while reading plan options.", e);
        }
      }

      final FragmentHandle handle = fragment.getHandle();
      final FragmentTicket ticket = services.protect(clerk.newFragmentTicket(fragment));
      logger.debug("Getting initial memory allocation of {}", fragment.getMemInitial());
      logger.debug("Fragment max allocation: {}", fragment.getMemMax());

      // Add the fragment context to the root allocator.
      final BufferAllocator allocator;
      try {
        allocator = ticket.newChildAllocator("frag:" + QueryIdHelper.getFragmentId(fragment.getHandle()),
          fragment.getMemInitial(), fragment.getMemMax());
        Preconditions.checkNotNull(allocator, "Unable to acquire allocator");
        services.protect(allocator);
      } catch (final OutOfMemoryException e) {
        throw UserException.memoryError(e)
          .addContext("Fragment", handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId())
          .build(logger);
      } catch(final Throwable e) {
        throw new ExecutionSetupException("Failure while getting memory allocator for fragment.", e);
      }

      final FragmentStats stats = new FragmentStats(allocator, fragment.getAssignment());
      final SharedResourceManager sharedResources = SharedResourceManager.newBuilder()
        .addGroup(PIPELINE_RES_GRP)
        .addGroup(WORK_QUEUE_RES_GRP)
        .build();

      if (!roles.contains(ClusterCoordinator.Role.COORDINATOR)) {
        // set the SYSTEM options in the system option manager, but only do it on non-coordinator nodes
        for (OptionValue option : list.getSystemOptions()) {
          optionManager.setOption(option);
        }
      }
      // add the remaining options (QUERY, SESSION) to the fragment option manager
      final OptionManager fragmentOptions = new FragmentOptionManager(optionManager, list.getNonSystemOptions());

      final FlushableSendingAccountor flushable = new FlushableSendingAccountor(sharedResources.getGroup(PIPELINE_RES_GRP));
      final ExecutionControls controls = new ExecutionControls(fragmentOptions, fragment.getAssignment());

      final ContextInformation contextInfo =
          contextInformationFactory.newContextFactory(fragment.getCredentials(), fragment.getContext());

      final OperatorContextCreator creator = new OperatorContextCreator(
          stats,
          allocator,
          compiler,
          config,
          handle,
          controls,
          funcRegistry,
          namespace,
          fragmentOptions,
          executorService,
          contextInfo,
          nodeDebugContextProvider);

      final ExecToCoordTunnel coordTunnel = execToCoord.getTunnel(fragment.getForeman());
      final FragmentStatusReporter statusReporter = new FragmentStatusReporter(fragment.getHandle(), stats, coordTunnel, allocator);
      final DeferredException exception = new DeferredException();
      final StatusHandler handler = new StatusHandler(exception);
      final TunnelProvider tunnelProvider = new TunnelProviderImpl(flushable.getAccountor(), coordTunnel, dataCreator, handler, sharedResources.getGroup(PIPELINE_RES_GRP));

      final FragmentExecutor executor = new FragmentExecutor(
          statusReporter,
          config,
          fragment,
          coord,
          planReader,
          sharedResources,
          opCreator,
          allocator,
          schemaUpdater,
          contextInfo,
          creator,
          funcRegistry,
          tunnelProvider,
          flushable,
          stats,
          ticket,
          storagePluginRegistry,
          exception,
          eventProvider
          );

      commit.commit();
      return executor;

    }

  }

  @SuppressWarnings("serial")
  private class AutoCloseableList extends ArrayList<AutoCloseable> implements AutoCloseable {
    private final List<AutoCloseable> items = new ArrayList<>();

    public <T extends AutoCloseable> T protect(T impl){
      items.add(impl);
      return impl;
    }

    @Override
    public void close() throws Exception {
      Collections.reverse(items);
      AutoCloseables.close(items);
    }

  }
}
