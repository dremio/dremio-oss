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
package com.dremio.sabot.exec.fragment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.fn.DecimalFunctionImplementationRegistry;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.CachedFragmentReader;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.FragmentOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.exec.EventProvider;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.FragmentTicket;
import com.dremio.sabot.exec.FragmentWorkManager.ExecConnectionCreator;
import com.dremio.sabot.exec.MaestroProxy;
import com.dremio.sabot.exec.QueriesClerk;
import com.dremio.sabot.exec.QueryStarter;
import com.dremio.sabot.exec.QueryTicket;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.ContextInformationFactory;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.context.StatusHandler;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.jobresults.client.JobResultsClientFactory;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.spill.SpillService;
import com.dremio.services.jobresults.common.JobResultsTunnel;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.util.internal.OutOfDirectMemoryError;

/**
 * Singleton utility to help in constructing a FragmentExecutor.
 */
public class FragmentExecutorBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutorBuilder.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(FragmentExecutorBuilder.class);

  static final String PIPELINE_RES_GRP = "pipeline";
  static final String WORK_QUEUE_RES_GRP = "work-queue";
  static final String OOB_QUEUE = "oob-queue";

  @VisibleForTesting
  public static final String INJECTOR_DO_WORK = "injectOOMOnBuild";

  private final QueriesClerk clerk;
  private final FragmentExecutors fragmentExecutors;
  private final CoordinationProtos.NodeEndpoint nodeEndpoint;
  private final MaestroProxy maestroProxy;
  private final SabotConfig config;
  private final ClusterCoordinator coord;
  private final ExecutorService executorService;
  private final OptionManager optionManager;
  private final ExecConnectionCreator dataCreator;
  private final NamespaceService namespace;

  private final OperatorCreatorRegistry opCreator;
  private final FunctionImplementationRegistry funcRegistry;
  private final DecimalFunctionImplementationRegistry decimalFuncRegistry;
  private final CodeCompiler compiler;
  private final PhysicalPlanReader planReader;
  private final Set<ClusterCoordinator.Role> roles;
  private final CatalogService sources;
  private final ContextInformationFactory contextInformationFactory;
  private final NodeDebugContextProvider nodeDebugContextProvider;
  private final SpillService spillService;
  private final Provider<JobResultsClientFactory> jobResultsClientFactoryProvider;
  private Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider;

  public FragmentExecutorBuilder(
    QueriesClerk clerk,
    FragmentExecutors fragmentExecutors,
    CoordinationProtos.NodeEndpoint nodeEndpoint,
    MaestroProxy maestroProxy,
    SabotConfig config,
    ClusterCoordinator coord,
    ExecutorService executorService,
    OptionManager optionManager,
    ExecConnectionCreator dataCreator,
    ScanResult scanResult,
    PhysicalPlanReader planReader,
    NamespaceService namespace,
    CatalogService sources,
    ContextInformationFactory contextInformationFactory,
    FunctionImplementationRegistry functions,
    DecimalFunctionImplementationRegistry decimalFunctions,
    NodeDebugContextProvider nodeDebugContextProvider,
    SpillService spillService,
    CodeCompiler codeCompiler,
    Set<ClusterCoordinator.Role> roles,
    Provider<JobResultsClientFactory> jobResultsClientFactoryProvider,
    Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider) {
    this.clerk = clerk;
    this.fragmentExecutors = fragmentExecutors;
    this.nodeEndpoint = nodeEndpoint;
    this.maestroProxy = maestroProxy;
    this.config = config;
    this.coord = coord;
    this.executorService = executorService;
    this.optionManager = optionManager;
    this.dataCreator = dataCreator;
    this.namespace = namespace;
    this.planReader = planReader;
    this.opCreator = new OperatorCreatorRegistry(scanResult);
    this.funcRegistry = functions;
    this.decimalFuncRegistry = decimalFunctions;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.compiler = codeCompiler;
    this.roles = roles;
    this.sources = sources;
    this.contextInformationFactory = contextInformationFactory;
    this.nodeDebugContextProvider = nodeDebugContextProvider;
    this.spillService = spillService;
    this.jobResultsClientFactoryProvider = jobResultsClientFactoryProvider;
  }

  public FragmentExecutors getFragmentExecutors() { return fragmentExecutors; }

  public CoordinationProtos.NodeEndpoint getNodeEndpoint() {
    return nodeEndpoint;
  }

  public PhysicalPlanReader getPlanReader() {
    return planReader;
  }

  public QueriesClerk getClerk() { return clerk; }

  /**
   * Obtains a query ticket, then starts the query with this query ticket
   *
   * The query might be built and started in the calling thread, *or*, it might be built and started by a worker thread
   */
  public void buildAndStartQuery(PlanFragmentFull firstFragment,final SchedulingInfo schedulingInfo,
                                 final QueryStarter queryStarter) {
    clerk.buildAndStartQuery(firstFragment, schedulingInfo, queryStarter);
  }

  public FragmentExecutor build(final QueryTicket queryTicket,
                                final PlanFragmentFull fragment,
                                final EventProvider eventProvider,
                                final SchedulingInfo schedulingInfo,
                                final CachedFragmentReader cachedReader) throws Exception {

    final AutoCloseableList services = new AutoCloseableList();
    final PlanFragmentMajor major = fragment.getMajor();

    try(RollbackCloseable commit = new RollbackCloseable(services)){
      final OptionList list = cachedReader.readOptions(fragment);
      final FragmentHandle handle = fragment.getHandle();
      final FragmentTicket ticket = services.protect(clerk.newFragmentTicket(queryTicket, fragment, schedulingInfo));
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

      try {
        final FragmentStats stats = new FragmentStats(allocator, handle, fragment.getAssignment(), optionManager.getOption(ExecConstants.STORE_IO_TIME_WARN_THRESH_MILLIS));
        final SharedResourceManager sharedResources = SharedResourceManager.newBuilder()
            .addGroup(PIPELINE_RES_GRP)
            .addGroup(WORK_QUEUE_RES_GRP)
            .build();

        if (!roles.contains(ClusterCoordinator.Role.COORDINATOR)) {
          // set the SYSTEM options in the system option manager, but only do it on non-coordinator nodes
          boolean enableHeapMonitoringOptionPresent = false;
          boolean thresholdOptionPresent = false;

          for (OptionValue option : list.getSystemOptions()) {
            if(ExecConstants.ENABLE_HEAP_MONITORING.getOptionName().equals(option.getName())) {
              enableHeapMonitoringOptionPresent = true;
            } else if(ExecConstants.HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE.getOptionName().equals(option.getName())) {
              thresholdOptionPresent = true;
            }
            optionManager.setOption(option);
          }

          // Deleting heap monitor related options if not present in system options.
          // This will ensure that heap monitor system options which were reset
          // (to default) on coordinator will be also reset on non-coordinator nodes.
          if (!enableHeapMonitoringOptionPresent) {
            optionManager.deleteOption(ExecConstants.ENABLE_HEAP_MONITORING.getOptionName(),
                                       OptionValue.OptionType.SYSTEM);
          }

          if (!thresholdOptionPresent) {
            optionManager.deleteOption(ExecConstants.HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE.getOptionName(),
                                       OptionValue.OptionType.SYSTEM);
          }
        }
        // add the remaining options (QUERY, SESSION) to the fragment option manager
        final FragmentOptionManager fragmentOptionManager = new FragmentOptionManager(
            optionManager.getOptionValidatorListing(), list);
        final OptionManager fragmentOptions = OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(new DefaultOptionManager(optionManager.getOptionValidatorListing()))
            .withOptionManager(fragmentOptionManager)
            .build();

        final FlushableSendingAccountor flushable = new FlushableSendingAccountor(sharedResources.getGroup(PIPELINE_RES_GRP));
        final ExecutionControls controls = new ExecutionControls(fragmentOptions, fragment.getAssignment());

        final ContextInformation contextInfo =
            contextInformationFactory.newContextFactory(major.getCredentials(), major.getContext());

        // create rpc connections
        final JobResultsTunnel jobResultsTunnel = jobResultsClientFactoryProvider.get()
            .getJobResultsClient(major.getForeman(), allocator, QueryIdHelper.getFragmentId(fragment.getHandle())).getTunnel();
        final DeferredException exception = new DeferredException();
        final StatusHandler handler = new StatusHandler(exception);
        final TunnelProvider tunnelProvider = new TunnelProviderImpl(flushable.getAccountor(), jobResultsTunnel, dataCreator, handler, sharedResources.getGroup(PIPELINE_RES_GRP));

        final OperatorContextCreator creator = new OperatorContextCreator(
            stats,
            allocator,
            compiler,
            config,
            handle,
            controls,
            funcRegistry,
            decimalFuncRegistry,
            namespace,
            fragmentOptions,
            this,
            executorService,
            spillService,
            contextInfo,
            nodeDebugContextProvider,
            tunnelProvider,
            major.getAllAssignmentList(),
            cachedReader.getPlanFragmentsIndex().getEndpointsIndex(),
            nodeEndpointProvider,
            major.getExtFragmentAssignmentsList()
          );

        final FragmentStatusReporter statusReporter = new FragmentStatusReporter(fragment.getHandle(), stats,
            maestroProxy, allocator);
        final FragmentExecutor executor = new FragmentExecutor(
            statusReporter,
            config,
            controls,
            fragment,
            coord,
            cachedReader,
            sharedResources,
            opCreator,
            allocator,
            contextInfo,
            creator,
            funcRegistry,
            decimalFuncRegistry,
            tunnelProvider,
            flushable,
            fragmentOptions,
            stats,
            ticket,
            sources,
            exception,
            eventProvider,
            spillService
        );
        commit.commit();

        injector.injectChecked(controls, INJECTOR_DO_WORK, OutOfMemoryException.class);

        return executor;
      } catch (Exception e) {
        UserException.Builder builder = UserException.systemError(e).message("Failure while constructing fragment.")
                .addContext("Location - ",
                        String.format("Major Fragment:%d, Minor fragment:%d", handle.getMajorFragmentId(), handle.getMinorFragmentId()));

        OutOfMemoryException oom = ErrorHelper.findWrappedCause(e, OutOfMemoryException.class);
        if (oom != null) {
          nodeDebugContextProvider.addMemoryContext(builder, oom);
        } else if (ErrorHelper.findWrappedCause(e, OutOfDirectMemoryError.class) != null) {
          nodeDebugContextProvider.addMemoryContext(builder);
        }
        throw builder.build(logger);
      }
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
