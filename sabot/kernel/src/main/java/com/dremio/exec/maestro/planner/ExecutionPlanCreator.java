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
package com.dremio.exec.maestro.planner;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.maestro.MaestroObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.ExecutionPlanningResources;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.MakeFragmentsVisitor;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.SimpleParallelizer;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.util.MemoryAllocationUtilities;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.basic.BasicResourceConstants;
import com.dremio.resource.basic.QueueType;
import com.dremio.service.execselector.ExecutorSelectionContext;
import com.dremio.service.execselector.ExecutorSelectionHandle;
import com.dremio.service.execselector.ExecutorSelectionHandleImpl;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;

public class ExecutionPlanCreator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecutionPlanCreator.class);

  public static ExecutionPlanningResources getParallelizationInfo(
    final QueryContext queryContext,
    MaestroObserver observer,
    final PhysicalPlan plan,
    ExecutorSelectionService executorSelectionService,
    ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) throws ExecutionSetupException {

    final Root rootOperator = plan.getRoot();
    final Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);

    observer.planParallelStart();
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final ExecutionPlanningResources executionPlanningResources = SimpleParallelizer.getExecutionPlanningResources(queryContext, observer, executorSelectionService,
      resourceSchedulingDecisionInfo, rootFragment);
    observer.planParallelized(executionPlanningResources.getPlanningSet());
    stopwatch.stop();
    observer.planAssignmentTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    return executionPlanningResources;
  }

  public static ExecutionPlan getExecutionPlan(
    final QueryContext queryContext,
    final PhysicalPlanReader reader,
    MaestroObserver observer,
    final PhysicalPlan plan,
    ResourceSet allocationSet,
    PlanningSet planningSet,
    ExecutorSelectionService executorSelectionService,
    ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
    GroupResourceInformation groupResourceInformation
  ) throws ExecutionSetupException {

    final Root rootOperator = plan.getRoot();
    final Fragment rootOperatorFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);

    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext,
      observer, executorSelectionService, resourceSchedulingDecisionInfo, groupResourceInformation);
    final long queryPerNodeFromResourceAllocation =  allocationSet.getPerNodeQueryMemoryLimit();
    planningSet.setMemoryAllocationPerNode(queryPerNodeFromResourceAllocation);

    // set bounded memory for all bounded memory operations
    MemoryAllocationUtilities.setupBoundedMemoryAllocations(
        plan,
        queryContext.getOptions(),
        groupResourceInformation,
        planningSet,
        queryPerNodeFromResourceAllocation);

    // pass all query, session and non-default system options to the fragments
    final OptionList fragmentOptions = filterDCSControlOptions(queryContext.getNonDefaultOptions());

    // index repetitive items to reduce rpc size.
    final PlanFragmentsIndex.Builder indexBuilder = new PlanFragmentsIndex.Builder();

    final List<PlanFragmentFull> planFragments = parallelizer.getFragments(
      fragmentOptions,
      planningSet,
      reader,
      rootOperatorFragment,
      indexBuilder);

    traceFragments(queryContext, planFragments);

    return new ExecutionPlan(queryContext.getQueryId(), plan, planFragments, indexBuilder);
  }

  /**
   *  Left just for testing
   * @param queryContext
   * @param reader
   * @param observer
   * @param plan
   * @param queueType
   * @return
   * @throws ExecutionSetupException
   */
  @Deprecated
  public static ExecutionPlan getExecutionPlan(
    final QueryContext queryContext,
    final PhysicalPlanReader reader,
    MaestroObserver observer,
    final PhysicalPlan plan,
    final QueueType queueType) throws ExecutionSetupException {

    // step one, check to make sure that there available execution nodes.
    final Collection<NodeEndpoint> endpoints = queryContext.getActiveEndpoints();
    if (endpoints.isEmpty()){
      throw UserException.resourceError().message("No executors currently available.").build(logger);
    }

    final Root rootOperator = plan.getRoot();
    final Fragment rootOperatorFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
    // Executor selection service whose only purpose is to return 'endpoints'
    ExecutorSelectionService executorSelectionService = new ExecutorSelectionService() {
      @Override
      public ExecutorSelectionHandle getExecutors(int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext) {
        return new ExecutorSelectionHandleImpl(endpoints);
      }

      @Override
      public ExecutorSelectionHandle getAllActiveExecutors(ExecutorSelectionContext executorSelectionContext) {
        return new ExecutorSelectionHandleImpl(endpoints);
      }

      @Override
      public void start() throws Exception {
      }

      @Override
      public void close() throws Exception {
      }
    };
    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext, observer, executorSelectionService);
    // pass all query, session and non-default system options to the fragments
    final OptionList fragmentOptions = filterDCSControlOptions(queryContext.getNonDefaultOptions());

    CoordExecRPC.QueryContextInformation queryContextInformation = queryContext.getQueryContextInfo();

    // update query limit based on the queueType
    final OptionManager options = queryContext.getOptions();
    final boolean memoryControlEnabled = options.getOption(BasicResourceConstants.ENABLE_QUEUE_MEMORY_LIMIT);
    final long memoryLimit = (queueType == QueueType.SMALL) ?
      options.getOption(BasicResourceConstants.SMALL_QUEUE_MEMORY_LIMIT):
      options.getOption(BasicResourceConstants.LARGE_QUEUE_MEMORY_LIMIT);
    if (memoryControlEnabled && memoryLimit > 0) {
      final long queryMaxAllocation = queryContext.getQueryContextInfo().getQueryMaxAllocation();
      queryContextInformation = CoordExecRPC.QueryContextInformation.newBuilder(queryContextInformation)
        .setQueryMaxAllocation(Math.min(memoryLimit, queryMaxAllocation)).build();
    }

    // index repetitive items to reduce rpc size.
    final PlanFragmentsIndex.Builder indexBuilder = new PlanFragmentsIndex.Builder();

    final List<PlanFragmentFull> planFragments = parallelizer.getFragments(
        fragmentOptions,
        queryContext.getCurrentEndpoint(),
        queryContext.getQueryId(),
        reader,
        rootOperatorFragment,
        indexBuilder,
        queryContext.getSession(),
        queryContextInformation,
        queryContext.getFunctionRegistry());

    traceFragments(queryContext, planFragments);

    return new ExecutionPlan(queryContext.getQueryId(), plan, planFragments, indexBuilder);
  }

  // remove any dcs control options; these will not be present in executor image
  private static OptionList filterDCSControlOptions(OptionList nonDefaultSystemOptions) {
    OptionList nonDCSControlOptions = new OptionList();
    nonDCSControlOptions.addAll(nonDefaultSystemOptions.stream().filter(option -> {
        if (option.getName().startsWith("dcs.control")) {
          return false;
        }
        return true;
      }
    ).collect(Collectors.toList()));
    return nonDCSControlOptions;
  }

  private static void traceFragments(QueryContext queryContext, List<PlanFragmentFull> fullPlanFragments) {
    if (logger.isTraceEnabled()) {
      final StringBuilder sb = new StringBuilder();
      sb.append("PlanFragments for query ");
      sb.append(queryContext.getQueryId());
      sb.append('\n');

      final int fragmentCount = fullPlanFragments.size();
      int fragmentIndex = 0;
      for (final PlanFragmentFull full : fullPlanFragments) {
        final PlanFragmentMajor major = full.getMajor();
        final PlanFragmentMinor minor = full.getMinor();

        sb.append("PlanFragment(");
        sb.append(++fragmentIndex);
        sb.append('/');
        sb.append(fragmentCount);
        sb.append(") major_fragment_id ");
        sb.append(minor.getMajorFragmentId());
        sb.append(" minor_fragment_id ");
        sb.append(minor.getMinorFragmentId());
        sb.append(" memMax ");
        sb.append(minor.getMemMax());
        sb.append('\n');

        final NodeEndpoint endpointAssignment = minor.getAssignment();
        sb.append("  NodeEndpoint address ");
        sb.append(endpointAssignment.getAddress());
        sb.append('\n');

        String jsonString = "<<malformed JSON>>";
        sb.append("  fragment_json: ");
        final ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream is = PhysicalPlanReader.toInputStream(major.getFragmentJson(), major.getFragmentCodec())) {

          final Object json = objectMapper.readValue(is, Object.class);
          jsonString = objectMapper.writeValueAsString(json);
        } catch (final Exception e) {
          // we've already set jsonString to a fallback value
        }
        sb.append(jsonString);

        logger.trace(sb.toString());
      }
    }
  }

}
