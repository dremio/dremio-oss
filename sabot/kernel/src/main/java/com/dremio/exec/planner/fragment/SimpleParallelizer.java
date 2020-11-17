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
package com.dremio.exec.planner.fragment;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.maestro.MaestroObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange.ParallelizationDependency;
import com.dremio.exec.physical.base.FragmentRoot;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Receiver;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.planner.fragment.Materializer.IndexedFragmentNode;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.Collector;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.FragmentCodec;
import com.dremio.exec.proto.CoordExecRPC.MajorFragmentAssignment;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.SelectedExecutorsResourceInformation;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.dremio.service.execselector.ExecutorSelectionContext;
import com.dremio.service.execselector.ExecutorSelectionHandle;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.execselector.ExecutorSelectionUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

/**
 * The simple parallelizer determines the level of parallelization of a plan based on the cost of the underlying
 * operations.  It doesn't take into account system load or other factors.  Based on the cost of the query, the
 * parallelization for each major fragment will be determined.  Once the amount of parallelization is done, assignment
 * is done based on round robin assignment ordered by operator affinity (locality) to available execution SabotNodes.
 */
public class SimpleParallelizer implements ParallelizationParameters {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);

  private final long parallelizationThreshold;
  private int maxWidthPerNode;
  private final int maxGlobalWidth;
  private final double affinityFactor;
  private final boolean useNewAssignmentCreator;
  private final double assignmentCreatorBalanceFactor;
  private final MaestroObserver observer;
  private final ExecutionNodeMap executionMap;
  private final FragmentCodec fragmentCodec;
  private final QueryContext queryContext;
  private final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;
  private ExecutorSelectionService executorSelectionService;  // NB: re-assigned in unit tests, hence not final
  private final int targetNumFragsPerNode;
  private final boolean shouldIgnoreLeafAffinity;

  public SimpleParallelizer(QueryContext context, MaestroObserver observer, ExecutorSelectionService executorSelectionService) {
    this(context, observer, executorSelectionService, null, context.getGroupResourceInformation());
  }

  public SimpleParallelizer(QueryContext context,
                            MaestroObserver observer,
                            ExecutorSelectionService executorSelectionService,
                            ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
                            GroupResourceInformation groupResourceInformation) {
    this.queryContext = context;
    this.resourceSchedulingDecisionInfo = resourceSchedulingDecisionInfo;
    OptionManager optionManager = context.getOptions();
    long sliceTarget = context.getPlannerSettings().getSliceTarget();
    this.parallelizationThreshold = sliceTarget > 0 ? sliceTarget : 1;
    this.maxGlobalWidth = (int) optionManager.getOption(ExecConstants.MAX_WIDTH_GLOBAL);
    this.affinityFactor = optionManager.getOption(ExecConstants.AFFINITY_FACTOR);
    this.useNewAssignmentCreator = !optionManager.getOption(ExecConstants.OLD_ASSIGNMENT_CREATOR);
    this.assignmentCreatorBalanceFactor = optionManager.getOption(ExecConstants.ASSIGNMENT_CREATOR_BALANCE_FACTOR);
    this.observer = observer;
    this.fragmentCodec = FragmentCodec.valueOf(optionManager.getOption(ExecConstants.FRAGMENT_CODEC).toUpperCase());
    this.executorSelectionService = executorSelectionService;
    this.targetNumFragsPerNode = Ints.saturatedCast(optionManager.getOption(ExecutorSelectionService.TARGET_NUM_FRAGS_PER_NODE));
    this.shouldIgnoreLeafAffinity = optionManager.getOption(ExecConstants.SHOULD_IGNORE_LEAF_AFFINITY);
    final ExecutorSelectionHandle handle = executorSelectionService.getAllActiveExecutors(new ExecutorSelectionContext(resourceSchedulingDecisionInfo));
    this.executionMap = new ExecutionNodeMap(handle.getExecutors());
    computeMaxWidthPerNode(groupResourceInformation);
  }

  private void computeMaxWidthPerNode(GroupResourceInformation groupResourceInformation) {
    OptionManager optionManager = queryContext.getOptions();
      final long configuredMaxWidthPerNode = groupResourceInformation.getAverageExecutorCores(optionManager);
      if (configuredMaxWidthPerNode == 0) {
        ExecutorSelectionUtils.throwEngineOffline(resourceSchedulingDecisionInfo.getQueueTag());
      }
      final double maxWidthFactor = queryContext.getWorkStatsProvider().get().getMaxWidthFactor(groupResourceInformation);
      maxWidthPerNode = (int) Math.max(1, configuredMaxWidthPerNode * maxWidthFactor);
    if (logger.isDebugEnabled() && maxWidthFactor < 1) {
      final float clusterLoad = queryContext.getWorkStatsProvider().get().getClusterLoad();
      logger.debug("Cluster load {} exceeded cutoff, max_width_factor = {}. current max_width = {}",
        clusterLoad, maxWidthFactor, maxWidthPerNode);
    }
  }

  @VisibleForTesting
  public SimpleParallelizer(long parallelizationThreshold,
                            int maxWidthPerNode,
                            int maxGlobalWidth,
                            double affinityFactor,
                            MaestroObserver observer,
                            boolean useNewAssignmentCreator,
                            double assignmentCreatorBalanceFactor,
                            boolean shouldIgnoreLeafAffinity) {
    this.executionMap = new ExecutionNodeMap(Collections.<NodeEndpoint>emptyList());
    this.parallelizationThreshold = parallelizationThreshold;
    this.maxWidthPerNode = maxWidthPerNode;
    this.maxGlobalWidth = maxGlobalWidth;
    this.affinityFactor = affinityFactor;
    this.observer = observer;
    this.useNewAssignmentCreator = useNewAssignmentCreator;
    this.assignmentCreatorBalanceFactor = assignmentCreatorBalanceFactor;
    this.fragmentCodec = FragmentCodec.NONE;
    this.queryContext = null;
    this.targetNumFragsPerNode = 1;
    this.resourceSchedulingDecisionInfo = null;
    this.shouldIgnoreLeafAffinity = shouldIgnoreLeafAffinity;
  }

  @Override
  public long getSliceTarget() {
    return parallelizationThreshold;
  }

  @Override
  public int getMaxWidthPerNode() {
    return maxWidthPerNode;
  }

  @Override
  public int getMaxGlobalWidth() {
    return maxGlobalWidth;
  }

  @Override
  public double getAffinityFactor() {
    return affinityFactor;
  }

  @Override
  public boolean useNewAssignmentCreator() {
    return useNewAssignmentCreator;
  }

  @Override
  public double getAssignmentCreatorBalanceFactor(){
    return assignmentCreatorBalanceFactor;
  }

  @Override
  public boolean shouldIgnoreLeafAffinity() {
    return shouldIgnoreLeafAffinity;
  }

  /**
   * Generate a set of assigned fragments based on the provided fragment tree. Do not allow parallelization stages
   * to go beyond the global max width.
   *
   * @param options         Option list
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param reader          Tool used to read JSON plans
   * @param rootFragment    The root node of the PhysicalPlan that we will be parallelizing.
   * @param session         UserSession of user who launched this query.
   * @param queryContextInfo Info related to the context when query has started.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ExecutionSetupException
   */
  @Deprecated // ("only used in test")
  public List<PlanFragmentFull> getFragments(
      OptionList options,
      NodeEndpoint foremanNode,
      QueryId queryId,
      PhysicalPlanReader reader,
      Fragment rootFragment,
      PlanFragmentsIndex.Builder indexBuilder,
      UserSession session,
      QueryContextInformation queryContextInfo,
      FunctionLookupContext functionLookupContext) throws ExecutionSetupException {
    observer.planParallelStart();
    final Stopwatch stopwatch = Stopwatch.createStarted();
    // NB: OK to close resources in unit tests only
    try (final ExecutionPlanningResources resources = getExecutionPlanningResources(queryContext, observer, executorSelectionService,
      resourceSchedulingDecisionInfo, rootFragment)) {
      observer.planParallelized(resources.getPlanningSet());
      stopwatch.stop();
      observer.planAssignmentTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
      stopwatch.start();
      List<PlanFragmentFull> fragments = generateWorkUnit(options, foremanNode, queryId, reader, rootFragment,
          resources.getPlanningSet(), indexBuilder, session, queryContextInfo, functionLookupContext);
      stopwatch.stop();
      observer.planGenerationTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
      observer.plansDistributionComplete(new QueryWorkUnit(fragments));
      return fragments;
    } catch (Exception e) {
      // Test-only code. Wrap in a runtime exception, then re-throw
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate set of assigned fragments based on predefined PlanningSet
   * versus doing parallelization in place
   * QueryContext has to be not null from construction of Parallelizer
   * @param options
   * @param planningSet
   * @param reader
   * @param rootFragment
   * @return
   * @throws ExecutionSetupException
   */
  public List<PlanFragmentFull> getFragments(
    OptionList options,
    PlanningSet planningSet,
    PhysicalPlanReader reader,
    Fragment rootFragment,
    PlanFragmentsIndex.Builder indexBuilder) throws ExecutionSetupException {
    Preconditions.checkNotNull(queryContext);
    final Stopwatch stopwatch = Stopwatch.createStarted();
    List<PlanFragmentFull> fragments =
      generateWorkUnit(options, reader, rootFragment, planningSet, indexBuilder);
    stopwatch.stop();
    observer.planGenerationTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    observer.plansDistributionComplete(new QueryWorkUnit(fragments));
    return fragments;
  }

  /**
   * Select executors, parallelize fragments and get the planning resources.
   *
   * @
   * @param rootFragment
   * @return
   */


  public static ExecutionPlanningResources getExecutionPlanningResources(QueryContext context,
                                                                         MaestroObserver observer,
                                                                         ExecutorSelectionService executorSelectionService,
                                                                         ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
                                                                         Fragment rootFragment) throws ExecutionSetupException {
    SimpleParallelizer parallelizer = new SimpleParallelizer(context, observer, executorSelectionService, resourceSchedulingDecisionInfo, context.getGroupResourceInformation());
    PlanningSet planningSet = new PlanningSet();
    parallelizer.initFragmentWrappers(rootFragment, planningSet);
    final Set<Wrapper> leafFragments = constructFragmentDependencyGraph(planningSet);
    // NB: for queries with hard affinity, we need to use all endpoints, so the parallelizer, below, is given an
    //     opportunity to find the nodes that match said affinity
    // Start parallelizing from leaf fragments
    Pointer<Boolean> hasHardAffinity = new Pointer<>(false);

    // Start parallelizing from the leaf fragments.

    int idealNumFragments = 0;
    for (Wrapper wrapper : leafFragments) {
      idealNumFragments += parallelizer.computePhaseStats(wrapper, planningSet, hasHardAffinity);
    }
    int idealNumNodes = IntMath.divide(idealNumFragments, parallelizer.targetNumFragsPerNode, RoundingMode.CEILING);
    final Stopwatch stopWatch = Stopwatch.createStarted();
    ExecutorSelectionContext executorContext = new ExecutorSelectionContext(resourceSchedulingDecisionInfo);
    ExecutorSelectionHandle executorSelectionHandle = hasHardAffinity.value
      ? executorSelectionService.getAllActiveExecutors(executorContext)
      : executorSelectionService.getExecutors(idealNumNodes, executorContext);

    GroupResourceInformation groupResourceInformation = new SelectedExecutorsResourceInformation(executorSelectionHandle.getExecutors());
    parallelizer.computeMaxWidthPerNode(groupResourceInformation);

    final ExecutionPlanningResources executionPlanningResources = new ExecutionPlanningResources(planningSet, executorSelectionHandle, groupResourceInformation);
    final Collection<NodeEndpoint> selectedEndpoints = executorSelectionHandle.getExecutors();
    stopWatch.stop();
    observer.executorsSelected(stopWatch.elapsed(TimeUnit.MILLISECONDS),
      idealNumFragments, idealNumNodes, selectedEndpoints.size(),
      executorSelectionHandle.getPlanDetails() +
        " selectedEndpoints: " + EndpointHelper.getMinimalString(selectedEndpoints) +
        " hardAffinity: " + hasHardAffinity.value);
    if (selectedEndpoints.isEmpty()) {
      ExecutorSelectionUtils.throwEngineOffline(resourceSchedulingDecisionInfo.getQueueTag());
    }

    for (Wrapper wrapper : leafFragments) {
      parallelizer.parallelizePhase(wrapper, planningSet, selectedEndpoints);
    }

    return executionPlanningResources;
  }

  // For every fragment, create a Wrapper in PlanningSet.
  @VisibleForTesting
  public void initFragmentWrappers(Fragment rootFragment, PlanningSet planningSet) {
    planningSet.get(rootFragment);

    for(ExchangeFragmentPair fragmentPair : rootFragment) {
      initFragmentWrappers(fragmentPair.getNode(), planningSet);
    }
  }

  /**
   * Based on the affinity of the Exchange that separates two fragments, setup fragment dependencies.
   *
   * @param planningSet
   * @return Returns a list of leaf fragments in fragment dependency graph.
   */
  private static Set<Wrapper> constructFragmentDependencyGraph(PlanningSet planningSet) {

    // Set up dependency of fragments based on the affinity of exchange that separates the fragments.
    for(Wrapper currentFragmentWrapper : planningSet) {
      ExchangeFragmentPair sendingExchange = currentFragmentWrapper.getNode().getSendingExchangePair();
      if (sendingExchange != null) {
        ParallelizationDependency dependency = sendingExchange.getExchange().getParallelizationDependency();
        Wrapper receivingFragmentWrapper = planningSet.get(sendingExchange.getNode());

        if (dependency == ParallelizationDependency.RECEIVER_DEPENDS_ON_SENDER) {
          receivingFragmentWrapper.addFragmentDependency(currentFragmentWrapper);
        } else if (dependency == ParallelizationDependency.SENDER_DEPENDS_ON_RECEIVER) {
          currentFragmentWrapper.addFragmentDependency(receivingFragmentWrapper);
        }
      }
    }

    // Identify leaf fragments. Leaf fragments are fragments that have no other fragments depending on them for
    // parallelization info. First assume all fragments are leaf fragments. Go through the fragments one by one and
    // remove the fragment on which the current fragment depends on.
    final Set<Wrapper> roots = Sets.newHashSet();
    for(Wrapper w : planningSet) {
      roots.add(w);
    }

    for(Wrapper wrapper : planningSet) {
      final List<Wrapper> fragmentDependencies = wrapper.getFragmentDependencies();
      if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
        for(Wrapper dependency : fragmentDependencies) {
          if (roots.contains(dependency)) {
            roots.remove(dependency);
          }
        }
      }
    }

    return roots;
  }

  /**
   * Compute the parallelization stats for a given phase. Dependent phases are processed first before
   * processing the given phase.
   * Returns the maximum number of (minor) fragments that could be used by this phase and its dependents
   */
  private int computePhaseStats(Wrapper fragmentWrapper, PlanningSet planningSet, Pointer<Boolean> hasHardAffinity) {
    // If the fragment is already processed, return.
    if (fragmentWrapper.isStatsComputationDone()) {
      return 0;
    }

    // First compute the fragment stats for fragments on which this fragment depends on.
    int width = 0;
    final List<Wrapper> fragmentDependencies = fragmentWrapper.getFragmentDependencies();
    if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
      for(Wrapper dependency : fragmentDependencies) {
        width += computePhaseStats(dependency, planningSet, hasHardAffinity);
      }
    }

    // Find stats. Stats include various factors including cost of physical operators, parallelizability of
    // work in physical operator and affinity of physical operator to certain nodes.
    fragmentWrapper.getNode().getRoot().accept(new StatsCollector(planningSet, executionMap), fragmentWrapper);
    DistributionAffinity fragmentAffinity = fragmentWrapper.getStats().getDistributionAffinity();
    width += fragmentAffinity.getFragmentParallelizer()
      .getIdealFragmentWidth(fragmentWrapper, this);
    if (DistributionAffinity.HARD.equals(fragmentAffinity)) {
      hasHardAffinity.value = true;
    }
    fragmentWrapper.statsComputationDone();
    return width;
  }

  /**
   * Helper method for parallelizing a given phase. Dependent phases are parallelized first before
   * parallelizing the given phase.
   * Assumes the stats have already been computed for all
   */
  private void parallelizePhase(Wrapper fragmentWrapper, PlanningSet planningSet,
                                Collection<NodeEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    assert fragmentWrapper.isStatsComputationDone();

    // If the fragment is already parallelized, return.
    if (fragmentWrapper.isEndpointsAssignmentDone()) {
      return;
    }

    // First parallelize fragments on which this fragment depends on.
    final List<Wrapper> fragmentDependencies = fragmentWrapper.getFragmentDependencies();
    if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
      for(Wrapper dependency : fragmentDependencies) {
        parallelizePhase(dependency, planningSet, activeEndpoints);
      }
    }

    fragmentWrapper.getStats().getDistributionAffinity()
      .getFragmentParallelizer()
      .parallelizeFragment(fragmentWrapper, this, activeEndpoints);
  }

  /**
   * To facilitate generating workunits
   * with the assumption that QueryContext is NOT null
   * it's not always going to be true, since e.g. QueryContextInfo
   * may change between ctor and this method
   * @param options
   * @param reader
   * @param rootNode
   * @param planningSet
   * @return
   * @throws ExecutionSetupException
   */
  private List<PlanFragmentFull> generateWorkUnit(
    OptionList options,
    PhysicalPlanReader reader,
    Fragment rootNode,
    PlanningSet planningSet,
    PlanFragmentsIndex.Builder indexBuilder) throws ExecutionSetupException {
    Preconditions.checkNotNull(queryContext);
    return generateWorkUnit(options,
      queryContext.getCurrentEndpoint(),
      queryContext.getQueryId(),
      reader,
      rootNode,
      planningSet,
      indexBuilder,
      queryContext.getSession(),
      queryContext.getQueryContextInfo(),
      queryContext.getFunctionRegistry());
  }

    protected List<PlanFragmentFull> generateWorkUnit(
      OptionList options,
      NodeEndpoint foremanNode,
      QueryId queryId,
      PhysicalPlanReader reader,
      Fragment rootNode,
      PlanningSet planningSet,
      PlanFragmentsIndex.Builder indexBuilder,
      UserSession session,
      QueryContextInformation queryContextInfo,
      FunctionLookupContext functionLookupContext) throws ExecutionSetupException {

    final List<PlanFragmentFull> fragments = Lists.newArrayList();
    EndpointsIndex.Builder builder = indexBuilder.getEndpointsIndexBuilder();
    MajorFragmentAssignmentCache majorFragmentAssignmentsCache = new MajorFragmentAssignmentCache();
    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1) {
        throw new ForemanSetupException(String.format("Failure while trying to setup fragment. " +
                "The root fragment must always have parallelization one. In the current case, the width was set to %d.",
                wrapper.getWidth()));
      }
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      CoordExecRPC.QueryContextInformation queryContextInformation = CoordExecRPC.QueryContextInformation.newBuilder
        (queryContextInfo)
        .setQueryMaxAllocation(wrapper.getMemoryAllocationPerNode()).build();

      // come up with a list of minor fragments assigned for each endpoint.
      final List<FragmentAssignment> assignments = new ArrayList<>();

      if(queryContext.getOptions().getOption(VectorizedHashAggOperator.OOB_SPILL_TRIGGER_ENABLED) ||
        queryContext.getOptions().getOption(ExternalSortOperator.OOB_SORT_TRIGGER_ENABLED)) {

        // collate by node.
        ArrayListMultimap<Integer, Integer> assignMap = ArrayListMultimap.create();
        for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
          assignMap.put(builder.addNodeEndpoint(wrapper.getAssignedEndpoint(minorFragmentId)), minorFragmentId);
        }

        // create getAssignment lists.
        for(int ep : assignMap.keySet()) {
          assignments.add(
            FragmentAssignment.newBuilder()
              .setAssignmentIndex(ep)
              .addAllMinorFragmentId(assignMap.get(ep))
              .build());
        }
      }

      // Create a minorFragment for each major fragment.
      PlanFragmentMajor major = null;
      boolean majorAdded = false;
      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(new Materializer(wrapper.getSplitSets(), builder), iNode);

        Preconditions.checkArgument(op instanceof FragmentRoot);
        FragmentRoot root = (FragmentRoot) op;

        FragmentHandle handle =
          FragmentHandle //
            .newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId)
            .setQueryId(queryId) //
            .build();

        // Build the major fragment only once.
        if (!majorAdded) {
          majorAdded = true;

          // get plan as JSON
          ByteString plan;
          ByteString optionsData;
          try {
            plan = reader.writeJsonBytes(root, fragmentCodec);
            optionsData = reader.writeJsonBytes(options, fragmentCodec);
          } catch (JsonProcessingException e) {
            throw new ForemanSetupException("Failure while trying to convert fragment into json.", e);
          }

          // If any of the operators report ext communicable fragments, fill in the assignment and node details.
          final Set<Integer> extCommunicableMajorFragments = physicalOperatorRoot.accept(new ExtCommunicableFragmentCollector(), wrapper);
          majorFragmentAssignmentsCache.populateIfAbsent(planningSet, builder, extCommunicableMajorFragments);
          final List<MajorFragmentAssignment> extFragmentAssignments =
                  majorFragmentAssignmentsCache.getAssignments(planningSet, builder, extCommunicableMajorFragments);
          major =
              PlanFragmentMajor.newBuilder()
                  .setForeman(foremanNode)
                  .setFragmentJson(plan)
                  .setHandle(handle.toBuilder().clearMinorFragmentId().build())
                  .setLeafFragment(isLeafFragment)
                  .setContext(queryContextInformation)
                  .setMemInitial(wrapper.getInitialAllocation())
                  .setOptionsJson(optionsData)
                  .setCredentials(session.getCredentials())
                  .setPriority(queryContextInfo.getPriority())
                  .setFragmentCodec(fragmentCodec)
                  .addAllAllAssignment(assignments)
                  .addAllExtFragmentAssignments(extFragmentAssignments)
                  .build();

          if (logger.isTraceEnabled()) {
            logger.trace(
                "Remote major fragment:\n {}", DremioStringUtils.unescapeJava(major.toString()));
          }
        }

        final NodeEndpoint assignment = wrapper.getAssignedEndpoint(minorFragmentId);
        final NodeEndpoint endpoint = builder.getMinimalEndpoint(assignment);

        List<MinorAttr> attrList = MinorDataCollector.collect(handle,
          endpoint,
          root,
          new MinorDataSerDe(reader,fragmentCodec),
          indexBuilder);


        // Build minor specific info and attributes.
        PlanFragmentMinor minor = PlanFragmentMinor.newBuilder()
          .setMajorFragmentId(wrapper.getMajorFragmentId())
          .setMinorFragmentId(minorFragmentId)
          .setAssignment(endpoint)
          .setMemMax(wrapper.getMemoryAllocationPerNode())
          .addAllCollector(CountRequiredFragments.getCollectors(root))
          .addAllAttrs(attrList)
          .build();

        if (logger.isTraceEnabled()) {
          logger.trace(
            "Remote minor fragment:\n {}", DremioStringUtils.unescapeJava(minor.toString()));
        }

        fragments.add(new PlanFragmentFull(major, minor));
      }
    }

    return fragments;
  }

  /**
   * Designed to setup initial values for arriving fragment accounting.
   */
  protected static class CountRequiredFragments extends AbstractPhysicalVisitor<Void, List<Collector>, RuntimeException> {

    private CountRequiredFragments() {
    }

    public static List<Collector> getCollectors(PhysicalOperator root) {
      CountRequiredFragments counter = new CountRequiredFragments();
      List<Collector> collectors = Lists.newArrayList();
      root.accept(counter, collectors);
      return collectors;
    }

    @Override
    public Void visitReceiver(Receiver receiver, List<Collector> collectors) throws RuntimeException {
      collectors.add(Collector.newBuilder()
        .setIsSpooling(receiver.isSpooling())
        .setOppositeMajorFragmentId(receiver.getSenderMajorFragmentId())
        .setSupportsOutOfOrder(receiver.supportsOutOfOrderExchange())
        .addAllIncomingMinorFragmentIndex(receiver.getProvidingEndpoints())
        .build());
      return null;
    }

    @Override
    public Void visitOp(PhysicalOperator op, List<Collector> collectors) throws RuntimeException {
      for (PhysicalOperator o : op) {
        o.accept(this, collectors);
      }
      return null;
    }

  }

  // Test-only: change the executor selection service
  @VisibleForTesting
  public void setExecutorSelectionService(ExecutorSelectionService executorSelectionService) {
    this.executorSelectionService = executorSelectionService;
  }

  private class MajorFragmentAssignmentCache {
    private final Map<Integer, MajorFragmentAssignment> majorFragmentAssignments = new HashMap<>();

    private List<MajorFragmentAssignment> getAssignments(final PlanningSet planningSet,
                                                         final EndpointsIndex.Builder builder,
                                                         final Set<Integer> requiredFragments) {
      populateIfAbsent(planningSet, builder, requiredFragments);
      return requiredFragments.stream().map(majorFragmentAssignments::get).collect(Collectors.toList());
    }

    private void populateIfAbsent(final PlanningSet planningSet,
                                  final EndpointsIndex.Builder builder,
                                  final Set<Integer> requiredFragments) {
      if (requiredFragments.isEmpty()) {
        return;
      }
      if (majorFragmentAssignments.keySet().containsAll(requiredFragments)) {
        return;
      }
      for (Wrapper wrapper : planningSet) {
        final int majorFragment = wrapper.getMajorFragmentId();
        if (!requiredFragments.contains(majorFragment) || majorFragmentAssignments.containsKey(majorFragment)) {
          continue;
        }
        final ArrayListMultimap<Integer, Integer> assignMap = ArrayListMultimap.create();
        for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
          assignMap.put(builder.addNodeEndpoint(wrapper.getAssignedEndpoint(minorFragmentId)), minorFragmentId);
        }

        // create getAssignment lists.
        final List<FragmentAssignment> assignments = assignMap.keySet().stream()
                .map(ep -> FragmentAssignment.newBuilder().setAssignmentIndex(ep).addAllMinorFragmentId(assignMap.get(ep)).build())
                .collect(Collectors.toList());

        majorFragmentAssignments.putIfAbsent(majorFragment, MajorFragmentAssignment.newBuilder()
                .setMajorFragmentId(majorFragment).addAllAllAssignment(assignments).build());
      }
    }
  }
}
