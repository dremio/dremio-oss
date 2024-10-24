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
package com.dremio.exec.maestro;

import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.maestro.FragmentSubmitListener.FragmentSubmitFailures;
import com.dremio.exec.maestro.FragmentSubmitListener.FragmentSubmitSuccess;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PlanFragmentStats;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.ActivateFragments;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Class used to start remote fragment execution. */
class FragmentStarter {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FragmentStarter.class);
  private static long RPC_WAIT_IN_MSECS_PER_FRAGMENT = 5000L;
  private static final long RPC_MIN_WAIT_IN_MSECS = 30000L;
  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(FragmentStarter.class);
  public OptionManager optionManager;

  @VisibleForTesting
  public static final String INJECTOR_BEFORE_START_FRAGMENTS_ERROR = "beforeStartFragmentsError";

  @VisibleForTesting
  public static final String INJECTOR_AFTER_START_FRAGMENTS_ERROR = "afterStartFragmentsError";

  @VisibleForTesting
  public static final String INJECTOR_BEFORE_START_FRAGMENTS_PAUSE = "beforeStartFragmentsPause";

  @VisibleForTesting
  public static final String INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_ERROR =
      "beforeActivateFragmentsError";

  @VisibleForTesting
  public static final String INJECTOR_AFTER_ACTIVATE_FRAGMENTS_ERROR =
      "afterActivateFragmentsError";

  @VisibleForTesting
  public static final String INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_PAUSE =
      "beforeActivateFragmentsPause";

  @VisibleForTesting
  public static final String INJECTOR_AFTER_ON_COMPLETED_PAUSE = "afterOnCompletedPause";

  private final ExecutorServiceClientFactory executorServiceClientFactory;
  private final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;
  private final ExecutionControls executionControls;
  private MaestroObserver observer = null;

  public FragmentStarter(
      ExecutorServiceClientFactory executorServiceClientFactory,
      ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo,
      ExecutionControls executionControls,
      OptionManager optionManager) {
    this.executorServiceClientFactory = executorServiceClientFactory;
    this.resourceSchedulingDecisionInfo = resourceSchedulingDecisionInfo;
    this.executionControls = executionControls;
    this.optionManager = optionManager;
  }

  public void start(ExecutionPlan plan, MaestroObserver observer) {
    Preconditions.checkNotNull(observer, "observer should not be null.");
    startFragments(plan, observer);
  }

  /**
   * Set up the fragments for execution. Some may be local, and some may be remote. Messages are
   * sent immediately, so they may start returning data even before we complete this.
   *
   * @param plan the execution plan
   */
  protected void startFragments(ExecutionPlan plan, MaestroObserver observer) {
    final Collection<PlanFragmentFull> fullFragments = plan.getFragments();
    if (fullFragments.isEmpty()) {
      // nothing to do here
      return;
    }

    injector.injectChecked(
        executionControls, INJECTOR_BEFORE_START_FRAGMENTS_ERROR, IllegalStateException.class);
    injector.injectPause(executionControls, INJECTOR_BEFORE_START_FRAGMENTS_PAUSE, logger);

    /*
     * Done using two rpcs to each endpoint :
     *
     * 1. StartFragments
     *    The fragments are instantiated at the end point and can accept incoming messages. At this
     *    point, none of the fragments can send messages.
     *
     * 2. ActivateFragments
     *    Start execution pipeline, can send messages.
     */
    final Multimap<NodeEndpoint, PlanFragmentFull> fragmentMap = ArrayListMultimap.create();

    // record all fragments for status purposes.
    for (final PlanFragmentFull fragmentFull : fullFragments) {
      final PlanFragmentMajor major = fragmentFull.getMajor();

      if (logger.isTraceEnabled()) {
        // major.getFragmentJson() might be costly (internal ByteString <-> String conversion)
        try {
          logger.trace(
              "Tracking remote node {} with data {}",
              fragmentFull.getAssignment(),
              PhysicalPlanReader.toString(major.getFragmentJson(), major.getFragmentCodec()));
        } catch (IOException e) {
          logger.warn("Error when trying to read fragment", e);
        }
      }
      fragmentMap.put(fragmentFull.getAssignment(), fragmentFull);
    }

    PlanFragmentStats stats = new PlanFragmentStats();
    Stopwatch stopwatch = Stopwatch.createStarted();

    try {
      sendStartFragmentMessages(plan, fragmentMap, stats);
    } finally {
      stopwatch.stop();
      observer.fragmentsStarted(stopwatch.elapsed(TimeUnit.MILLISECONDS), stats.getSummary());
    }

    stopwatch.reset();

    injector.injectChecked(
        executionControls, INJECTOR_AFTER_START_FRAGMENTS_ERROR, IllegalStateException.class);

    this.observer = observer;
    stopwatch.start();
    injector.injectPause(executionControls, INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_PAUSE, logger);

    try {
      sendActivateFragmentMessages(plan, fragmentMap);
    } finally {
      stopwatch.stop();

      // No waiting on acks of sent activate fragment rpcs; so this number is not reliable
      observer.fragmentsActivated(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    injector.injectChecked(
        executionControls, INJECTOR_AFTER_ACTIVATE_FRAGMENTS_ERROR, IllegalStateException.class);
  }

  @WithSpan("send-start-fragments")
  private void sendStartFragmentMessages(
      ExecutionPlan plan,
      Multimap<NodeEndpoint, PlanFragmentFull> fragmentMap,
      PlanFragmentStats stats) {
    final int numFragments = fragmentMap.keySet().size();
    /*
     * We need to wait for the start rpcs to be sent before sending the activate rpcs. We'll use
     * this latch to wait for the responses.
     *
     * However, in order not to hang the process if any of the RPC requests fails, we always
     * count down (see FragmentSubmitFailures), but we count the number of failures so that we'll
     * know if any submissions did fail.
     */
    final ExtendedLatch endpointLatch = new ExtendedLatch(numFragments);
    final FragmentSubmitFailures fragmentSubmitFailures = new FragmentSubmitFailures();
    final FragmentSubmitSuccess fragmentSubmitSuccess = new FragmentSubmitSuccess();
    final List<NodeEndpoint> endpointsIndex =
        plan.getIndexBuilder().getEndpointsIndexBuilder().getAllEndpoints();

    // send rpcs to start fragments
    for (final NodeEndpoint ep : fragmentMap.keySet()) {
      final List<MinorAttr> sharedAttrs =
          plan.getIndexBuilder().getSharedAttrsIndexBuilder(ep).getAllAttrs();
      sendStartFragments(
          ep,
          fragmentMap.get(ep),
          endpointsIndex,
          sharedAttrs,
          endpointLatch,
          fragmentSubmitFailures,
          stats,
          fragmentSubmitSuccess);
    }

    final long timeout =
        Long.max(
            RPC_WAIT_IN_MSECS_PER_FRAGMENT * numFragments,
            Long.max(
                RPC_MIN_WAIT_IN_MSECS,
                optionManager.getOption(ExecConstants.FRAGMENT_STARTER_TIMEOUT)));
    FragmentSubmitListener.awaitUninterruptibly(
        endpointsIndex,
        endpointLatch,
        fragmentSubmitFailures,
        fragmentSubmitSuccess,
        numFragments,
        timeout);

    FragmentSubmitListener.checkForExceptions(fragmentSubmitFailures);
  }

  @WithSpan("send-activate-fragments")
  private void sendActivateFragmentMessages(
      ExecutionPlan plan, Multimap<NodeEndpoint, PlanFragmentFull> fragmentMap) {
    /*
     * Send the activate fragment rpcs; we don't wait for these. Any problems will come in through
     * the regular sendListener event delivery.
     */
    final ActivateFragments activateFragments =
        ActivateFragments.newBuilder().setQueryId(plan.getQueryId()).build();
    if (optionManager.getOption(ExecConstants.ENABLE_DYNAMIC_LOAD_ROUTING)) {
      Optional<NodeEndpoint> ep =
          fragmentMap.keySet().stream()
              .max(Comparator.comparing(object -> object.getAddress().toString()));
      sendActivateFragments(ep.get(), activateFragments);
      return;
    }
    for (final NodeEndpoint ep : fragmentMap.keySet()) {
      sendActivateFragments(ep, activateFragments);
    }
  }

  /**
   * Send all the remote fragments belonging to a single target node in one request.
   *
   * @param assignment the node assigned to these fragments
   * @param fullFragments the set of fragments
   * @param latch the countdown latch used to track the requests to all endpoints
   * @param fragmentSubmitFailures the submission failure counter used to track the requests to all
   *     endpoints
   */
  private void sendStartFragments(
      final NodeEndpoint assignment,
      final Collection<PlanFragmentFull> fullFragments,
      List<NodeEndpoint> endpointsIndex,
      List<MinorAttr> sharedAttrs,
      final CountDownLatch latch,
      final FragmentSubmitFailures fragmentSubmitFailures,
      PlanFragmentStats planFragmentStats,
      final FragmentSubmitSuccess fragmentSubmitSuccess) {

    final InitializeFragments.Builder fb =
        InitializeFragments.newBuilder().setQuerySentTime(System.currentTimeMillis());
    final PlanFragmentSet.Builder setb = fb.getFragmentSetBuilder();

    Set<Integer> majorsAddedSet = new HashSet<>();
    for (final PlanFragmentFull fullFragment : fullFragments) {
      final PlanFragmentMajor major = fullFragment.getMajor();

      // add major info to the msg only once.
      int majorId = fullFragment.getMajorFragmentId();
      if (!majorsAddedSet.contains(majorId)) {
        majorsAddedSet.add(majorId);
        setb.addMajor(major);
      }

      // add minor info.
      setb.addMinor(fullFragment.getMinor());
    }

    if (resourceSchedulingDecisionInfo != null
        && resourceSchedulingDecisionInfo.getQueueId() != null) {
      CoordExecRPC.SchedulingInfo.Builder schedulingInfo =
          CoordExecRPC.SchedulingInfo.newBuilder()
              .setQueueId(resourceSchedulingDecisionInfo.getQueueId());
      if (resourceSchedulingDecisionInfo.getWorkloadClass() != null) {
        schedulingInfo.setWorkloadClass(resourceSchedulingDecisionInfo.getWorkloadClass());
      }
      if (resourceSchedulingDecisionInfo.getExtraInfo() != null) {
        schedulingInfo.setAdditionalInfo(
            ByteString.copyFrom(resourceSchedulingDecisionInfo.getExtraInfo()));
      }
      fb.setSchedulingInfo(schedulingInfo);
    }
    setb.addAllEndpointsIndex(endpointsIndex);
    setb.addAllAttr(sharedAttrs);
    final InitializeFragments initFrags = fb.build();
    planFragmentStats.add(assignment, initFrags);

    logger.debug("Sending remote fragments to \nNode:\n{} \n\nData:\n{}", assignment, initFrags);
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(
            observer,
            assignment,
            initFrags,
            latch,
            fragmentSubmitFailures,
            fragmentSubmitSuccess,
            executionControls,
            injector);

    executorServiceClientFactory
        .getClientForEndpoint(assignment)
        .startFragments(initFrags, listener);
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private void sendActivateFragments(
      final NodeEndpoint assignment, ActivateFragments activateFragments) {
    logger.debug(
        "Sending activate for remote fragments to \nNode:\n{} \n\nData:\n{}",
        assignment,
        activateFragments);
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(
            observer, assignment, activateFragments, null, null, null, executionControls, injector);

    try {
      injector.injectChecked(
          executionControls, INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_ERROR, IllegalStateException.class);
    } catch (IllegalStateException ex) {
      listener.onError(ex);
      return;
    }
    executorServiceClientFactory
        .getClientForEndpoint(assignment)
        .activateFragments(activateFragments, listener);
  }
}
