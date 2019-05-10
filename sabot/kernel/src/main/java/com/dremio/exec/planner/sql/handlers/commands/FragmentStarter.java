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
package com.dremio.exec.planner.sql.handlers.commands;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PlanFragmentStats;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.work.EndpointListener;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;

/**
 * Class used to start remote fragment execution.
 */
class FragmentStarter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStarter.class);

  private static final long RPC_WAIT_IN_MSECS_PER_FRAGMENT = 5000;

  private final CoordToExecTunnelCreator tunnelCreator;
  private final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;
  private AttemptObserver observer = null;

  public FragmentStarter(CoordToExecTunnelCreator tunnelCreator,
                         ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    this.tunnelCreator = tunnelCreator;
    this.resourceSchedulingDecisionInfo = resourceSchedulingDecisionInfo;
  }

  public void start(ExecutionPlan plan, AttemptObserver observer) throws Exception {
    Preconditions.checkNotNull(observer, "observer should not be null.");
    startFragments(plan, observer);
  }

  /**
   * Set up the fragments for execution. Some may be local, and some may be remote.
   * Messages are sent immediately, so they may start returning data even before we complete this.
   *
   * @param plan the execution plan
   * @throws ForemanException
   */
  protected void startFragments(ExecutionPlan plan, AttemptObserver observer) throws ForemanException {
    final Collection<PlanFragmentFull> fullFragments = plan.getFragments();
    if (fullFragments.isEmpty()) {
      // nothing to do here
      return;
    }
    /*
     * We will send a single message to each endpoint, regardless of how many fragments will be
     * executed there. We need to start up the intermediate fragments first so that they will be
     * ready once the leaf fragments start producing data. To satisfy both of these, we will
     * make a pass through the fragments and put them into these two maps according to their
     * leaf/intermediate state, as well as their target node.
     */
    final Multimap<NodeEndpoint, PlanFragmentFull> leafFragmentMap = ArrayListMultimap.create();
    final Multimap<NodeEndpoint, PlanFragmentFull> intFragmentMap = ArrayListMultimap.create();

    // record all fragments for status purposes.
    for (final PlanFragmentFull fragmentFull : fullFragments) {
      final PlanFragmentMajor major = fragmentFull.getMajor();

      if (logger.isTraceEnabled()) {
        // major.getFragmentJson() might be costly (internal ByteString <-> String conversion)
        try {
          logger.trace("Tracking intermediate remote node {} with data {}",
              fragmentFull.getAssignment(),
              PhysicalPlanReader.toString(major.getFragmentJson(), major.getFragmentCodec()));
        } catch (IOException e) {
          logger.warn("Error when trying to read fragment", e);
        }
      }
      if (major.getLeafFragment()) {
        leafFragmentMap.put(fragmentFull.getAssignment(), fragmentFull);
      } else {
        intFragmentMap.put(fragmentFull.getAssignment(), fragmentFull);
      }
    }

    /*
     * We need to wait for the intermediates to be sent so that they'll be set up by the time
     * the leaves start producing data. We'll use this latch to wait for the responses.
     *
     * However, in order not to hang the process if any of the RPC requests fails, we always
     * count down (see FragmentSubmitFailures), but we count the number of failures so that we'll
     * know if any submissions did fail.
     */
    final int numIntFragments = intFragmentMap.keySet().size();
    final ExtendedLatch endpointLatch = new ExtendedLatch(numIntFragments);
    final FragmentSubmitFailures fragmentSubmitFailures = new FragmentSubmitFailures();
    final List<NodeEndpoint> endpointsIndex = plan.getIndexBuilder().getEndpointsIndexBuilder().getAllEndpoints();

    PlanFragmentStats stats = new PlanFragmentStats();
    Stopwatch stopwatch = Stopwatch.createStarted();
    // send remote intermediate fragments
    for (final NodeEndpoint ep : intFragmentMap.keySet()) {
      sendRemoteFragments(ep, intFragmentMap.get(ep), endpointsIndex, endpointLatch, fragmentSubmitFailures, stats);
    }

    final long timeout = RPC_WAIT_IN_MSECS_PER_FRAGMENT * numIntFragments;
    if(numIntFragments > 0 && !endpointLatch.awaitUninterruptibly(timeout)){
      long numberRemaining = endpointLatch.getCount();
      throw UserException.connectionError()
          .message(
              "Exceeded timeout (%d) while waiting send intermediate work fragments to remote nodes. " +
                  "Sent %d and only heard response back from %d nodes.",
              timeout, numIntFragments, numIntFragments - numberRemaining)
          .build(logger);
    }
    stopwatch.stop();
    observer.intermediateFragmentScheduling(stopwatch.elapsed(TimeUnit.MILLISECONDS), stats.getSummary());

    // if any of the intermediate fragment submissions failed, fail the query
    final List<FragmentSubmitFailures.SubmissionException> submissionExceptions = fragmentSubmitFailures.submissionExceptions;
    if (submissionExceptions.size() > 0) {
      Set<NodeEndpoint> endpoints = Sets.newHashSet();
      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (FragmentSubmitFailures.SubmissionException e : fragmentSubmitFailures.submissionExceptions) {
        NodeEndpoint endpoint = e.nodeEndpoint;
        if (endpoints.add(endpoint)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(endpoint.getAddress());
        }
      }
      throw UserException.connectionError(submissionExceptions.get(0).rpcException)
          .message("Error setting up remote intermediate fragment execution")
          .addContext("Nodes with failures", sb.toString())
          .build(logger);
    }

    stats = new PlanFragmentStats();
    stopwatch.reset();
    stopwatch.start();
    this.observer = observer;
    /*
     * Send the remote (leaf) fragments; we don't wait for these. Any problems will come in through
     * the regular sendListener event delivery.
     */
    for (final NodeEndpoint ep : leafFragmentMap.keySet()) {
      sendRemoteFragments(ep, leafFragmentMap.get(ep), endpointsIndex,
        null, null, stats);
    }
    stopwatch.stop();
    // No waiting on acks of sent leaf fragments; so this number is not be reliable
    observer.leafFragmentScheduling(stopwatch.elapsed(TimeUnit.MILLISECONDS), stats.getSummary());
  }

  /**
   * Send all the remote fragments belonging to a single target node in one request.
   *
   * @param assignment the node assigned to these fragments
   * @param fullFragments the set of fragments
   * @param latch the countdown latch used to track the requests to all endpoints
   * @param fragmentSubmitFailures the submission failure counter used to track the requests to all endpoints
   */
  private void sendRemoteFragments(final NodeEndpoint assignment, final Collection<PlanFragmentFull> fullFragments,
      List<NodeEndpoint> endpointsIndex, final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures,
      PlanFragmentStats planFragmentStats) {

    final InitializeFragments.Builder fb = InitializeFragments.newBuilder();
    final PlanFragmentSet.Builder setb = fb.getFragmentSetBuilder();

    Set<Integer> majorsAddedSet = new HashSet<>();
    for(final PlanFragmentFull fullFragment : fullFragments) {
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

    if (resourceSchedulingDecisionInfo != null && resourceSchedulingDecisionInfo.getQueueId() != null) {
      CoordExecRPC.SchedulingInfo.Builder schedulingInfo =
        CoordExecRPC.SchedulingInfo.newBuilder().setQueueId(resourceSchedulingDecisionInfo.getQueueId());
      if (resourceSchedulingDecisionInfo.getWorkloadClass() != null) {
        schedulingInfo.setWorkloadClass(resourceSchedulingDecisionInfo.getWorkloadClass());
      }
      if (resourceSchedulingDecisionInfo.getExtraInfo() != null) {
        schedulingInfo.setAdditionalInfo(ByteString.copyFrom(resourceSchedulingDecisionInfo.getExtraInfo()));
      }
      fb.setSchedulingInfo(schedulingInfo);
    }
    setb.addAllEndpointsIndex(endpointsIndex);
    final InitializeFragments initFrags = fb.build();
    planFragmentStats.add(assignment, initFrags);

    logger.debug("Sending remote fragments to \nNode:\n{} \n\nData:\n{}", assignment, initFrags);
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(assignment, initFrags, latch, fragmentSubmitFailures);
    tunnelCreator.getTunnel(assignment).sendFragments(listener, initFrags);
  }

  /**
   * Used by {@link FragmentSubmitListener} to track the number of submission failures.
   */
  private static class FragmentSubmitFailures {
    static class SubmissionException {
      final NodeEndpoint nodeEndpoint;
      final RpcException rpcException;

      SubmissionException(
          final NodeEndpoint nodeEndpoint,
          final RpcException rpcException) {
        this.nodeEndpoint = nodeEndpoint;
        this.rpcException = rpcException;
      }
    }

    final List<SubmissionException> submissionExceptions = Collections.synchronizedList(new LinkedList<>());

    void addFailure(final NodeEndpoint nodeEndpoint, final RpcException rpcException) {
      submissionExceptions.add(new SubmissionException(nodeEndpoint, rpcException));
    }
  }

  private class FragmentSubmitListener extends EndpointListener<Ack, InitializeFragments> {
    private final CountDownLatch latch;
    private final FragmentSubmitFailures fragmentSubmitFailures;

    /**
     * Constructor.
     *
     * @param endpoint the endpoint for the submission
     * @param value the initialize fragments message
     * @param latch the latch to count down when the status is known; may be null
     * @param fragmentSubmitFailures the counter to use for failures; must be non-null iff latch is non-null
     */
    public FragmentSubmitListener(final NodeEndpoint endpoint, final InitializeFragments value,
        final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures) {
      super(endpoint, value);
      Preconditions.checkState((latch == null) == (fragmentSubmitFailures == null));
      this.latch = latch;
      this.fragmentSubmitFailures = fragmentSubmitFailures;
    }

    @Override
    public void success(final Ack ack, final ByteBuf byteBuf) {
      if (latch != null) {
        latch.countDown();
      }
    }

    @Override
    public void failed(final RpcException ex) {
      if (latch != null) { // this block only applies to intermediate fragments
        fragmentSubmitFailures.addFailure(endpoint, ex);
        latch.countDown();
      } else { // this block only applies to leaf fragments
        observer.startLeafFragmentFailed(new RpcException(String.format("Failure sending leaf fragment to %s:%d.", endpoint.getAddress(), endpoint.getFabricPort()), ex));
      }
    }

    @Override
    public void interrupted(final InterruptedException e) {
      // AttemptManager shouldn't get interrupted while waiting for the RPC outcome of fragment submission.
      // Consider the interrupt as failure.
      final String errMsg = "Interrupted while waiting for the RPC outcome of fragment submission.";
      logger.error(errMsg, e);
      failed(new RpcException(errMsg, e));
    }
  }




}
