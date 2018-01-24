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
package com.dremio.exec.work.protector;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.DelegatingAttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;

import io.netty.buffer.ByteBuf;

/**
 * Can re-run a query if needed/possible without the user noticing.
 *
 * Handles anything related to external queryId. An external queryId is a regular queryId but with it's
 * last byte set to 0. This byte is reserved for internal queries to store the mode (fast/protected) and
 * the attempt number
 *
 * Dremio only knows about this class if we are sending/receiving message to/from the client. Everything
 * else uses AttemptManager and QueryId, which is an internal queryId
 *
 * Keeps track of all the info we need to instantiate a new attemptManager
 */
public class Foreman {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);

  // we need all these to start new attemptManager instances if we have to
  private final Executor executor;
  private final SabotContext context;
//  private final ForemenManager manager;
  private final CompletionListener listener;
  private final UserSession session;
  private final UserRequest request;
  private final OptionProvider config;
  private final QueryObserver observer;
  private final ReAttemptHandler attemptHandler;
  private final CoordToExecTunnelCreator tunnelCreator;
  private final Cache<Long, PreparedPlan> plans;

  private AttemptId attemptId; // id of last attempt

  private volatile AttemptManager attemptManager; // last running query


  private volatile boolean canceled; // did the user cancel the query ?

  protected Foreman(
          final SabotContext context,
          final Executor executor,
          final CompletionListener listener,
          final ExternalId externalId,
          final QueryObserver observer,
          final UserSession session,
          final UserRequest request,
          final OptionProvider config,
          final ReAttemptHandler attemptHandler,
          final CoordToExecTunnelCreator tunnelCreator,
          Cache<Long, PreparedPlan> plans) {
    this.attemptId = AttemptId.of(externalId);
    this.executor = executor;
    this.context = context;
    this.listener = listener;
    this.session = session;
    this.request = request;
    this.config = config;
    this.observer = observer;
    this.attemptHandler = attemptHandler;
    this.tunnelCreator = tunnelCreator;
    this.plans = plans;
  }

  public void start() {
    newAttempt(AttemptReason.NONE);
  }

  public void nodesUnregistered(Set<NodeEndpoint> unregistereds){
    AttemptManager manager = attemptManager;
    if(manager != null){
      manager.nodesUnregistered(unregistereds);
    }
  }

  public void nodesRegistered(Set<NodeEndpoint> unregistereds){
    AttemptManager manager = attemptManager;
    if(manager != null){
      manager.nodesRegistered(unregistereds);
    }
  }

  private void newAttempt(AttemptReason reason) {
    // we should ideally check if the query wasn't cancelled before starting a new attempt but this will over-complicate
    // things as the observer expects a query profile at completion and this may not be available if the cancellation
    // is too early
    final Observer attemptObserver = new Observer(observer.newAttempt(attemptId, reason));
    final FragmentsStateListener fragmentsStateListener = new FragmentsTerminationListener(attemptObserver);

    attemptHandler.newAttempt();

    OptionProvider optionProvider = config;
    if (reason != AttemptReason.NONE && attemptHandler.hasOOM()) {
      optionProvider = new LowMemOptionProvider(config);
    }

    attemptManager = newAttemptManager(context, attemptId, request, attemptObserver, session,
      optionProvider, tunnelCreator, plans, fragmentsStateListener);
    executor.execute(attemptManager);
  }

  protected AttemptManager newAttemptManager(SabotContext context, AttemptId attemptId, UserRequest queryRequest,
      AttemptObserver observer, UserSession session, OptionProvider options, CoordToExecTunnelCreator tunnelCreator,
      Cache<Long, PreparedPlan> plans, FragmentsStateListener fragmentsStateListener) {
    final QueryContext queryContext = new QueryContext(session, context, attemptId.toQueryId(),
        queryRequest.getPriority(), queryRequest.getMaxAllocation());
    return new AttemptManager(context, attemptId, queryRequest, observer, options, tunnelCreator, plans,
        queryContext, fragmentsStateListener);
  }

  public void updateStatus(FragmentStatus status) {
    AttemptManager manager = attemptManager;
    if(manager != null && manager.getQueryId().equals(status.getHandle().getQueryId())){
      manager.updateStatus(status);
    }
  }

  public void dataFromScreenArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
    final AttemptManager manager = attemptManager;
    if(manager == null){
      logger.warn("Dropping data from screen, no active attempt manager.");
      return;
    }

    manager.dataFromScreenArrived(header, data, sender);
  }

  public void updateNodeQueryStatus(NodeQueryStatus status) {
    AttemptManager manager = attemptManager;
    if(manager != null && manager.getQueryId().equals(status.getId())){
      manager.updateNodeQueryStatus(status);
    }
  }

  private boolean recoverFromFailure(AttemptReason reason) {
    // request a new attemptId
    attemptId = attemptId.nextAttempt();

    logger.info("{}: Starting new attempt because of {}", attemptId, reason);

    synchronized (this) {
      if (canceled) {
        return false; // no need to run a new attempt
      }
      newAttempt(reason);
    }

    return true;
  }

  public QueryState getState(){
    if(attemptManager == null){
      return null;
    }

    return attemptManager.getState();
  }

  /**
   * Get the currently active profile. Only returns value iff there is a current attempt and it is either starting or running.
   * @return QueryProfile.
   */
  public Optional<QueryProfile> getCurrentProfile(){
    if(attemptManager == null){
      return Optional.absent();
    }

    QueryProfile profile = attemptManager.getQueryProfile();
    QueryState state = attemptManager.getState();

    if (state == QueryState.RUNNING || state == QueryState.STARTING) {
      return Optional.of(profile);
    }

    return Optional.absent();
  }

  public ExternalId getExternalId() {
    return attemptId.getExternalId();
  }

  public synchronized void cancel() {
    if (!canceled) {
      canceled = true;

      if (attemptManager != null) {
        attemptManager.cancel();
      }
    }
  }

  public synchronized void resume() {
    if (attemptManager != null) {
      attemptManager.resume();
    }
  }

  // checks if the plan (after physical transformation) contains hash aggregate
  private static boolean containsHashAggregate(final RelNode relNode) {
    if (relNode instanceof HashAggPrel) {
      return true;
    }
    else {
      for (final RelNode child : relNode.getInputs()) {
        if (containsHashAggregate(child)) {
          return true;
        } // else, continue
      }
    }
    return false;
  }

  private class FragmentsTerminationListener implements FragmentsStateListener {

    private final Observer observer;
    public FragmentsTerminationListener(Observer observer) {
      this.observer = observer;
    }

    @Override
    public void allFragmentsRetired() {
      observer.allFragmentsRetired();
    }
  }

  private class Observer extends DelegatingAttemptObserver {

    private boolean containsHashAgg = false;
    private volatile UserResult userResult = null;
    private final AtomicInteger announceArrival = new AtomicInteger(0);
    boolean isCompleteAfterAllReports = false;
    private static final int INITIAL = 0;
    private static final int FRAGMENTS_RETIRED_INVOKED = 1;
    private static final int ATTEMPT_COMPLETION_INVOKED = 2;

    Observer(final AttemptObserver delegate) {
      super(delegate);
    }

    @Override
    public void execDataArrived(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
      try {
        // any failures here should notify the listener and release the batches
        result = attemptHandler.convertIfNecessary(result);
      } catch (Exception ex) {
        outcomeListener.failed(RpcException.mapException(ex));
        for (ByteBuf byteBuf : result.getBuffers()) {
          byteBuf.release();
        }
        return;
      }

      super.execDataArrived(outcomeListener, result);
    }

    @Override
    public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
      if (phase == PlannerPhase.PHYSICAL) {
        containsHashAgg = containsHashAggregate(after);
      }
      super.planRelTransform(phase, planner, before, after, millisTaken);
    }

    /*
     * Design Notes from DX-9802:
     *
     * WorkFlow:
     *
     *   Query didn't fail
     *      - no change
     *   Query failed but then we saw it cancelled
     *      - no change
     *   Query failed and was not cancelled and attempt handler didn't allow reattempt
     *      - no change
     *   Query failed and was not cancelled and reattempt reason was not OOM
     *      - no change (attemptCompletion does reattempt)
     *   Query failed and was not cancelled and reattempt reason was OOM
     *      - this is where design has changed as described below
     *
     * Reattempts due to OOM failures are handled when all fragments
     * are retired. So OOM reattempt is issued in allFragmentsRetired()
     * callback and attemptCompletion does the job of
     * determining the reattempt reason to be OOM through attempthandler.
     * This is the simplest case where attemptCompletion is
     * called before allFragmentsRetired.
     *
     * However, there could be a case (concurrency) where allFragmentsRetired
     * callback was invoked before (or at the same time) as
     * attemptCompletion. So the interleaving is not predictable
     * and these two callbacks should account for that and correctly issue
     * OOM reattempt at most once.
     *
     * As an example, if allFragmentsRetired was called first then
     * it still can't issue an OOM reattempt because the decision
     * of whether OOM reattempt is needed or not lies with
     * attemptCompletion and the latter should issue the
     * reattempt if it knows allFragmentsRetired has already been called.
     *
     * To handle such cases we use atomic integer to implement a lock
     * and communication mechanism between the two functions in case
     * there is no clear happens-before relationship between the two.
     *
     * The lock can hold values {0, 1, 2} and they are checked to
     * implement serialization.
     *
     * 0 - INITIAL
     * 1 - FRAGMENTS_RETIRED_INVOKED
     * 2 - ATTEMPT_COMPLETION_INVOKED
     *
     * allFragmentsRetired() will set the value to 1 announcing it's
     * arrival and grabbing a virtual lock.
     *
     * attemptCompletion will set the value to 2 announcing it's
     * arrival and grabbing a virtual lock.
     *
     * Whoever doesn't grab the virtual lock will issue the OOM reattempt.
     * This works for all cases of interleaving.
     */
    @Override
    public void attemptCompletion(UserResult result) {
      attemptManager = null; // make sure we don't pass cancellation requests to this attemptManager anymore
      userResult = result;

      final QueryState queryState = result.getState();
      final boolean queryFailed = queryState == QueryState.FAILED;

      /* if the query failed we may be able to recover from it
       * if it wasn't cancelled and the attemptHandler allows
       * the reattempt.
       */
      if (queryFailed) {
        if (canceled) {
          logger.info("{}: cannot re-attempt the query, user already cancelled it", attemptId);
        } else {
          final AttemptReason reason = attemptHandler.isRecoverable(
            new ReAttemptContext(attemptId, result.getException(), containsHashAgg));
          if (reason != AttemptReason.NONE) {
            super.attemptCompletion(userResult);
            if (reason != AttemptReason.OUT_OF_MEMORY) {
              /* attempt reason is not OOM -- no change in the flow */
              if (issueReattempt(reason)) {
                return;
              }
            } else {
              isCompleteAfterAllReports = true;
              final int oldValue = announceArrival.getAndSet(ATTEMPT_COMPLETION_INVOKED);
              if (oldValue == FRAGMENTS_RETIRED_INVOKED) {
                /* issue OOM reattempt */
                if (issueReattempt(reason)) {
                  return;
                }
              }
            }
          }
        }
      }

      if (!isCompleteAfterAllReports) {
        /* no more attempts are needed/possible. this is conditional since
         * if the OOM reattempt is done by allFragmentsRetired() then it will
         * take care (if needed) of calling completion on query observer and
         * listener.
         */
        observer.execCompletion(userResult);
        listener.completed();
      }
    }

    /*
     * In attemptCompletion we had already checked that query failure
     * was recoverable and the reason was OOM. If the reason was not OOM
     * then this function is a NOOP because the reattempt would have already
     * been issued by attemptCompletion. This callback will be invoked
     * by QueryManager instance corresponding to the AttemptManager instance
     * Foreman is working with.
     */
    public void allFragmentsRetired() {
      final int oldValue = announceArrival.getAndSet(FRAGMENTS_RETIRED_INVOKED);
      if (oldValue != INITIAL) {
        /* issue OOM reattempt */
        if (issueReattempt(AttemptReason.OUT_OF_MEMORY)) {
          return;
        }
        observer.execCompletion(userResult);
        listener.completed();
      }
    }

    private boolean issueReattempt(AttemptReason reason) {
      /* this is required so that if we return false from here,
       * attempt can be marked completed on query observer and listener
       * in attemptCompletion
       */
      isCompleteAfterAllReports = false;
      try {
        if (recoverFromFailure(reason)) {
          return true;
        }
      } catch (Exception e) {
        logger.error("{}: something went wrong when re-attempting the query", attemptId.getExternalId(), e);
      }
      return false;
    }
  }

  private static class LowMemOptionProvider implements OptionProvider {

    private final OptionProvider optionProvider;

    LowMemOptionProvider(OptionProvider optionProvider) {
      this.optionProvider = optionProvider;
    }

    @Override
    public void applyOptions(OptionManager manager) {
      if (optionProvider != null) {
        optionProvider.applyOptions(manager);
      }
      // TODO(DX-5912): disable hash join after merge join is implemented
      // manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY,
      //    PlannerSettings.HASHJOIN.getOptionName(), false));
      manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY,
        PlannerSettings.HASHAGG.getOptionName(), false));
    }
  }
}
