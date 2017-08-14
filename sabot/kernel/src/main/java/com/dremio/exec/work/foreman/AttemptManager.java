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
package com.dremio.exec.work.foreman;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.codehaus.jackson.map.ObjectMapper;

import com.dremio.common.CatastrophicFailure;
import com.dremio.common.EventProcessor;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.BackwardsCompatObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.commands.AsyncCommand;
import com.dremio.exec.planner.sql.handlers.commands.AsyncCommand.QueueType;
import com.dremio.exec.planner.sql.handlers.commands.CommandCreator;
import com.dremio.exec.planner.sql.handlers.commands.CommandRunner;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.DistributedSemaphore.DistributedLease;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.OutOfDirectMemoryError;

/**
 * AttemptManager manages all the fragments (local and remote) for a single query where this
 * is the driving node.
 *
 * The flow is as follows:
 * - AttemptManager is submitted as a runnable.
 * - Runnable does query planning.
 * - state changes from PENDING to RUNNING
 * - Status listener are activated
 * - Runnable sends out starting fragments
 * - The Runnable's run() completes, but the AttemptManager stays around
 * - AttemptManager listens for state change messages.
 * - state change messages can drive the state to FAILED or CANCELED, in which case
 *   messages are sent to running fragments to terminate
 * - when all fragments complete, state change messages drive the state to COMPLETED
 */
public class AttemptManager implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AttemptManager.class);
  private static final org.slf4j.Logger QUERY_LOGGER = org.slf4j.LoggerFactory.getLogger("query.logger");
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(AttemptManager.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final AttemptId attemptId;
  private final QueryId queryId;
  private final String queryIdString;
  private final UserRequest queryRequest;
  private final QueryContext queryContext;
  private final BufferAllocator backwardCompatAllocator;
  private final QueryManager queryManager; // handles lower-level details of query execution
  private final SabotContext sabotContext;
  private final Cache<Long, PreparedPlan> plans;
  private volatile QueryState state;

  private volatile DistributedLease lease; // used to limit the number of concurrent queries

  private final StateSwitch stateSwitch = new StateSwitch();
  private final AttemptResult foremanResult = new AttemptResult();
  private final boolean queuingEnabled;
  private Object extraResultData;
  private final CoordToExecTunnelCreator tunnelCreator;
  private final AttemptObservers observers;
  private final Pointer<QueryId> prepareId;
  private String commandDescription = "<unknown>";

  /**
   * Constructor. Sets up the AttemptManager, but does not initiate any execution.
   *
   * @param attemptId the id for the query
   * @param queryRequest the query to execute
   */
  public AttemptManager(
      final SabotContext context,
      final AttemptId attemptId,
      final UserRequest queryRequest,
      final AttemptObserver observer,
      final UserSession session,
      final OptionProvider options,
      final CoordToExecTunnelCreator tunnelCreator,
      final Cache<Long, PreparedPlan> plans
      ) {
    this.attemptId = attemptId;
    this.queryId = attemptId.toQueryId();
    this.queryIdString = QueryIdHelper.getQueryId(queryId);
    this.queryRequest = queryRequest;
    this.sabotContext = context;
    this.tunnelCreator = tunnelCreator;
    this.plans = plans;
    this.prepareId = new Pointer<>();

    final QueryPriority priority = queryRequest.getPriority();
    final long maxAllocation = queryRequest.getMaxAllocation();
    this.queryContext = new QueryContext(session, sabotContext, queryId, priority, maxAllocation);
    this.backwardCompatAllocator = queryContext.getAllocator()
      .newChildAllocator("backward-compatibility", 0, Long.MAX_VALUE);
    this.observers = AttemptObservers.of(BackwardsCompatObserver.wrapIfOld(session, observer, backwardCompatAllocator));
    this.queryManager = new QueryManager(queryId, queryContext, new CompletionListenerImpl(), prepareId,
      observers, context.getOptionManager().getOption(PlannerSettings.VERBOSE_PROFILE), queryContext.getSchemaTreeProvider());

    final OptionManager optionManager = queryContext.getOptions();
    if(options != null){
      options.applyOptions(optionManager);
    }
    this.queuingEnabled = optionManager.getOption(ExecConstants.ENABLE_QUEUE);

    final QueryState initialState = queuingEnabled ? QueryState.ENQUEUED : QueryState.STARTING;
    recordNewState(initialState);
  }


  private class CompletionListenerImpl implements CompletionListener {

    @Override
    public void succeeded() {
      addToEventQueue(QueryState.COMPLETED, null);
    }

    @Override
    public void failed(Exception ex) {
      addToEventQueue(QueryState.FAILED, ex);
    }

  }
  public QueryId getQueryId(){
    return queryId;
  }

  public QueryState getState(){
    return state;
  }

  public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes){
    queryManager.getNodeStatusListener().nodesUnregistered(unregisteredNodes);
  }

  public void nodesRegistered(Set<NodeEndpoint> registeredNodes){
    queryManager.getNodeStatusListener().nodesRegistered(registeredNodes);
  }

  public void dataFromScreenArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
    if(data != null){
      // we're going to send this some place, we need increment to ensure this is around long enough to send.
      data.retain();
      observers.execDataArrived(new ScreenShuttle(sender), new QueryWritableBatch(header, data));
    } else {
      observers.execDataArrived(new ScreenShuttle(sender), new QueryWritableBatch(header));
    }

  }

  /**
   * Shuttles acknowledgments back to screen fragment as user acks messages.
   * Fragments therefore cannot complete until the acknowledgement is shuttled
   * back to the sending node.
   */
  private class ScreenShuttle implements RpcOutcomeListener<Ack>{
    private final ResponseSender sender;

    public ScreenShuttle(ResponseSender sender) {
      this.sender = sender;
    }

    @Override
    public void failed(RpcException paramRpcException) {
      ackit();
      addToEventQueue(QueryState.FAILED, paramRpcException);
    }

    @Override
    public void interrupted(InterruptedException paramInterruptedException) {
      ackit();
      addToEventQueue(QueryState.CANCELED, paramInterruptedException);
    }

    @Override
    public void success(Ack paramV, ByteBuf paramByteBuf) {
      ackit();
    }

    private void ackit(){
      // No matter what, we ack back to the original node. Since we manage
      // execution, we can cancel on failure from here. We don't ack until we've
      // passed data through, ensuring that we have full backpressure.
      sender.send(new Response(RpcType.ACK, Acks.OK));
    }
  }

  public void updateStatus(FragmentStatus status) {
    queryManager.getFragmentStatusListener().statusUpdate(status);
  }

  public QueryProfile getQueryProfile() {
    return queryManager.getQueryProfile(queryRequest.getDescription(), state, null);
  }

  /**
   * Cancel the query. Asynchronous -- it may take some time for all remote fragments to be
   * terminated.
   */
  public void cancel() {
    // Note this can be called from outside of run() on another thread, or after run() completes
    addToEventQueue(QueryState.CANCELED, null);
  }

  /**
   * Called by execution pool to do query setup, and kick off remote execution.
   *
   * <p>Note that completion of this function is not necessarily the end of the AttemptManager's role
   * in the query's lifecycle.
   */
  @Override
  public void run() {
    // rename the thread we're using for debugging purposes
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(queryIdString + ":foreman");


    try {
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-beginning", ForemanException.class);

      observers.queryStarted(queryRequest, queryContext.getSession().getCredentials().getUserName());

      CommandCreator creator = newCommandCreator(queryContext, observers, prepareId);
      CommandRunner<?> command = creator.toCommand();
      logger.debug("Using command: {}.", command);

      switch(command.getCommandType()){
      case ASYNC_QUERY:
        Preconditions.checkState(command instanceof AsyncCommand, "Asynchronous query must be an AsyncCommand");
        command.plan();
        acquireQuerySemaphoreIfNecessary(((AsyncCommand) command).getQueueType());
        if(queuingEnabled){
          moveToState(QueryState.STARTING, null);
        }
        extraResultData = command.execute();
        break;

      case SYNC_QUERY:
        if (queuingEnabled) {
          moveToState(QueryState.STARTING, null);
        }
        command.plan();
        extraResultData = command.execute();
        addToEventQueue(QueryState.COMPLETED, null);
        break;

      case SYNC_RESPONSE:
        if (queuingEnabled) {
          moveToState(QueryState.STARTING, null);
        }
        command.plan();
        extraResultData = command.execute();
        addToEventQueue(QueryState.COMPLETED, null);
        break;

      default:
        throw new IllegalStateException("unhandled command type.");
      }
      commandDescription = command.getDescription();

      moveToState(QueryState.RUNNING, null);

      injector.injectChecked(queryContext.getExecutionControls(), "run-try-end", ForemanException.class);
    } catch (final UserException | ForemanException e) {
      moveToState(QueryState.FAILED, e);
    } catch (final OutOfMemoryError e) {
      if (e instanceof OutOfDirectMemoryError || "Direct buffer memory".equals(e.getMessage())) {
        moveToState(QueryState.FAILED, UserException.memoryError(e).build(logger));
      } else {
        /*
         * FragmentExecutors use a NodeStatusListener to watch out for the death of their query's AttemptManager. So, if we
         * die here, they should get notified about that, and cancel themselves; we don't have to attempt to notify
         * them, which might not work under these conditions.
         */
        CatastrophicFailure.exit(e, "Unable to handle out of memory condition in AttemptManager.", -1);
      }
    } catch (Throwable ex) {
      moveToState(QueryState.FAILED,
          new ForemanException("Unexpected exception during fragment initialization: " + ex.getMessage(), ex));

    } finally {
      /*
       * Begin accepting external events.
       *
       * Doing this here in the finally clause will guarantee that it occurs. Otherwise, if there
       * is an exception anywhere during setup, it wouldn't occur, and any events that are generated
       * as a result of any partial setup that was done (such as the FragmentSubmitListener,
       * the ResponseSendListener, or an external call to cancel()), will hang the thread that makes the
       * event delivery call.
       *
       * If we do throw an exception during setup, and have already moved to QueryState.FAILED, we just need to
       * make sure that we can't make things any worse as those events are delivered, but allow
       * any necessary remaining cleanup to proceed.
       *
       * Note that cancellations cannot be simulated before this point, i.e. pauses can be injected, because AttemptManager
       * would wait on the cancelling thread to signal a resume and the cancelling thread would wait on the AttemptManager
       * to accept events.
       */
      try {
        stateSwitch.start();
      } catch (Exception e) {
        moveToState(QueryState.FAILED, e);
      }

      // restore the thread's original name
      currentThread.setName(originalName);
    }

    /*
     * Note that despite the run() completing, the AttemptManager could continue to exist, and receives
     * events about fragment completions. It won't go away until everything is completed, failed, or cancelled.
     */
  }

  protected CommandCreator newCommandCreator(QueryContext queryContext, AttemptObserver observer, Pointer<QueryId> prepareId) {
    return new CommandCreator(this.sabotContext, queryContext, tunnelCreator, queryRequest,
      observer, plans, prepareId, attemptId.getAttemptNum());
  }

  private void releaseLease() {
    while (lease != null) {
      try {
        lease.close();
        lease = null;
      } catch (final InterruptedException e) {
        // if we end up here, the while loop will try again
      } catch (final Exception e) {
        logger.warn("Failure while releasing lease.", e);
        break;
      }
    }
  }

//  private void log(final PhysicalPlan plan) {
//    if (logger.isDebugEnabled()) {
//      try {
//        final String planText = queryContext.getLpPersistence().getMapper().writeValueAsString(plan);
//        logger.debug("Physical {}", planText);
//      } catch (final IOException e) {
//        logger.warn("Error while attempting to log physical plan.", e);
//      }
//    }
//  }

  /**
   * Manages the end-state processing for AttemptManager.
   *
   * End-state processing is tricky, because even if a query appears to succeed, but
   * we then encounter a problem during cleanup, we still want to mark the query as
   * failed. So we have to construct the successful result we would send, and then
   * clean up before we send that result, possibly changing that result if we encounter
   * a problem during cleanup. We only send the result when there is nothing left to
   * do, so it will account for any possible problems.
   *
   * The idea here is to make close()ing the ForemanResult do the final cleanup and
   * sending. Closing the result must be the last thing that is done by AttemptManager.
   */
  private class AttemptResult implements AutoCloseable {
    private QueryState resultState = null;
    private volatile Exception resultException = null;
    private boolean isClosed = false;

    /**
     * Set up the result for a COMPLETED or CANCELED state.
     *
     * <p>Note that before sending this result, we execute cleanup steps that could
     * result in this result still being changed to a FAILED state.
     *
     * @param queryState one of COMPLETED or CANCELED
     */
    public void setCompleted(final QueryState queryState) {
      Preconditions.checkArgument((queryState == QueryState.COMPLETED) || (queryState == QueryState.CANCELED));
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);

      resultState = queryState;
    }

    /**
     * Set up the result for a FAILED state.
     *
     * <p>Failures that occur during cleanup processing will be added as suppressed
     * exceptions.
     *
     * @param exception the exception that led to the FAILED state
     */
    public void setFailed(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);

      resultState = QueryState.FAILED;
      resultException = exception;
    }

    /**
     * Ignore the current status and force the given failure as current status.
     * NOTE: Used only for testing purposes. Shouldn't be used in production.
     */
    public void setForceFailure(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);

      resultState = QueryState.FAILED;
      resultException = exception;
    }

    /**
     * Add an exception to the result. All exceptions after the first become suppressed
     * exceptions hanging off the first.
     *
     * @param exception the exception to add
     */
    private void addException(final Exception exception) {
      Preconditions.checkNotNull(exception);

      if (resultException == null) {
        resultException = exception;
      } else {
        resultException.addSuppressed(exception);
      }
    }

    /**
     * Close the given resource, catching and adding any caught exceptions via {@link #addException(Exception)}. If an
     * exception is caught, it will change the result state to FAILED, regardless of what its current value.
     *
     * @param autoCloseable
     *          the resource to close
     */
    private void suppressingClose(final AutoCloseable autoCloseable) {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      if (autoCloseable == null) {
        return;
      }

      try {
        autoCloseable.close();
      } catch(final Exception e) {
        /*
         * Even if the query completed successfully, we'll still report failure if we have
         * problems cleaning up.
         */
        resultState = QueryState.FAILED;
        addException(e);
      }
    }

    private void logQuerySummary() {
      try {
        LoggedQuery q = new LoggedQuery(
            queryIdString,
            queryContext.getQueryContextInfo().getDefaultSchemaName(),
            queryRequest.getDescription(),
            new Date(queryContext.getQueryContextInfo().getQueryStartTime()),
            new Date(System.currentTimeMillis()),
            state,
            queryContext.getSession().getCredentials().getUserName(),
            commandDescription);
        QUERY_LOGGER.info(MAPPER.writeValueAsString(q));
      } catch (Exception e) {
        logger.error("Failure while recording query information to query log.", e);
      }
    }

    @Override
    public void close() {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      // to track how long the query takes
      queryManager.markEndTime();

      logger.debug(queryIdString + ": cleaning up.");
      injector.injectPause(queryContext.getExecutionControls(), "foreman-cleanup", logger);

      // log the query summary
      logQuerySummary();

      sabotContext.getClusterCoordinator()
          .getServiceSet(ClusterCoordinator.Role.EXECUTOR)
          .removeNodeStatusListener(queryManager.getNodeStatusListener());

      suppressingClose(backwardCompatAllocator);
      suppressingClose(queryContext);

      /*
       * We do our best to write the latest state, but even that could fail. If it does, we can't write
       * the (possibly newly failing) state, so we continue on anyway.
       *
       * We only need to do this if the resultState differs from the last recorded state
       */
      if (resultState != state) {
        recordNewState(resultState);
      }

      final UserException uex;
      if (resultException != null) {
        uex = UserException.systemError(resultException).addIdentity(queryContext.getCurrentEndpoint()).build(logger);
      } else {
        uex = null;
      }

      /*
       * If sending the result fails, we don't really have any way to modify the result we tried to send;
       * it is possible it got sent but the result came from a later part of the code path. It is also
       * possible the connection has gone away, so this is irrelevant because there's nowhere to
       * send anything to.
       */
      try {
        // send whatever result we ended up with
        final UserResult result = new UserResult(extraResultData, queryId, resultState,
          queryManager.getQueryProfile(queryRequest.getDescription(), state, uex), uex);
        observers.attemptCompletion(result);
      } catch(final Exception e) {
        addException(e);
        logger.warn("Exception sending result to client", resultException);
      }

      try {
        releaseLease();
      } finally {
        isClosed = true;
      }
    }
  }

  private static class StateEvent {
    final QueryState newState;
    final Exception exception;

    StateEvent(final QueryState newState, final Exception exception) {
      this.newState = newState;
      this.exception = exception;
    }
  }

  /**
   * Tells the foreman to move to a new state.
   *
   * @param newState the state to move to
   * @param exception if not null, the exception that drove this state transition (usually a failure)
   */
  private void moveToState(final QueryState newState, final Exception exception) {
    logger.debug(queryIdString + ": State change requested {} --> {}", state, newState,
      exception);
    switch (state) {
      case ENQUEUED:
        switch (newState) {
          case FAILED:
            Preconditions.checkNotNull(exception, "exception cannot be null when new state is failed");
            recordNewState(newState);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;
          case STARTING:
            recordNewState(newState);
            observers.execStarted(queryManager.getQueryProfile(queryRequest.getDescription(), newState, null));
            return;
        }
        break;
      case STARTING:
        if (newState == QueryState.RUNNING) {
          recordNewState(QueryState.RUNNING);
          return;
        }

        //$FALL-THROUGH$

      case RUNNING: {
        /*
         * For cases that cancel executing fragments, we have to record the new
         * state first, because the cancellation of the local root fragment will
         * cause this to be called recursively.
         */
        switch (newState) {
          case CANCELED: {
            assert exception == null;
            recordNewState(QueryState.CANCELED);
            queryManager.cancelExecutingFragments(tunnelCreator);
            foremanResult.setCompleted(QueryState.CANCELED);
          /*
           * We don't close the foremanResult until we've gotten
           * acknowledgements, which happens below in the case for current state
           * == CANCELLATION_REQUESTED.
           */
            return;
          }

          case COMPLETED: {
            assert exception == null;
            recordNewState(QueryState.COMPLETED);
            foremanResult.setCompleted(QueryState.COMPLETED);
            foremanResult.close();
            return;
          }

          case FAILED: {
            assert exception != null;
            recordNewState(QueryState.FAILED);
            queryManager.cancelExecutingFragments(tunnelCreator);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;
          }

        }
        break;
      }

      case CANCELED:
        if ((newState == QueryState.CANCELED)
          || (newState == QueryState.COMPLETED)
          || (newState == QueryState.FAILED)) {

          if (sabotContext.getConfig().getBoolean(ExecConstants.RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS)) {
            if (newState == QueryState.FAILED) {
              assert exception != null;
              recordNewState(QueryState.FAILED);
              foremanResult.setForceFailure(exception);
            }
          }
          /*
           * These amount to a completion of the cancellation requests' cleanup;
           * now we can clean up and send the result.
           */
          foremanResult.close();
        }
        return;

      case COMPLETED:
      case FAILED:
        logger
          .warn(
            "Dropping request to move to {} state as query is already at {} state (which is terminal).",
            newState, state);
        return;
    }

    throw new IllegalStateException(String.format(
      "Failure trying to change states: %s --> %s", state.name(),
      newState.name()));
  }

  private class StateSwitch extends EventProcessor<StateEvent> {

    void addEvent(final QueryState newState, final Exception exception) {
      sendEvent(new StateEvent(newState, exception));
    }

    @Override
    protected void processEvent(StateEvent event) {
      moveToState(event.newState, event.exception);
    }
  }

  void addToEventQueue(final QueryState newState, final Exception exception) {
    stateSwitch.addEvent(newState, exception);
  }

  public void fail(Exception exception) {
    moveToState(QueryState.FAILED, exception);
  }

  private void recordNewState(final QueryState newState) {
    state = newState;
  }

  private void acquireQuerySemaphoreIfNecessary(QueueType queueType) throws ForemanSetupException {
    if(!queuingEnabled){
      return;
    }

    final OptionManager optionManager = queryContext.getOptions();
    final long queueThreshold = optionManager.getOption(ExecConstants.QUEUE_THRESHOLD_SIZE);

    final long queueTimeout = optionManager.getOption(ExecConstants.QUEUE_TIMEOUT);
    final String queueName;

    try {
      @SuppressWarnings("resource")
      final ClusterCoordinator clusterCoordinator = sabotContext.getClusterCoordinator();
      final DistributedSemaphore distributedSemaphore;

      // get the appropriate semaphore
      switch (queueType) {
        case LARGE:
          final int largeQueue = (int) optionManager.getOption(ExecConstants.LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.large", largeQueue);
          queueName = "large";
          break;
        case SMALL:
          final int smallQueue = (int) optionManager.getOption(ExecConstants.SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.small", smallQueue);
          queueName = "small";
          break;
        default:
          throw new ForemanSetupException("Unsupported Queue type: " + queueType);
      }

      lease = distributedSemaphore.acquire(queueTimeout, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      throw new ForemanSetupException("Unable to acquire slot for query.", e);
    }

    if (lease == null) {
      throw UserException
          .resourceError()
          .message(
              "Unable to acquire queue resources for query within timeout.  Timeout for %s queue was set at %d seconds.",
              queueName, queueTimeout / 1000)
          .build(logger);
    }

  }

}
