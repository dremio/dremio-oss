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
package com.dremio.exec.work.foreman;

import java.util.Set;

import com.dremio.common.EventProcessor;
import com.dremio.common.ProcessExit;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.commands.AsyncCommand;
import com.dremio.exec.planner.sql.handlers.commands.CommandCreator;
import com.dremio.exec.planner.sql.handlers.commands.CommandRunner;
import com.dremio.exec.planner.sql.handlers.commands.CommandRunner.CommandType;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.dremio.service.Pointer;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.execselector.ExecutorSelectionService;
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
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(AttemptManager.class);

  private final AttemptId attemptId;
  private final QueryId queryId;
  private final String queryIdString;
  private final UserRequest queryRequest;
  private final QueryContext queryContext;
  private final QueryManager queryManager; // handles lower-level details of query execution
  private final SabotContext sabotContext;
  private final Cache<Long, PreparedPlan> plans;
  private volatile QueryState state;
  private volatile String cancelReason;
  private volatile boolean clientCancelled;

  private final StateSwitch stateSwitch = new StateSwitch();
  private final AttemptResult foremanResult = new AttemptResult();
  private Object extraResultData;
  private final CoordToExecTunnelCreator tunnelCreator;
  private final AttemptObservers observers;
  private final Pointer<QueryId> prepareId;
  private final ResourceAllocator queryResourceManager;
  private final CommandPool commandPool;
  private final ExecutorSelectionService executorSelectionService;
  private CommandRunner<?> command;

  /**
   * if set to true, query is not going to be scheduled on a separate thread
   */
  private final boolean runInSameThread;

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
      final OptionProvider options,
      final CoordToExecTunnelCreator tunnelCreator,
      final Cache<Long, PreparedPlan> plans,
      final QueryContext queryContext,
      final ResourceAllocator queryResourceManager,
      final CommandPool commandPool,
      final ExecutorSelectionService executorSelectionService,
      final boolean runInSameThread
      ) {
    this.attemptId = attemptId;
    this.queryId = attemptId.toQueryId();
    this.queryIdString = QueryIdHelper.getQueryId(queryId);
    this.queryRequest = queryRequest;
    this.sabotContext = context;
    this.tunnelCreator = tunnelCreator;
    this.queryResourceManager = queryResourceManager;
    this.commandPool = commandPool;
    this.executorSelectionService = executorSelectionService;
    this.plans = plans;
    this.prepareId = new Pointer<>();

    this.queryContext = queryContext;
    this.observers = AttemptObservers.of(observer);
    this.observers.add(new FragmentActivateObserver());
    this.runInSameThread = runInSameThread;

    final OptionManager optionManager = this.queryContext.getOptions();
    if(options != null){
      options.applyOptions(optionManager);
    }

    this.queryManager = new QueryManager(queryId, this.queryContext, this.tunnelCreator, new CompletionListenerImpl(), prepareId,
      observers, optionManager.getOption(PlannerSettings.VERBOSE_PROFILE),
      optionManager.getOption(PlannerSettings.INCLUDE_DATASET_PROFILE), this.queryContext.getCatalog());

    recordNewState(QueryState.ENQUEUED);
  }

  private class FragmentActivateObserver extends AbstractAttemptObserver {

    @Override
    public void activateFragmentFailed(Exception ex) {
      addToEventQueue(QueryState.FAILED, ex);
    }
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

  public void dataFromScreenArrived(QueryData header, ByteBuf data, ResponseSender sender) {
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

  /**
   * Publish query progress.
   */
  public void publishProgress() {
    queryManager.publishProgress();
  }

  public void updateStatus(FragmentStatus status) {
    queryManager.getFragmentStatusListener().statusUpdate(status);
  }

  public void updateNodeQueryStatus(NodeQueryStatus status) {
    queryManager.updateNodeQueryStatus(status);
  }

  public QueryProfile getQueryProfile() {
    return queryManager.getQueryProfile(queryRequest.getDescription(), state, null, cancelReason);
  }

  /**
   * Cancel the query. Asynchronous -- it may take some time for all remote fragments to be
   * terminated.
   *
   * @param reason          description of the cancellation
   * @param clientCancelled true if the client application explicitly issued a cancellation (via end user action), or
   *                        false otherwise (i.e. when pushing the cancellation notification to the end user)
   */
  public void cancel(String reason, boolean clientCancelled) {
    // Note this can be called from outside of run() on another thread, or after run() completes
    this.cancelReason = reason;
    this.clientCancelled = clientCancelled;
    addToEventQueue(QueryState.CANCELED, null);
  }

  /**
   * Resume a paused query
   */
  public void resume() {
    queryContext.getExecutionControls().unpauseAll();
  }

  @Override
  public void run() {
    // rename the thread we're using for debugging purposes
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(queryIdString + ":foreman");


    try {
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-beginning", ForemanException.class);

      // planning is done in the command pool
      commandPool.submit(CommandPool.Priority.LOW, attemptId.toString() + ":foreman-planning",
        (waitInMillis) -> {
          observers.commandPoolWait(waitInMillis);
          observers.queryStarted(queryRequest, queryContext.getSession().getCredentials().getUserName());
          plan();
          return null;
        }, runInSameThread).get();

      if (command.getCommandType() == CommandType.ASYNC_QUERY) {

        AsyncCommand asyncCommand = (AsyncCommand) command;

        moveToState(QueryState.STARTING, null);

        // allocate execution resources on the calling thread, as this will most likely block
        asyncCommand.allocateResources();

        // do execution planning in the bound pool
        commandPool.submit(CommandPool.Priority.MEDIUM,
          attemptId.toString() + ":execution-planning",
          (waitInMillis) -> {
            observers.commandPoolWait(waitInMillis);
            asyncCommand.planExecution();
            return null;
          }, runInSameThread).get();

        asyncCommand.startFragments();
      }

      moveToState(QueryState.RUNNING, null);

      injector.injectChecked(queryContext.getExecutionControls(), "run-try-end", ForemanException.class);
    } catch (ResourceUnavailableException e) {
      // resource allocation failure is treated as a cancellation and not a failure
      cancelReason = e.getMessage();
      moveToState(QueryState.CANCELED, null); // ENQUEUED/STARTING -> CANCELED transition
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
        ProcessExit.exitHeap(e);
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

  private void plan() throws Exception {
    CommandCreator creator = newCommandCreator(queryContext, observers, prepareId);
    command = creator.toCommand();
    logger.debug("Using command: {}.", command);

    switch (command.getCommandType()) {
      case ASYNC_QUERY:
        Preconditions.checkState(command instanceof AsyncCommand, "Asynchronous query must be an AsyncCommand");
        command.plan();
        break;

      case SYNC_QUERY:
      case SYNC_RESPONSE:
        moveToState(QueryState.STARTING, null);
        command.plan();
        extraResultData = command.execute();
        addToEventQueue(QueryState.COMPLETED, null);
        break;

      default:
        throw new IllegalStateException(
          String.format("command type %s not supported in plan()", command.getCommandType()));
    }
  }

  protected CommandCreator newCommandCreator(QueryContext queryContext, AttemptObserver observer, Pointer<QueryId> prepareId) {
    return new CommandCreator(this.sabotContext, queryContext, tunnelCreator, queryRequest,
      observer, plans, prepareId, attemptId.getAttemptNum(), queryResourceManager, executorSelectionService);
  }

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

    @Override
    public void close() {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      // publish progress for this attempt
      queryManager.publishProgress();

      // to track how long the query takes
      queryManager.markEndTime();

      logger.debug(queryIdString + ": cleaning up.");
      injector.injectPause(queryContext.getExecutionControls(), "foreman-cleanup", logger);

      sabotContext.getClusterCoordinator()
          .getServiceSet(ClusterCoordinator.Role.EXECUTOR)
          .removeNodeStatusListener(queryManager.getNodeStatusListener());

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
          queryManager.getQueryProfile(queryRequest.getDescription(), state, uex, cancelReason), uex, cancelReason,
            clientCancelled);
        observers.attemptCompletion(result);
      } catch(final Exception e) {
        addException(e);
        logger.warn("Exception sending result to client", resultException);
      }

      try {
        command.close();
      } catch (final Exception e) {
        logger.error("Exception while invoking 'close' on command {}", command, e);
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
   * Do not call it directly from external, all the state changes should go through {@link #addToEventQueue(QueryState, Exception)}
   * and they will be synchronized as events to prevent unpredictable failure.
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
          return;

        case CANCELED: {
          assert exception == null;
          recordNewState(QueryState.CANCELED);
          foremanResult.setCompleted(QueryState.CANCELED);
          foremanResult.close();
          return;
        }
        }
        break;

      case STARTING:
        switch (newState) {

        case RUNNING: {
          recordNewState(QueryState.RUNNING);
          observers.execStarted(queryManager.getQueryProfile(queryRequest.getDescription(), newState, null, cancelReason));
          return;
        }

        case CANCELED: {
          assert exception == null;
          recordNewState(QueryState.CANCELED);
          foremanResult.setCompleted(QueryState.CANCELED);
          foremanResult.close();
          return;
        }
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
            queryManager.cancelExecutingFragments();
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
            queryManager.cancelExecutingFragments();
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

  private void recordNewState(final QueryState newState) {
    state = newState;
  }
 }
