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

import static com.dremio.sabot.exec.fragment.FragmentExecutorBuilder.PIPELINE_RES_GRP;
import static com.dremio.sabot.exec.fragment.FragmentExecutorBuilder.WORK_QUEUE_RES_GRP;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.CatastrophicFailure;
import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.FragmentPriority;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.sabot.driver.OperatorCreator;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.driver.Pipeline;
import com.dremio.sabot.driver.PipelineCreator;
import com.dremio.sabot.driver.SchemaChangeListener;
import com.dremio.sabot.driver.UserDelegatingOperatorCreator;
import com.dremio.sabot.exec.EventProvider;
import com.dremio.sabot.exec.QueriesClerk.FragmentTicket;
import com.dremio.sabot.exec.StateTransitionException;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.receiver.IncomingBuffers;
import com.dremio.sabot.task.AsyncTask;
import com.dremio.sabot.task.Task.State;
import com.dremio.sabot.task.TaskDescriptor;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.sabot.threads.sharedres.SharedResourcesContextImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.util.internal.OutOfDirectMemoryError;

/**
 * A reusable Runnable and Task that executes a fragment's pipeline. This
 * runnable is designed to stop regularly such that it can be rescheduled as
 * necessary. It needs to be run repeatedly until getState() returns State.DONE.
 *
 * Virtually all work is done in a single thread to avoid any concurrency
 * complexities. Any incoming messages are staged until the next time this is
 * scheduled and then the execution thread is responsible for handling those
 * messages.
 */
public class FragmentExecutor {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutor.class);

  /** threadsafe fields, influenced by external events. **/
  private final FragmentExecutorListener listener = new FragmentExecutorListener();
  private final ForemanDeathListener crashListener = new ForemanDeathListener();

  /** start of private execution thread only fields. **/
  private final String name;
  private final DoAsPumper pumper = new DoAsPumper();
  private final FragmentStatusReporter statusReporter;
  private final DeferredException deferredException;

  private final PlanFragment fragment;
  private final UserGroupInformation queryUserUgi;
  private final ClusterCoordinator clusterCoordinator;
  private final PhysicalPlanReader reader;
  private final SharedResourceManager sharedResources;
  private final OperatorCreatorRegistry opCreator;
  private final BufferAllocator allocator;
  private final SchemaChangeListener updater;
  private final ContextInformation contextInfo;
  private final OperatorContextCreator contextCreator;
  private final FunctionLookupContext functionLookupContext;
  private final TunnelProvider tunnelProvider;
  private final FlushableSendingAccountor flushable;
  private final FragmentStats stats;
  private final FragmentTicket ticket;
  private final StoragePluginRegistry storagePluginRegistry;

  private boolean retired = false;
  private boolean isSetup = false;

  // All tasks start as runnable. Only the execution thread will be allowed to change this value so no locking is needed.
  private volatile State taskState = State.RUNNABLE;

  // All Fragments starts as awaiting allocation. Changed by only execution thread. Modified externally thus volatile setting.
  private volatile FragmentState state = FragmentState.AWAITING_ALLOCATION;

  private Pipeline pipeline;
  private final IncomingBuffers buffers;

  private volatile TaskDescriptor taskDescriptor;

  private final EventProvider eventProvider;

  private final FragmentWorkQueue workQueue;

  private final SettableFuture<Boolean> cancelled;

  public FragmentExecutor(
      FragmentStatusReporter statusReporter,
      SabotConfig config,
      PlanFragment fragment,
      ClusterCoordinator clusterCoordinator,
      PhysicalPlanReader reader,
      SharedResourceManager sharedResources,
      OperatorCreatorRegistry opCreator,
      BufferAllocator allocator,
      SchemaChangeListener updater,
      ContextInformation contextInfo,
      OperatorContextCreator contextCreator,
      FunctionLookupContext functionLookupContext,
      TunnelProvider tunnelProvider,
      FlushableSendingAccountor flushable,
      FragmentStats stats,
      final FragmentTicket ticket,
      StoragePluginRegistry storagePluginRegistry,
      DeferredException exception,
      EventProvider eventProvider) {
    super();
    this.name = QueryIdHelper.getExecutorThreadName(fragment.getHandle());
    this.queryUserUgi = ImpersonationUtil.createProxyUgi(fragment.getCredentials().getUserName());
    this.statusReporter = statusReporter;
    this.fragment = fragment;
    this.clusterCoordinator = clusterCoordinator;
    this.reader = reader;
    this.sharedResources = sharedResources;
    this.opCreator = opCreator;
    this.functionLookupContext = functionLookupContext;
    this.allocator = allocator;
    this.updater = updater;
    this.contextInfo = contextInfo;
    this.contextCreator = contextCreator;
    this.tunnelProvider = tunnelProvider;
    this.flushable = flushable;
    this.stats = stats;
    this.ticket = ticket;
    this.deferredException = exception;
    this.storagePluginRegistry = storagePluginRegistry;
    this.workQueue = new FragmentWorkQueue(sharedResources.getGroup(WORK_QUEUE_RES_GRP));
    this.buffers = new IncomingBuffers(deferredException, sharedResources.getGroup(PIPELINE_RES_GRP), workQueue, tunnelProvider, fragment, allocator, config);
    this.eventProvider = eventProvider;
    this.cancelled = SettableFuture.create();
  }

  /**
   * Do some work.
   */
  private void run(){
    assert taskState != State.DONE : "Attempted to run a task with state of DONE.";
    assert eventProvider != null : "Attempted to run without an eventProvider";
    stats.runStarted();

    // update thread name.
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(originalName + " - " + name);

    try {

      // if we're already done, we're finishing clean up. No core method
      // execution is necessary, simply exit this block so we finishRun() below.
      // We do this because a failure state will put us in a situation.
      if(state == FragmentState.CANCELLED || state == FragmentState.FAILED || state == FragmentState.FINISHED) {
        return;
      }

      // if there are any deferred exceptions, exit.
      if(deferredException.hasException()){
        transitionToFailed(null);
        return;
      }

      // if cancellation is requested, that is always the top priority.
      if (eventProvider.isCancelled()) {
        transitionToCancelled();
        taskState = State.DONE;
        return;
      }

      final Runnable work = workQueue.poll();
      if (work != null) {
        // we don't know how long it will take to process one work unit, we rely on the scheduler to execute
        // this fragment again if it didn't run long enough
        work.run();
        return;
      }

      // setup the execution if it isn't setup.
      if(!isSetup){
        stats.setupStarted();
        try {
          setupExecution();
        } finally {
          stats.setupEnded();
        }
        // exit since we just did setup which could be a non-trivial amount of work. Allow the scheduler to decide whether we should continue.
        return;
      }

      // handle any previously sent fragment finished messages.
      FragmentHandle finishedFragment;
      while ((finishedFragment = eventProvider.pollFinishedReceiver()) != null) {
        pipeline.getTerminalOperator().receivingFragmentFinished(finishedFragment);
      }

      // pump the pipeline
      // TODO: look at whether the doAs here is actually necessary.
      taskState = queryUserUgi.doAs(pumper);

      // if we've finished all work, let's wrap up.
      if(taskState == State.DONE){
        transitionToFinished();
      }

    } catch (OutOfMemoryError e) {
      // handle out of memory errors differently from other error types.
      if (e instanceof OutOfDirectMemoryError || "Direct buffer memory".equals(e.getMessage())) {
        transitionToFailed(e);
      } else {
        // we have a heap out of memory error. The JVM in unstable, exit.
        CatastrophicFailure.exit(e, "Unable to handle out of memory condition in FragmentExecutor.", 2);
      }
    } catch (Throwable e) {
      transitionToFailed(e);
    } finally {

      try {
        finishRun(originalName);
      } finally {
        stats.runEnded();
      }
    }

  }

  /**
   * Informs FragmentExecutor to refresh state with the expectation that a
   * previously blocked state is now moving to an unblocked state.
   */
  private void refreshState() {
    Preconditions.checkArgument(taskState == State.BLOCKED, "Should only called when we were previously blocked.");
    Preconditions.checkArgument(sharedResources.isAvailable(), "Should only be called once at least one shared group is available: " + sharedResources.toString());
    taskState = State.RUNNABLE;
  }

  /**
   * Class used to pump data within the query user's doAs space.
   */
  private class DoAsPumper implements PrivilegedExceptionAction<State> {

    @Override
    public State run() throws Exception {
      return pipeline.pumpOnce();
    }

  }

  /**
   * Returns the current fragment status if the fragment is running. Otherwise, returns no status.
   *
   * @return FragmentStatus or null.
   */
  public FragmentStatus getStatus() {
    /*
     * If the query is not in a running state, the operator tree is still being constructed and
     * there is no reason to poll for intermediate results.
     *
     * Previously the call to get the operator stats with the AbstractStatusReporter was happening
     * before this check. This caused a concurrent modification exception as the list of operator
     * stats is iterated over while collecting info, and added to while building the operator tree.
     */
    if (state != FragmentState.RUNNING) {
      return null;
    }

    return statusReporter.getStatus(FragmentState.RUNNING);
  }

  private void setupExecution() throws Exception{
    logger.debug("Starting fragment {}:{} on {}:{}", fragment.getHandle().getMajorFragmentId(), fragment.getHandle().getMinorFragmentId(), fragment.getAssignment().getAddress(), fragment.getAssignment().getUserPort());

    final PhysicalOperator rootOperator = reader.readFragmentOperator(fragment.getFragmentJson(), fragment.getFragmentCodec());

    final OperatorCreator operatorCreator = new UserDelegatingOperatorCreator(contextInfo.getQueryUser(), opCreator);
    pipeline = PipelineCreator.get(
        new FragmentExecutionContext(fragment.getForeman(), updater, storagePluginRegistry, cancelled),
        buffers,
        operatorCreator,
        contextCreator,
        functionLookupContext,
        rootOperator,
        tunnelProvider,
        new SharedResourcesContextImpl(sharedResources)
        );

    pipeline.setup();

    clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).addNodeStatusListener(crashListener);

    transitionToRunning();
    isSetup = true;
  }

  // called every time a run is completed.
  private void finishRun(String originalThreadName) {

    // if we're in a terminal state, send final outcome.
    stats.finishStarted();
    try {
      switch(state){
      case FAILED:
      case FINISHED:
      case CANCELLED:
        retire();

      default:
        // noop
      }

    } finally {
      Thread.currentThread().setName(originalThreadName);
      stats.finishEnded();
    }
  }

  /**
   * Entered by something other than the execution thread. Ensures this fragment gets rescheduled as soon as possible.
   */
  private void requestCancellation(){
    this.cancelled.set(true);
    this.sharedResources.getGroup(PIPELINE_RES_GRP).markAllAvailable();
  }

  private State getState() {
    return taskState;
  }

  private void retire() {
    Preconditions.checkArgument(!retired, "Fragment executor already required.");

    if(!flushable.flushMessages()) {
      // rerun retire if we have messages still pending send completion.
      taskState = State.BLOCKED;
      return;
    }

    deferredException.suppressingClose(pipeline);
    // make sure to close incoming buffers before we call flushMessages() otherwise we may block before
    // we sent ACKs to other fragments and force other fragments to wait on us
    deferredException.suppressingClose(buffers);

    if(!flushable.flushMessages()) {
      // rerun retire if we have messages still pending send completion.
      taskState = State.BLOCKED;
      return;
    } else {
      taskState = State.DONE;
    }

    clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).removeNodeStatusListener(crashListener);

    deferredException.suppressingClose(contextCreator);
    deferredException.suppressingClose(allocator);
    deferredException.suppressingClose(ticket);

    // if defferedexception is set, update state to failed.
    if(deferredException.hasException()){
      transitionToFailed(null);
    }

    // send the final state of the fragment. only the main execution thread can send the final state and it can
    // only be sent once.
    if (state == FragmentState.FAILED) {
      final FragmentHandle handle = fragment.getHandle();

      @SuppressWarnings("deprecation")
      final UserException uex = UserException.systemError(deferredException.getAndClear())
          .addIdentity(fragment.getAssignment())
          .addContext("Fragment", handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId())
          .build(logger);
      statusReporter.fail(uex);
    } else {
      statusReporter.stateChanged(state);
    }

    retired = true;
    logger.debug("Fragment finished {}:{} on {}:{}", fragment.getHandle().getMajorFragmentId(), fragment.getHandle().getMinorFragmentId(), fragment.getAssignment().getAddress(), fragment.getAssignment().getUserPort());
  }

  private void transitionToFinished(){
    switch(state){
    case FAILED:
    case CANCELLED:
      // don't override a terminal state.
      dropStateChange(FragmentState.FINISHED);
      return;

    default:
      state = FragmentState.FINISHED;
    }
  }

  private void transitionToCancelled(){
    switch(state){
    case FAILED:
      dropStateChange(FragmentState.CANCELLED);
      return;
    default:
      state = FragmentState.CANCELLED;
    }
  }

  private void transitionToFailed(Throwable t) {
    if(t != null){
      deferredException.addThrowable(t);
    }
    switch(state){
    case FAILED:
      dropStateChange(FragmentState.FAILED);
      return;
    default:
      state = FragmentState.FAILED;
      return;
    }
  }

  private void transitionToRunning() {
    switch(state){
    case FAILED:
    case CANCELLED:
      // we've already moved to a terminal state.
      dropStateChange(FragmentState.RUNNING);
      return;

    // reasonable initial states.
    case AWAITING_ALLOCATION:
    case SENDING:
      state = FragmentState.RUNNING;
      return;

    case FINISHED:
    case RUNNING:
      errorStateChange(FragmentState.RUNNING);
      return;
    default:
      return;

    }
  }

  /**
   * Responsible for listening to a death to the driving force behind this
   * fragment. If the driving node crashes, all the PipelineExecutors have to
   * shoot themselves.
   */
  private class ForemanDeathListener implements NodeStatusListener {

    @Override
    public void nodesRegistered(final Set<NodeEndpoint> registereds) {
    }

    @Override
    public void nodesUnregistered(final Set<NodeEndpoint> unregistereds) {
      final NodeEndpoint foremanEndpoint = fragment.getForeman();
      if (unregistereds.contains(foremanEndpoint)) {
        logger.warn("AttemptManager {} no longer active.  Cancelling fragment {}.",
                    foremanEndpoint.getAddress(),
                    QueryIdHelper.getQueryIdentifier(fragment.getHandle()));
        requestCancellation();
      }
    }

  }

  private void errorStateChange(final FragmentState target) {
    final String msg = "%s: Invalid state transition %s --> %s";
    throw new StateTransitionException(String.format(msg, name, state.name(), target.name()));
  }

  private void dropStateChange(final FragmentState target) {
    logger.debug(name + ": Dropping state transition {} --> {}", state.name(), target.name());
  }

  public FragmentExecutorListener getListener(){
    return listener;
  }

  public FragmentHandle getHandle(){
    return fragment.getHandle();
  }

  public FragmentPriority getPriority(){
    return fragment.getPriority();
  }

  public AsyncTask asAsyncTask(){
    return new AsyncTaskImpl();
  }

  public NodeEndpoint getForeman(){
    return fragment.getForeman();
  }

  public String getBlockingStatus(){
    return sharedResources.toString();
  }

  public TaskDescriptor getTaskDescriptor() {
    return taskDescriptor;
  }

  /**
   * Facade for external events.
   */
  public class FragmentExecutorListener {

    public void handle(final IncomingDataBatch batch) throws FragmentSetupException, IOException {
      buffers.batchArrived(batch);
    }

    public void handle(FragmentStreamComplete completion) {
      buffers.completionArrived(completion);
    }

    public void cancel() {
      requestCancellation();
    }

    public FragmentHandle getHandle() {
      return fragment.getHandle();
    }

  }

  /**
   * Facade to expose this as an AsyncTask but protect other uses of APIs.
   */
  private class AsyncTaskImpl implements AsyncTask {

    @Override
    public void run() {
      FragmentExecutor.this.run();
    }

    @Override
    public void refreshState() {
      FragmentExecutor.this.refreshState();
    }

    @Override
    public State getState() {
      return FragmentExecutor.this.getState();
    }

    @Override
    public void updateSleepDuration(long duration) {
      stats.setSleepingDuration(duration);
    }

    @Override
    public void updateBlockedDuration(long duration) {
      stats.setBlockedDuration(duration);
    }

    @Override
    public void setWakeupCallback(AvailabilityCallback callback) {
      FragmentExecutor.this.sharedResources.setNextCallback(callback);
    }

    @Override
    public void setTaskDescriptor(TaskDescriptor descriptor) {
      taskDescriptor = descriptor;
    }
  }
}
