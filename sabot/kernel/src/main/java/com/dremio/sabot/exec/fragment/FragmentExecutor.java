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

import static com.dremio.sabot.exec.fragment.FragmentExecutorBuilder.PIPELINE_RES_GRP;
import static com.dremio.sabot.exec.fragment.FragmentExecutorBuilder.WORK_QUEUE_RES_GRP;

import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.DeferredException;
import com.dremio.common.ProcessExit;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.MemoryDebugInfo;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.CachedFragmentReader;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.driver.Pipeline;
import com.dremio.sabot.driver.PipelineCreator;
import com.dremio.sabot.exec.EventProvider;
import com.dremio.sabot.exec.FragmentTicket;
import com.dremio.sabot.exec.StateTransitionException;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FragmentStats;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.receiver.IncomingBuffers;
import com.dremio.sabot.task.AsyncTask;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.dremio.sabot.task.Task.State;
import com.dremio.sabot.task.TaskDescriptor;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.dremio.sabot.threads.sharedres.ActivableResource;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.dremio.sabot.threads.sharedres.SharedResourcesContextImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.spill.SpillService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.NettyArrowBuf;
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
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(FragmentExecutor.class);

  @VisibleForTesting
  public static final String INJECTOR_DO_WORK = "injectOOMOnRun";

  /** threadsafe fields, influenced by external events. **/
  private final FragmentExecutorListener listener = new FragmentExecutorListener();
  private final ForemanDeathListener crashListener = new ForemanDeathListener();

  /** start of private execution thread only fields. **/
  private final String name;
  private final DoAsPumper pumper = new DoAsPumper();
  private final FragmentStatusReporter statusReporter;
  private final DeferredException deferredException;

  private final PlanFragmentFull fragment;
  private final ClusterCoordinator clusterCoordinator;
  private final CachedFragmentReader reader;
  private final SharedResourceManager sharedResources;
  private final OperatorCreatorRegistry opCreator;
  private final BufferAllocator allocator;
  private final OperatorContextCreator contextCreator;
  private final FunctionLookupContext functionLookupContext;
  private final FunctionLookupContext decimalFunctionLookupContext;
  private final TunnelProvider tunnelProvider;
  private final FlushableSendingAccountor flushable;
  private final OptionManager fragmentOptions;
  private final FragmentStats stats;
  private final FragmentTicket ticket;
  private final CatalogService sources;

  private boolean retired = false;
  private boolean isSetup = false;

  // All tasks start as runnable. Only the execution thread will be allowed to change this value so no locking is needed.
  private volatile State taskState = State.RUNNABLE;

  // All Fragments starts as awaiting allocation. Changed by only execution thread. Modified externally thus volatile setting.
  private volatile FragmentState state = FragmentState.AWAITING_ALLOCATION;

  private BufferAllocator outputAllocator;
  private Pipeline pipeline;
  private final IncomingBuffers buffers;

  private volatile TaskDescriptor taskDescriptor;

  private final EventProvider eventProvider;

  private final FragmentWorkQueue workQueue;

  private final SettableFuture<Boolean> cancelled;

  private final ExecutionControls executionControls;

  // The fragment will not be activated until it gets :
  // a. a activate/cancel from the foreman (or)
  // b. an incoming data/oob/finished msg from any upstream fragment.
  private final ActivableResource activateResource;

  public FragmentExecutor(
      FragmentStatusReporter statusReporter,
      SabotConfig config,
      ExecutionControls executionControls,
      PlanFragmentFull fragment,
      ClusterCoordinator clusterCoordinator,
      CachedFragmentReader reader,
      SharedResourceManager sharedResources,
      OperatorCreatorRegistry opCreator,
      BufferAllocator allocator,
      ContextInformation contextInfo,
      OperatorContextCreator contextCreator,
      FunctionLookupContext functionLookupContext,
      FunctionLookupContext decimalFunctionLookupContext,
      TunnelProvider tunnelProvider,
      FlushableSendingAccountor flushable,
      OptionManager fragmentOptions,
      FragmentStats stats,
      final FragmentTicket ticket,
      final CatalogService sources,
      DeferredException exception,
      EventProvider eventProvider,
      SpillService spillService) {
    super();
    this.name = QueryIdHelper.getExecutorThreadName(fragment.getHandle());
    this.statusReporter = statusReporter;
    this.fragment = fragment;
    this.clusterCoordinator = clusterCoordinator;
    this.reader = reader;
    this.sharedResources = sharedResources;
    this.opCreator = opCreator;
    this.functionLookupContext = functionLookupContext;
    this.decimalFunctionLookupContext = decimalFunctionLookupContext;
    this.allocator = allocator;
    this.contextCreator = contextCreator;
    this.tunnelProvider = tunnelProvider;
    this.flushable = flushable;
    this.fragmentOptions = fragmentOptions;
    this.stats = stats;
    this.ticket = ticket;
    this.deferredException = exception;
    this.sources = sources;
    this.activateResource = new ActivableResource(sharedResources.getGroup(PIPELINE_RES_GRP).createResource(
      "activate-signal-" + this.name, SharedResourceType.FRAGMENT_ACTIVATE_SIGNAL));
    this.workQueue = new FragmentWorkQueue(sharedResources.getGroup(WORK_QUEUE_RES_GRP));
    this.buffers = new IncomingBuffers(
      deferredException, sharedResources.getGroup(PIPELINE_RES_GRP), workQueue, tunnelProvider,
      fragment, allocator, config, executionControls, spillService, reader.getPlanFragmentsIndex());
    this.eventProvider = eventProvider;
    this.cancelled = SettableFuture.create();
    this.executionControls = executionControls;
  }

  /**
   * Do some work.
   */
  private void run(){
    assert taskState != State.DONE : "Attempted to run a task with state of DONE.";
    assert eventProvider != null : "Attempted to run without an eventProvider";

    if (!activateResource.isActivated()) {
      // All tasks are expected to begin in a runnable state. So, switch to the BLOCKED state on the
      // first call.
      taskState = State.BLOCKED_ON_SHARED_RESOURCE;
      return;
    }
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
      if (cancelled.isDone()) {
        Optional<Throwable> failedReason = eventProvider.getFailedReason();
        if (failedReason.isPresent()) {
          // check if it was failed due to an external reason (eg. by heap monitor).
          transitionToFailed(failedReason.get());
          return;
        }

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
      taskState = pumper.run();

      // if we've finished all work, let's wrap up.
      if(taskState == State.DONE){
        transitionToFinished();
      }

      injector.injectChecked(executionControls, INJECTOR_DO_WORK, OutOfMemoryError.class);

    } catch (OutOfMemoryError | OutOfMemoryException e) {
      // handle out of memory errors differently from other error types.
      if (e instanceof OutOfDirectMemoryError || e instanceof OutOfMemoryException || "Direct buffer memory".equals(e.getMessage()) || INJECTOR_DO_WORK.equals(e.getMessage())) {
        transitionToFailed(UserException.memoryError(e)
            .addContext(MemoryDebugInfo.getDetailsOnAllocationFailure(new OutOfMemoryException(e), allocator))
            .buildSilently());
      } else {
        // we have a heap out of memory error. The JVM in unstable, exit.
        ProcessExit.exitHeap(e);
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
    Preconditions.checkArgument(taskState == State.BLOCKED_ON_DOWNSTREAM ||
      taskState == State.BLOCKED_ON_UPSTREAM ||
      taskState == State.BLOCKED_ON_SHARED_RESOURCE, "Should only called when we were previously blocked.");
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
    final PlanFragmentMajor major = fragment.getMajor();
    final PlanFragmentMinor minor = fragment.getMinor();

    logger.debug("Starting fragment {}:{} on {}:{}", major.getHandle().getMajorFragmentId(), getHandle().getMinorFragmentId(), minor.getAssignment().getAddress(), minor.getAssignment().getUserPort());
    outputAllocator = ticket.newChildAllocator("output-frag:" + QueryIdHelper.getFragmentId(getHandle()),
      fragmentOptions.getOption(ExecConstants.OUTPUT_ALLOCATOR_RESERVATION),
      Long.MAX_VALUE);
    contextCreator.setFragmentOutputAllocator(outputAllocator);

    final PhysicalOperator rootOperator = reader.readFragment(fragment);
    FunctionLookupContext functionLookupContextToUse = functionLookupContext;
    if (fragmentOptions.getOption(PlannerSettings.ENABLE_DECIMAL_V2)) {
      functionLookupContextToUse = decimalFunctionLookupContext;
    }
    pipeline = PipelineCreator.get(
        new FragmentExecutionContext(major.getForeman(), sources, cancelled, major.getContext()),
        buffers,
        opCreator,
        contextCreator,
        functionLookupContextToUse,
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
   * Entered by something other than the execution thread. Makes this fragment's pipeline runnable.
   */
  private void requestActivate(String trigger) {
    this.activateResource.activate(trigger);
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
    Preconditions.checkArgument(!retired, "Fragment executor already retired.");

    if(!flushable.flushMessages()) {
      // rerun retire if we have messages still pending send completion.
      logger.debug("fragment retire blocked on downstream");
      taskState = State.BLOCKED_ON_DOWNSTREAM;
      return;
    }

    deferredException.suppressingClose(pipeline);
    // make sure to close incoming buffers before we call flushMessages() otherwise we may block before
    // we sent ACKs to other fragments and force other fragments to wait on us
    deferredException.suppressingClose(buffers);

    if(!flushable.flushMessages()) {
      // rerun retire if we have messages still pending send completion.
      logger.debug("fragment retire blocked on downstream");
      taskState = State.BLOCKED_ON_DOWNSTREAM;
      return;
    } else {
      taskState = State.DONE;
    }

    workQueue.retire();
    clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).removeNodeStatusListener(crashListener);

    deferredException.suppressingClose(contextCreator);
    deferredException.suppressingClose(outputAllocator);
    deferredException.suppressingClose(allocator);
    deferredException.suppressingClose(ticket);
    if (tunnelProvider != null && tunnelProvider.getCoordTunnel() != null) {
      deferredException.suppressingClose(tunnelProvider.getCoordTunnel().getTunnel());
    }

    // if defferedexception is set, update state to failed.
    if(deferredException.hasException()){
      transitionToFailed(null);
    }

    // send the final state of the fragment. only the main execution thread can send the final state and it can
    // only be sent once.
    final FragmentHandle handle = fragment.getMajor().getHandle();
    if (state == FragmentState.FAILED) {

      @SuppressWarnings("deprecation")
      final UserException uex = UserException.systemError(deferredException.getAndClear())
          .addIdentity(fragment.getMinor().getAssignment())
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
      final NodeEndpoint foremanEndpoint = fragment.getMajor().getForeman();
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

  public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
    return ticket.getSchedulingGroup();
  }

  public AsyncTask asAsyncTask(){
    return new AsyncTaskImpl();
  }

  public NodeEndpoint getForeman(){
    return fragment.getMajor().getForeman();
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

    public void handle(final IncomingDataBatch batch) {
      requestActivate("incoming data batch");
      buffers.batchArrived(batch);
    }

    public void handle(FragmentStreamComplete completion) {
      requestActivate("stream completion");
      buffers.completionArrived(completion);
    }

    public void handle(OutOfBandMessage message) {
      requestActivate("out of band message");
      message.retainBufferIfPresent();
      final AutoCloseable closeable = message.getBuffer() != null ? (NettyArrowBuf) message.getBuffer() : () -> {};
      workQueue.put(() -> {
          try {
            if (!isSetup) {
              if (message.getIsOptional()) {
                logger.warn("Fragment {} received optional OOB message in state {} for operatorId {}. Fragment is not yet set up. Ignoring message.",
                  this.getHandle().toString(), state.toString(), message.getOperatorId());
              } else {
                logger.error("Fragment {} received OOB message in state {} for operatorId {}. Fragment is not yet set up.",
                  this.getHandle().toString(), state.toString(), message.getOperatorId());
                throw new IllegalStateException("Unable to handle OOB message");
              }
            } else {
              pipeline.workOnOOB(message);
            }
          } catch(IllegalStateException e) {
            logger.warn("Failure while handling OOB message. {}", message, e);
            throw e;
          } catch (Exception e) {
            //propagate the exception
            logger.warn("Failure while handling OOB message. {}", message, e);
            throw new IllegalStateException(e);
          } finally {
            try {
              closeable.close();
            } catch (Exception e) {
              logger.error("Error while closing OOBMessage ref", e);
            }
          }
      }, closeable);
    }

    public void activate() {
      requestActivate("activate message from foreman");
    }

    public void cancel() {
      requestActivate("cancel message from foreman");
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
    public void updateBlockedOnDownstreamDuration(long duration) {
      stats.setBlockedOnDownstreamDuration(duration);
    }

    @Override
    public void updateBlockedOnUpstreamDuration(long duration) {
      stats.setBlockedOnUpstreamDuration(duration);
    }

    @Override
    public void addBlockedOnSharedResourceDuration(SharedResourceType resource, long duration) {
      stats.addBlockedOnSharedResourceDuration(resource, duration);
    }

    @Override
    public SharedResourceType getFirstBlockedResource() {
      return FragmentExecutor.this.sharedResources.getFirstBlockedResource("pipeline");
    }

    @Override
    public void setWakeupCallback(AvailabilityCallback callback) {
      FragmentExecutor.this.sharedResources.setNextCallback(callback);
    }

    @Override
    public void setTaskDescriptor(TaskDescriptor descriptor) {
      taskDescriptor = descriptor;
    }

    @Override
    public String toString() {
      return QueryIdHelper.getQueryIdentifier(FragmentExecutor.this.fragment.getHandle());
    }
  }
}
