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
package com.dremio.sabot.exec;

import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactoryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClient;
import com.dremio.service.maestroservice.MaestroClient;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.inject.Provider;

/** Tracker for one query in the local Proxy for the maestro service. */
class MaestroProxyQueryTracker implements QueryTracker {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaestroProxyQueryTracker.class);
  private static final int INITIAL_BACKOFF_MILLIS = 5;
  private static final int MAX_BACKOFF_MILLIS = 60_000;

  private final QueryId queryId;
  private final Provider<NodeEndpoint> selfEndpoint;
  private final long evictionDelayMillis;
  private final ScheduledThreadPoolExecutor retryExecutor;
  private final ClusterCoordinator clusterCoordinator;
  private final Map<FragmentHandle, FragmentStatus> lastFragmentStatuses = new HashMap<>();
  private final ForemanDeathListener foremanDeathListener = new ForemanDeathListener();
  private final FileCursorManagerFactory fileCursorManagerFactory =
      new FileCursorManagerFactoryImpl();

  private State state = State.INVALID;
  private boolean cancelled;
  private MaestroClient maestroServiceClient;
  private JobTelemetryExecutorClient jobTelemetryClient;
  private QueryTicket queryTicket;
  private NodeEndpoint foreman;
  private long querySentTime;
  private long expirationTime;
  private boolean resultsSent;
  private DremioPBError firstErrorInQuery;
  private FragmentHandle errorFragmentHandle;
  private volatile boolean foremanDead;
  private AtomicInteger pendingMessages = new AtomicInteger(0);
  private Set<FragmentHandle> pendingFragments = null;
  private Set<FragmentHandle> notStartedFragments = null;
  private boolean notifyFileCursorManagerDone;

  /**
   * Initialize with the set of fragment handles for the query before starting the query.
   *
   * @param pendingFragments
   */
  @Override
  public void initFragmentsForQuery(Set<FragmentHandle> pendingFragments) {
    Preconditions.checkState(
        pendingFragments != null && pendingFragments.size() > 0,
        "Pending " + "fragments should be non empty.");
    this.pendingFragments = pendingFragments;
    this.notStartedFragments = new HashSet<>(pendingFragments);
  }

  enum State {
    INVALID,
    STARTED,
    DONE
  }

  MaestroProxyQueryTracker(
      QueryId queryId,
      Provider<NodeEndpoint> selfEndpoint,
      long evictionDelayMillis,
      ScheduledThreadPoolExecutor retryExecutor,
      ClusterCoordinator clusterCoordinator) {
    this.queryId = queryId;
    this.selfEndpoint = selfEndpoint;
    this.evictionDelayMillis = evictionDelayMillis;
    this.retryExecutor = retryExecutor;
    this.expirationTime = System.currentTimeMillis() + evictionDelayMillis;
    this.clusterCoordinator = clusterCoordinator;
  }

  /**
   * Try to start a new query. While the start is in-progress, no completion event will be sent.
   *
   * @param queryTicket ticket for the query.
   * @param maestroServiceClient client to maestro service
   * @return true if query can be started.
   */
  @Override
  public synchronized boolean tryStart(
      QueryTicket queryTicket,
      NodeEndpoint foreman,
      MaestroClient maestroServiceClient,
      JobTelemetryExecutorClient jobTelemetryClient) {
    if (state != State.INVALID) {
      // query already started, probably a duplicate request.
      return false;
    } else if (cancelled) {
      // query already cancelled, probably a race between start and cancel
      return false;
    }

    this.state = State.STARTED;
    this.queryTicket = queryTicket;
    this.foreman = foreman;
    this.maestroServiceClient = maestroServiceClient;
    this.jobTelemetryClient = jobTelemetryClient;
    return true;
  }

  @Override
  public NodeEndpoint getForeman() {
    return foreman;
  }

  @Override
  public synchronized boolean isStarted() {
    return state != State.INVALID;
  }

  @Override
  public synchronized void setCancelled() {
    cancelled = true;
    expirationTime = System.currentTimeMillis() + evictionDelayMillis;
  }

  @Override
  public synchronized boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public synchronized boolean isTerminal() {
    return this.cancelled || state == State.DONE || isExpired();
  }

  @Override
  public boolean isExpired() {
    return (state == State.INVALID || state == State.DONE)
        && pendingMessages.get() == 0
        && System.currentTimeMillis() > expirationTime;
  }

  private static boolean isTerminal(FragmentState state) {
    return state == FragmentState.FAILED
        || state == FragmentState.FINISHED
        || state == FragmentState.CANCELLED;
  }

  private static boolean isRunningOrTerminal(FragmentState state) {
    return state == FragmentState.RUNNING || isTerminal(state);
  }

  @Override
  public synchronized void refreshFragmentStatus(FragmentStatus fragmentStatus) {
    final FragmentHandle handle = fragmentStatus.getHandle();
    checkAndRemoveFromUnstartedFragments(handle, fragmentStatus.getProfile().getState());

    FragmentStatus prevStatus = lastFragmentStatuses.get(handle);
    if (prevStatus != null && isTerminal(prevStatus.getProfile().getState())) {
      // can happen if there is a race between fragment completion and status reporter.
      return;
    }

    FragmentStatus fragmentStatusToSave = clearProfileError(fragmentStatus);
    lastFragmentStatuses.put(handle, fragmentStatusToSave);
  }

  @Override
  public Optional<ListenableFuture<Empty>> sendQueryProfile() {
    ExecutorQueryProfile profile;

    synchronized (this) {
      if (state == State.DONE) {
        return Optional.empty();
      }
      profile = getExecutorQueryProfile();
    }
    return Optional.of(jobTelemetryClient.putExecutorProfile(profile));
  }

  private ExecutorQueryProfile getExecutorQueryProfile() {
    ExecutorQueryProfile profile;
    Preconditions.checkState(queryTicket != null);

    List<FragmentStatus> fragmentStatuses = new ArrayList<>(lastFragmentStatuses.values());
    profile =
        ExecutorQueryProfile.newBuilder()
            .setQueryId(queryId)
            .setEndpoint(EndpointHelper.getMinimalEndpoint(selfEndpoint.get()))
            .setProgress(buildProgressMetrics(fragmentStatuses))
            .setNodeStatus(queryTicket.getStatus())
            .addAllFragments(fragmentStatuses)
            .build();
    return profile;
  }

  private static QueryProgressMetrics buildProgressMetrics(List<FragmentStatus> fragmentStatuses) {
    long recordCount = 0;
    long ctasRecordCount = 0;
    long arrowRecordCount = 0;
    for (FragmentStatus fragmentStatus : fragmentStatuses) {
      for (OperatorProfile operatorProfile : fragmentStatus.getProfile().getOperatorProfileList()) {
        for (StreamProfile streamProfile : operatorProfile.getInputProfileList()) {
          recordCount += streamProfile.getRecords();
          if (isCtasOperator(CoreOperatorType.valueOf(operatorProfile.getOperatorType()))) {
            ctasRecordCount += streamProfile.getRecords();
          } else if (isArrowOperator(CoreOperatorType.valueOf(operatorProfile.getOperatorType()))) {
            arrowRecordCount += streamProfile.getRecords();
          }
        }
      }
    }
    // derived from QueryProfileParser.java, all operators which produce output to client except
    // SCREEN,
    // first check ctas writers, if not present then arrow writer.
    long outputRecords = (ctasRecordCount > 0) ? ctasRecordCount : arrowRecordCount;
    return QueryProgressMetrics.newBuilder()
        .setRowsProcessed(recordCount)
        .setOutputRecords(outputRecords)
        .build();
  }

  private static boolean isCtasOperator(CoreOperatorType type) {
    switch (type) {
      case PARQUET_WRITER:
      case TEXT_WRITER:
      case JSON_WRITER:
        return true;

      default:
        return false;
    }
  }

  private static boolean isArrowOperator(CoreOperatorType type) {
    return type == CoreOperatorType.ARROW_WRITER;
  }

  /**
   * Error from any one of the the MinorFragmentProfile is sufficient to end the query and return
   * the error to the callers. So, once an error from a profile is captured in firstErrorInQuery,
   * errors from other profiles can be cleared. Clearing the errors from profiles helps not
   * consuming heap memory when the error messages are too long especially when the error messages
   * are due to insufficient heap memory.
   */
  private FragmentStatus clearProfileError(FragmentStatus fragmentStatus) {
    FragmentStatus newFragmentStatus = fragmentStatus;
    if (firstErrorInQuery != null && fragmentStatus.getProfile().getError() != null) {
      MinorFragmentProfile.Builder profileBuilder = fragmentStatus.getProfile().toBuilder();
      profileBuilder.clearError();
      newFragmentStatus = fragmentStatus.toBuilder().setProfile(profileBuilder.build()).build();
    }
    return newFragmentStatus;
  }

  private void checkAndRemoveFromUnstartedFragments(FragmentHandle handle, FragmentState state) {
    if (notifyFileCursorManagerDone || !isRunningOrTerminal(state)) {
      return;
    }

    boolean mustNotify = false;
    synchronized (this) {
      notStartedFragments.remove(handle);
      if (notStartedFragments.isEmpty()) {
        mustNotify = true;
      }
    }
    if (mustNotify) {
      fileCursorManagerFactory.notifyAllRegistrationsDone();
      notifyFileCursorManagerDone = true;
    }
  }

  /**
   * Handle the status change of one fragment.
   *
   * @param fragmentStatus
   */
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  @Override
  public void fragmentStatusChanged(FragmentStatus fragmentStatus) {
    Preconditions.checkState(
        pendingFragments != null,
        "Pending fragments should have been " + "registered before starting the query.");

    final FragmentHandle handle = fragmentStatus.getHandle();
    final MinorFragmentProfile profile = fragmentStatus.getProfile();
    checkAndRemoveFromUnstartedFragments(handle, profile.getState());

    NodeQueryFirstError firstError = null;
    NodeQueryScreenCompletion screenCompletion = null;
    synchronized (this) {
      switch (profile.getState()) {
        case FAILED:
          if (firstErrorInQuery == null) {
            firstErrorInQuery = profile.getError();
            errorFragmentHandle = fragmentStatus.getHandle();

            // propagate the first error.
            firstError =
                NodeQueryFirstError.newBuilder()
                    .setHandle(handle)
                    .setEndpoint(EndpointHelper.getMinimalEndpoint(selfEndpoint.get()))
                    .setForeman(foreman)
                    .setError(firstErrorInQuery)
                    .build();
          }
          // fall-through

        case CANCELLED:
        case FINISHED:
          FragmentStatus fragmentStatusToSave = clearProfileError(fragmentStatus);
          lastFragmentStatuses.put(handle, fragmentStatusToSave);
          pendingFragments.remove(fragmentStatus.getHandle());
          if (handle.getMajorFragmentId() == 0 && profile.getState() == FragmentState.FINISHED) {
            // operator with screen finished.
            screenCompletion =
                NodeQueryScreenCompletion.newBuilder()
                    .setId(handle.getQueryId())
                    .setEndpoint(EndpointHelper.getMinimalEndpoint(selfEndpoint.get()))
                    .setForeman(foreman)
                    .build();
          }
          checkIfResultsSent(fragmentStatus);
          checkIfAllFragmentDone();
          break;

        default:
          // ignore other status.
      }
    }

    if (screenCompletion != null) {
      final NodeQueryScreenCompletion completion = screenCompletion;
      Consumer<StreamObserver<Empty>> consumer =
          observer -> {
            try {
              logger.debug(
                  "sending screen completion to foreman {}:{}",
                  foreman.getAddress(),
                  foreman.getFabricPort());
              maestroServiceClient.screenComplete(completion, observer);
            } catch (Exception e) {
              observer.onError(e);
            }
          };
      consumer.accept(new RetryingObserver(consumer));
    }
    if (firstError != null) {
      final NodeQueryFirstError error = firstError;
      Consumer<StreamObserver<Empty>> consumer =
          observer -> {
            try {
              logger.debug(
                  "sending fragment error to foreman {}:{}",
                  foreman.getAddress(),
                  foreman.getFabricPort());
              maestroServiceClient.nodeFirstError(error, observer);
            } catch (Exception e) {
              observer.onError(e);
            }
          };
      consumer.accept(new RetryingObserver(consumer));
    }
  }

  private void checkIfAllFragmentDone() {
    if (state != State.STARTED) {
      return;
    }
    // thread safe - caller is synchronized, so last update
    // will make the set empty
    if (!pendingFragments.isEmpty()) {
      return;
    }

    // This is required so that all of the final metrics are reflected accurately.
    ExecutorQueryProfile finalQueryProfile = getExecutorQueryProfile();
    sendNodeCompletion(finalQueryProfile);
  }

  public void sendNodeCompletion(ExecutorQueryProfile finalQueryProfile) {
    state = State.DONE;
    lastFragmentStatuses.clear(); // not required any more.
    queryTicket = null;
    sendCompletionMessage(finalQueryProfile);
    firstErrorInQuery = null;
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private void sendCompletionMessage(ExecutorQueryProfile finalQueryProfile) {
    // all fragments are completed.
    final NodeQueryCompletion.Builder completionBuilder =
        NodeQueryCompletion.newBuilder()
            .setId(queryId)
            .setEndpoint(EndpointHelper.getMinimalEndpoint(selfEndpoint.get()))
            .setForeman(foreman)
            .setResultsSent(resultsSent)
            .setFinalNodeQueryProfile(finalQueryProfile);

    if (firstErrorInQuery != null) {
      completionBuilder.setFirstError(firstErrorInQuery);
      completionBuilder.setErrorMajorFragmentId(errorFragmentHandle.getMajorFragmentId());
      completionBuilder.setErrorMinorFragmentId(errorFragmentHandle.getMinorFragmentId());
    }

    expirationTime = System.currentTimeMillis() + evictionDelayMillis;
    final NodeQueryCompletion completion = completionBuilder.build();
    Consumer<StreamObserver<Empty>> consumer =
        observer -> {
          try {
            logger.debug(
                "sending node completion message to foreman {}:{}",
                foreman.getAddress(),
                foreman.getFabricPort());
            maestroServiceClient.nodeQueryComplete(completion, observer);
          } catch (Exception e) {
            observer.onError(e);
          }
        };
    consumer.accept(new RetryingObserver(consumer));
  }

  private void checkIfResultsSent(FragmentStatus status) {
    // root fragment
    if (status.getHandle().getMajorFragmentId() == 0) {
      for (OperatorProfile opProfile : status.getProfile().getOperatorProfileList()) {
        // screen operator
        if (opProfile.getOperatorId() == 0) {
          // If the screen operator received any input batches, assume it sent it too.
          for (StreamProfile streamProfile : opProfile.getInputProfileList()) {
            if (streamProfile.hasBatches() || streamProfile.hasRecords()) {
              resultsSent = true;
            }
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return "queryId "
        + QueryIdHelper.getQueryId(queryId)
        + " state "
        + state
        + " resultsSent "
        + resultsSent
        + " pendingPhases "
        + (queryTicket == null ? 0 : queryTicket.getActivePhaseTickets().size());
  }

  @Override
  public void setQuerySentTime(long querySentTime) {
    this.querySentTime = querySentTime;
  }

  @Override
  public long getQuerySentTime() {
    return querySentTime;
  }

  private class RetryingObserver implements StreamObserver<Empty> {
    Consumer<StreamObserver<Empty>> retryFunction;
    int backoffMillis;

    RetryingObserver(Consumer<StreamObserver<Empty>> retryFunction) {
      this.retryFunction = retryFunction;
      this.backoffMillis = INITIAL_BACKOFF_MILLIS;
      incrementPendingMessages();
    }

    @Override
    public void onNext(Empty o) {
      // no-op
    }

    @Override
    public void onError(Throwable throwable) {
      if (foremanDead) {
        // if foreman is dead, the message can be discarded.
        decrementPendingMessages();
      } else {
        // retry with back-off
        backoffMillis = Integer.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
        logger.warn(
            "sending failure for query {} to maestro failed, will retry after " + "backoff {} ms",
            QueryIdHelper.getQueryId(queryId),
            backoffMillis,
            throwable);
        retryExecutor.schedule(
            () -> retryFunction.accept(this), backoffMillis, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public void onCompleted() {
      decrementPendingMessages();
    }
  }
  ;

  private synchronized void incrementPendingMessages() {
    if (pendingMessages.getAndIncrement() == 0) {
      clusterCoordinator
          .getServiceSet(ClusterCoordinator.Role.COORDINATOR)
          .addNodeStatusListener(foremanDeathListener);
    }
  }

  private synchronized void decrementPendingMessages() {
    if (pendingMessages.decrementAndGet() == 0) {
      clusterCoordinator
          .getServiceSet(ClusterCoordinator.Role.COORDINATOR)
          .removeNodeStatusListener(foremanDeathListener);
    }
  }

  @Override
  public FileCursorManagerFactory getFileCursorManagerFactory() {
    return fileCursorManagerFactory;
  }

  private class ForemanDeathListener implements NodeStatusListener {

    @Override
    public void nodesRegistered(final Set<NodeEndpoint> registered) {}

    @Override
    public void nodesUnregistered(final Set<NodeEndpoint> unregistered) {
      if (unregistered.contains(foreman)) {
        foremanDead = true;
      }
    }
  }
}
