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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClient;
import com.dremio.service.maestroservice.MaestroClient;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

/**
 * Tracker for one query in the local Proxy for the maestro service.
 */
class MaestroProxyQueryTracker implements QueryTracker {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(MaestroProxyQueryTracker.class);
  private static final int INITIAL_BACKOFF_MILLIS = 5;
  private static final int MAX_BACKOFF_MILLIS = 60_000;

  private final QueryId queryId;
  private final NodeEndpoint selfEndpoint;
  private final long evictionDelayMillis;
  private final ScheduledThreadPoolExecutor retryExecutor;
  private final ClusterCoordinator clusterCoordinator;
  private final Map<FragmentHandle, FragmentStatus> lastFragmentStatuses = new HashMap<>();
  private final ForemanDeathListener foremanDeathListener = new ForemanDeathListener();

  private State state = State.INVALID;
  private boolean cancelled;
  private MaestroClient maestroServiceClient;
  private JobTelemetryExecutorClient jobTelemetryClient;
  private QueryTicket queryTicket;
  private NodeEndpoint foreman;
  private long expirationTime;
  private boolean resultsSent;
  private DremioPBError firstErrorInQuery;
  private FragmentHandle errorFragmentHandle;
  private volatile boolean foremanDead;
  private AtomicInteger pendingMessages = new AtomicInteger(0);
  private Set<FragmentHandle> pendingFragments = null;

  /**
   * Initialize with the set of fragment handles for the query before
   * starting the query.
   * @param pendingFragments
   */
  @Override
  public void initFragmentsForQuery(Set<FragmentHandle> pendingFragments) {
    Preconditions.checkState(pendingFragments != null && pendingFragments.size() > 0, "Pending " +
      "fragments should be non empty.");
    this.pendingFragments = pendingFragments;
  }

  enum State {
    INVALID,
    STARTED,
    DONE
  }

  MaestroProxyQueryTracker(QueryId queryId, NodeEndpoint selfEndpoint,
                           long evictionDelayMillis,
                           ScheduledThreadPoolExecutor retryExecutor,
                           ClusterCoordinator clusterCoordinator) {
    this.queryId = queryId;
    this.selfEndpoint = EndpointHelper.getMinimalEndpoint(selfEndpoint);
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
  public synchronized boolean tryStart(QueryTicket queryTicket,
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
  public boolean isExpired() {
    return (state == State.INVALID || state == State.DONE) &&
      pendingMessages.get() == 0 &&
      System.currentTimeMillis() > expirationTime;
  }

  static private boolean isTerminal(FragmentState state) {
    return state == FragmentState.FAILED
      || state == FragmentState.FINISHED
      || state == FragmentState.CANCELLED;

  }

  @Override
  public synchronized void refreshFragmentStatus(FragmentStatus fragmentStatus) {
    final FragmentHandle handle = fragmentStatus.getHandle();
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
    profile = ExecutorQueryProfile.newBuilder()
      .setQueryId(queryId)
      .setEndpoint(selfEndpoint)
      .setProgress(buildProgressMetrics(fragmentStatuses))
      .setNodeStatus(queryTicket.getStatus())
      .addAllFragments(fragmentStatuses)
      .build();
    return profile;
  }

  static private QueryProgressMetrics buildProgressMetrics(List<FragmentStatus> fragmentStatuses) {
    long recordCount = 0;
    for (FragmentStatus fragmentStatus : fragmentStatuses) {
      for (OperatorProfile operatorProfile : fragmentStatus.getProfile().getOperatorProfileList()) {
        for (StreamProfile streamProfile : operatorProfile.getInputProfileList()) {
          recordCount += streamProfile.getRecords();
        }
      }
    }
    return QueryProgressMetrics.newBuilder()
      .setRowsProcessed(recordCount)
      .build();
  }

  /**
   * Error from any one of the the MinorFragmentProfile is sufficient to end the query and return the
   * error to the callers. So, once an error from a profile is captured in firstErrorInQuery, errors from other
   * profiles can be cleared. Clearing the errors from profiles helps not consuming heap memory when the
   * error messages are too long especially when the error messages are due to insufficient heap memory.
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

  /**
   * Handle the status change of one fragment.
   *
   * @param fragmentStatus
   */
  @Override
  public void fragmentStatusChanged(FragmentStatus fragmentStatus) {
    Preconditions.checkState(pendingFragments != null, "Pending fragments should have been " +
      "registered before starting the query.");

    final FragmentHandle handle = fragmentStatus.getHandle();
    final MinorFragmentProfile profile = fragmentStatus.getProfile();

    NodeQueryFirstError firstError = null;
    NodeQueryScreenCompletion screenCompletion = null;
    synchronized (this) {
      switch (profile.getState()) {
        case FAILED:
          if (firstErrorInQuery == null) {
            firstErrorInQuery = profile.getError();
            errorFragmentHandle = fragmentStatus.getHandle();

            // propagate the first error.
            firstError = NodeQueryFirstError.newBuilder()
              .setHandle(handle)
              .setEndpoint(selfEndpoint)
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
            screenCompletion = NodeQueryScreenCompletion.newBuilder()
              .setId(handle.getQueryId())
              .setEndpoint(selfEndpoint)
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
            logger.debug("sending screen completion to foreman {}:{}",
              foreman.getAddress(), foreman.getFabricPort());
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
            logger.debug("sending fragment error to foreman {}:{}",
              foreman.getAddress(), foreman.getFabricPort());
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

  private void sendCompletionMessage(ExecutorQueryProfile finalQueryProfile) {
    // all fragments are completed.
    final NodeQueryCompletion.Builder completionBuilder =
        NodeQueryCompletion.newBuilder()
            .setId(queryId)
            .setEndpoint(selfEndpoint)
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
          logger.debug("sending node completion message to foreman {}:{}",
            foreman.getAddress(), foreman.getFabricPort());
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
    return "queryId " + QueryIdHelper.getQueryId(queryId) +
      " state " + state +
      " resultsSent " + resultsSent +
      " pendingPhases " + (queryTicket == null ? 0 : queryTicket.getActivePhaseTickets().size());
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
        logger.warn("sending failure for query {} to maestro failed, will retry after " +
          "backoff {} ms", QueryIdHelper.getQueryId(queryId), backoffMillis, throwable);
        retryExecutor.schedule(() -> retryFunction.accept(this), backoffMillis,
          TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public void onCompleted() {
      decrementPendingMessages();
    }

  };

  private synchronized void incrementPendingMessages() {
    if (pendingMessages.getAndIncrement() == 0) {
      clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).addNodeStatusListener(foremanDeathListener);
    }
  }

  private synchronized void decrementPendingMessages() {
    if (pendingMessages.decrementAndGet() == 0) {
      clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).removeNodeStatusListener(foremanDeathListener);
    }
  }

  private class ForemanDeathListener implements NodeStatusListener {

    @Override
    public void nodesRegistered(final Set<NodeEndpoint> registered) {
    }

    @Override
    public void nodesUnregistered(final Set<NodeEndpoint> unregistered) {
      if (unregistered.contains(foreman)) {
        foremanDead = true;
      }
    }

  }
}
