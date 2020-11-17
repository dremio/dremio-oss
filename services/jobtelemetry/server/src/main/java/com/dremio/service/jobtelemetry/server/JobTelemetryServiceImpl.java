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
package com.dremio.service.jobtelemetry.server;

import java.util.Optional;
import java.util.function.Consumer;

import com.dremio.common.AutoCloseables;
import com.dremio.common.nodes.EndpointHelper;
import com.dremio.common.util.Retryer;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.datastore.DatastoreException;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileResponse;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsRequest;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsResponse;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.dremio.service.jobtelemetry.PutExecutorProfileRequest;
import com.dremio.service.jobtelemetry.PutPlanningProfileRequest;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of gRPC service for ProfileService.
 */
public class JobTelemetryServiceImpl extends JobTelemetryServiceGrpc.JobTelemetryServiceImplBase implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobTelemetryServiceImpl.class);
  private static final int METRICS_PUBLISH_FREQUENCY_MILLIS = 2500;
  private static final int MAX_RETRIES = 3;

  private final MetricsStore metricsStore;
  private final ProfileStore profileStore;
  private final ProgressMetricsPublisher progressMetricsPublisher;
  private final BackgroundProfileWriter bgProfileWriter;
  private final boolean saveFullProfileOnQueryTermination;
  private Retryer retryer;

  @Inject
  JobTelemetryServiceImpl(MetricsStore metricsStore, ProfileStore profileStore, GrpcTracerFacade tracer) {
    this(metricsStore, profileStore, tracer, false,
      METRICS_PUBLISH_FREQUENCY_MILLIS);
  }

  JobTelemetryServiceImpl(MetricsStore metricsStore, ProfileStore profileStore, GrpcTracerFacade tracer,
                          boolean saveFullProfileOnQueryTermination) {
    this(metricsStore, profileStore, tracer, saveFullProfileOnQueryTermination,
      METRICS_PUBLISH_FREQUENCY_MILLIS);
  }

  public JobTelemetryServiceImpl(MetricsStore metricsStore, ProfileStore profileStore, GrpcTracerFacade tracer,
                          boolean saveFullProfileOnQueryTermination,
                          int metricsPublishFrequencyMillis) {
    this.metricsStore = metricsStore;
    this.profileStore = profileStore;
    this.progressMetricsPublisher = new ProgressMetricsPublisher(metricsStore,
      metricsPublishFrequencyMillis);
    this.bgProfileWriter = new BackgroundProfileWriter(profileStore, tracer);
    this.saveFullProfileOnQueryTermination = saveFullProfileOnQueryTermination;
    this.retryer = new Retryer.Builder()
      .retryIfExceptionOfType(DatastoreException.class)
      .setMaxRetries(MAX_RETRIES)
      .build();
  }

  @Override
  public void putQueryPlanningProfile(
    PutPlanningProfileRequest request, StreamObserver<Empty> responseObserver) {
    try {
      Preconditions.checkNotNull(request.getQueryId());

      profileStore.putPlanningProfile(request.getQueryId(), request.getProfile());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
        Status.INVALID_ARGUMENT
          .withDescription("put planning profile failed " + e.getMessage())
          .asRuntimeException());
    } catch (Exception ex) {
      logger.error("put planning profile failed", ex);
      responseObserver.onError(
        Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void putQueryTailProfile(
    PutTailProfileRequest request, StreamObserver<Empty> responseObserver) {
    try {
      QueryId queryId = request.getQueryId();
      Preconditions.checkNotNull(queryId);

      // update tail profile.
      profileStore.putTailProfile(queryId, request.getProfile());

      // TODO: ignore errors ??
      if (saveFullProfileOnQueryTermination) {
        saveFullProfileAndDeletePartial(queryId);
      }
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
        Status.INVALID_ARGUMENT
          .withDescription("put tail profile failed " + e.getMessage())
          .asRuntimeException());
    } catch (Exception ex) {
      logger.error("put tail profile failed", ex);
      responseObserver.onError(
        Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void putExecutorProfile(
    PutExecutorProfileRequest request, StreamObserver<Empty> responseObserver) {
    try {
      ExecutorQueryProfile profile = request.getProfile();
      Preconditions.checkNotNull(profile.getQueryId());

      // update progress metrics.
      putProgressMetrics(profile);

      // update executor profile.
      profileStore.putExecutorProfile(profile.getQueryId(), profile.getEndpoint(),
        profile);

      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("put executor profile failed " + e.getMessage())
              .asRuntimeException());
    } catch (Exception ex) {
      logger.error("put executor profile failed", ex);
      responseObserver.onError(
          Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  private void putProgressMetrics(ExecutorQueryProfile profile) {
    if(logger.isDebugEnabled()) {
      logger.debug("Updating progress metrics for query {}", QueryIdHelper.getQueryId(profile.getQueryId()));
    }
    metricsStore.put(profile.getQueryId(), EndpointHelper.getMinimalString(profile.getEndpoint()), profile.getProgress());
  }

  @Override
  public StreamObserver<GetQueryProgressMetricsRequest> getQueryProgressMetrics(
      StreamObserver<GetQueryProgressMetricsResponse> responseObserver) {
    return new StreamObserver<GetQueryProgressMetricsRequest>() {
      private boolean subscribed;
      private QueryId queryId;
      private Consumer<CoordExecRPC.QueryProgressMetrics> consumer =
        metrics -> {
          synchronized (responseObserver) {
            responseObserver.onNext(
              GetQueryProgressMetricsResponse.newBuilder()
                .setMetrics(metrics)
                .build()
            );
          }
        };

      @Override
      public void onNext(GetQueryProgressMetricsRequest request) {
        if (!subscribed) {
          try {
            queryId = request.getQueryId();
            Preconditions.checkNotNull(queryId);
            progressMetricsPublisher.addSubscriber(request.getQueryId(), consumer);
            subscribed = true;
          } catch (IllegalArgumentException e) {
            responseObserver.onError(
              Status.INVALID_ARGUMENT
                .withDescription("fetch query progress metrics failed " + e.getMessage())
                .asRuntimeException());
          } catch (Exception ex) {
            logger.error("fetch query progress metrics failed", ex);
            responseObserver.onError(
              Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
          }
        }
      }

      @Override
      public void onError(Throwable throwable) {
        if (subscribed) {
          // unsubscribe from the publisher.
          progressMetricsPublisher.removeSubscriber(queryId, consumer, false);
        }
      }

      @Override
      public void onCompleted() {
        if (subscribed) {
          // unsubscribe from the publisher.
          try {
            progressMetricsPublisher.removeSubscriber(queryId, consumer, true);
          } catch (Exception ex) {
            // ignore error.
            logger.error("publishing final metrics failed", ex);
          }
        }
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void getQueryProfile(
    GetQueryProfileRequest request, StreamObserver<GetQueryProfileResponse> responseObserver) {
    try {
      QueryId queryId = request.getQueryId();
      Preconditions.checkNotNull(queryId);

      QueryProfile mergedProfile = fetchOrBuildMergedProfile(queryId);
      responseObserver.onNext(
          GetQueryProfileResponse.newBuilder().setProfile(mergedProfile).build());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("get query profile failed " + e.getMessage())
              .asRuntimeException());
    } catch (Exception ex) {
      logger.error("get query profile failed", ex);
      responseObserver.onError(
          Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  private QueryProfile fetchOrBuildMergedProfile(QueryId queryId) {
    Optional<QueryProfile> fullProfile = profileStore.getFullProfile(queryId);
    if (fullProfile.isPresent()) {
      return fullProfile.get();
    }

    QueryProfile mergedProfile = buildFullProfile(queryId);
    // persist the merged profile, if in a terminal state
    if (isTerminal(mergedProfile.getState())) {
      bgProfileWriter.tryWriteAsync(queryId, mergedProfile);
    }
    return mergedProfile;
  }

  // build and save the full profile, delete the sub-profiles and metrics.
  private void saveFullProfileAndDeletePartial(QueryId queryId) {
    QueryProfile fullProfile = buildFullProfile(queryId);

    this.retryer.call(() -> {
      profileStore.putFullProfile(queryId, fullProfile);
      profileStore.deleteSubProfiles(queryId);
      metricsStore.delete(queryId);
      return null;
    });

  }

  private QueryProfile buildFullProfile(QueryId queryId) {
    QueryProfile planningProfile = profileStore.getPlanningProfile(queryId).orElse(null);
    QueryProfile tailProfile = profileStore.getTailProfile(queryId).orElse(null);
    if (planningProfile == null && tailProfile == null) {
      throw new IllegalArgumentException("profile not found for the given queryId");
    }

    return ProfileMerger.merge(
      planningProfile,
      tailProfile,
      profileStore.getAllExecutorProfiles(queryId)
    );
  }

  private boolean isTerminal(QueryState state) {
    return (state == QueryState.COMPLETED
        || state == QueryState.FAILED
        || state == QueryState.CANCELED);
  }

  @Override
  public void deleteProfile(
    DeleteProfileRequest request, StreamObserver<Empty> responseObserver) {
    try {
      // delete profile.
      profileStore.deleteProfile(request.getQueryId());
      metricsStore.delete(request.getQueryId());

      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
        Status.INVALID_ARGUMENT
          .withDescription("delete profile failed " + e.getMessage())
          .asRuntimeException());
    } catch (Exception ex) {
      logger.error("delete profile failed", ex);
      responseObserver.onError(
        Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  int getNumInprogressWrites() {
    return bgProfileWriter.getNumInprogressWrites();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(bgProfileWriter, progressMetricsPublisher, metricsStore,
      profileStore);
  }

}
