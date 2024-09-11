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

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.common.util.Retryer;
import com.dremio.datastore.DatastoreException;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileResponse;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.dremio.service.jobtelemetry.PutExecutorProfileRequest;
import com.dremio.service.jobtelemetry.PutPlanningProfileRequest;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/** Implementation of gRPC service for ProfileService. */
public class JobTelemetryServiceImpl extends JobTelemetryServiceGrpc.JobTelemetryServiceImplBase
    implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobTelemetryServiceImpl.class);
  private static final int MAX_RETRIES = 3;

  private final ProfileStore profileStore;
  private final BackgroundProfileWriter bgProfileWriter;
  private final boolean saveFullProfileOnQueryTermination;
  private final Retryer retryer;
  private final ContextMigratingExecutorService executorService;

  @Inject
  JobTelemetryServiceImpl(
      ProfileStore profileStore,
      GrpcTracerFacade tracer,
      @Named("requestThreadPool") ContextMigratingExecutorService executorService) {
    this(profileStore, tracer, false, executorService);
  }

  public JobTelemetryServiceImpl(
      ProfileStore profileStore,
      GrpcTracerFacade tracer,
      boolean saveFullProfileOnQueryTermination,
      ContextMigratingExecutorService executorService) {
    this.profileStore = profileStore;
    this.bgProfileWriter = new BackgroundProfileWriter(profileStore, tracer);
    this.saveFullProfileOnQueryTermination = saveFullProfileOnQueryTermination;
    this.retryer =
        Retryer.newBuilder()
            .retryIfExceptionOfType(DatastoreException.class)
            .setMaxRetries(MAX_RETRIES)
            .build();
    this.executorService = executorService;
  }

  @Override
  public void putQueryPlanningProfile(
      PutPlanningProfileRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug(
        "putQueryPlanningProfile - Request payload size : {} bytes", request.toByteString().size());
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
    logger.debug(
        "putQueryTailProfile - Request payload size : {} bytes", request.toByteString().size());
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
          Status.INTERNAL
              .withDescription(Throwables.getRootCause(ex).getMessage())
              .asRuntimeException());
    }
  }

  @Override
  public void putExecutorProfile(
      PutExecutorProfileRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug(
        "putExecutorProfile - Request payload size : {} bytes", request.toByteString().size());
    try {
      ExecutorQueryProfile profile = request.getProfile();
      Preconditions.checkNotNull(profile.getQueryId());

      // update executor profile.
      profileStore.putExecutorProfile(
          profile.getQueryId(), profile.getEndpoint(), profile, request.getIsFinal());

      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("put executor profile failed " + e.getMessage())
              .asRuntimeException());
    } catch (Exception ex) {
      logger.error("put executor profile failed: ", ex);
      responseObserver.onError(
          Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void getQueryProfile(
      GetQueryProfileRequest request, StreamObserver<GetQueryProfileResponse> responseObserver) {
    logger.debug(
        "getQueryProfile - Request payload size : {} bytes", request.toByteString().size());
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
              .withDescription("Unable to get query profile. " + e.getMessage())
              .asRuntimeException());
    } catch (Exception ex) {
      logger.error("Unable to get query profile.", ex);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(Throwables.getRootCause(ex).getMessage())
              .asRuntimeException());
    }
  }

  private QueryProfile fetchOrBuildMergedProfile(QueryId queryId)
      throws ExecutionException, InterruptedException {
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
  private void saveFullProfileAndDeletePartial(QueryId queryId)
      throws ExecutionException, InterruptedException {
    QueryProfile fullProfile = buildFullProfile(queryId);

    this.retryer.call(
        () -> {
          profileStore.putFullProfile(queryId, fullProfile);
          profileStore.deleteSubProfiles(queryId);
          return null;
        });
  }

  private QueryProfile buildFullProfile(QueryId queryId)
      throws ExecutionException, InterruptedException {
    CompletableFuture<QueryProfile> planningProfileFuture =
        CompletableFuture.supplyAsync(
            () -> (QueryProfile) profileStore.getPlanningProfile(queryId).orElse(null),
            executorService);
    CompletableFuture<QueryProfile> tailProfileFuture =
        CompletableFuture.supplyAsync(
            () -> (QueryProfile) profileStore.getTailProfile(queryId).orElse(null),
            executorService);
    CompletableFuture<Stream<ExecutorQueryProfile>> executorsProfilesFuture =
        CompletableFuture.supplyAsync(
            () -> profileStore.getAllExecutorProfiles(queryId), executorService);

    if (planningProfileFuture.get() == null && tailProfileFuture.get() == null) {
      throw new IllegalArgumentException("Profile not found for the given queryId.");
    }

    return ProfileMerger.merge(
        planningProfileFuture.get(), tailProfileFuture.get(), executorsProfilesFuture.get());
  }

  private boolean isTerminal(QueryState state) {
    return (state == QueryState.COMPLETED
        || state == QueryState.FAILED
        || state == QueryState.CANCELED);
  }

  @Override
  public void deleteProfile(DeleteProfileRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("deleteProfile - Request payload size : {} bytes", request.toByteString().size());
    try {
      // delete profile.
      profileStore.deleteProfile(request.getQueryId());

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
    AutoCloseables.close(bgProfileWriter, profileStore);
  }
}
