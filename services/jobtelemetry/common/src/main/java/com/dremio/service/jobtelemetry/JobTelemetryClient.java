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
package com.dremio.service.jobtelemetry;

import static com.dremio.telemetry.api.metrics.MeterProviders.newCounterProvider;

import com.dremio.common.util.Retryer;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client that redirects requests to either the local job service instance (software), or the remote
 * one (service).
 */
public class JobTelemetryClient implements Service {
  private static final Logger logger = LoggerFactory.getLogger(JobTelemetryClient.class);
  private static final int MAX_RETRIES = 3;
  private static final Set<Status.Code> retriableGrpcStatuses =
      ImmutableSet.of(
          Status.UNAVAILABLE.getCode(),
          Status.UNKNOWN.getCode(),
          Status.DEADLINE_EXCEEDED.getCode());

  private final Retryer retryer;
  private final Retryer exponentiaRetryer;
  private final GrpcChannelBuilderFactory grpcFactory;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpoint;
  private final Meter.MeterProvider<Counter> suppressedErrorCounter;

  private ManagedChannel channel;
  private JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub blockingStub;
  private JobTelemetryServiceGrpc.JobTelemetryServiceStub asyncStub;
  private JobTelemetryServiceGrpc.JobTelemetryServiceFutureStub futureStub;

  public JobTelemetryClient(
      GrpcChannelBuilderFactory grpcFactory,
      Provider<CoordinationProtos.NodeEndpoint> selfEndpoint) {
    this.grpcFactory = grpcFactory;
    this.selfEndpoint = selfEndpoint;
    suppressedErrorCounter =
        newCounterProvider(
            "JobTelemetryService.error.suppressed", "Counts the number of JTS Suppressed errors.");
    this.retryer =
        Retryer.newBuilder()
            .setWaitStrategy(Retryer.WaitStrategy.FLAT, 1000, 1000)
            .retryOnExceptionFunc(this::isRetriableException)
            .setMaxRetries(MAX_RETRIES)
            .build();
    this.exponentiaRetryer =
        Retryer.newBuilder()
            .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 1000, 10_000)
            .retryOnExceptionFunc(this::isRetriableException)
            .setMaxRetries(MAX_RETRIES)
            .build();
  }

  @Override
  public void start() {
    ManagedChannelBuilder<?> builder;

    if (JobTelemetryRpcUtils.getJobTelemetryHostname() == null) {
      builder =
          JobTelemetryRpcUtils.newLocalChannelBuilder(
              grpcFactory, selfEndpoint.get().getFabricPort());
    } else {
      builder =
          grpcFactory.newManagedChannelBuilder(
              JobTelemetryRpcUtils.getJobTelemetryHostname(),
              JobTelemetryRpcUtils.getJobTelemetryPort());
    }

    channel =
        builder
            .maxInboundMetadataSize(Integer.MAX_VALUE)
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .usePlaintext()
            .build();

    blockingStub = JobTelemetryServiceGrpc.newBlockingStub(channel);
    asyncStub = JobTelemetryServiceGrpc.newStub(channel);
    futureStub = JobTelemetryServiceGrpc.newFutureStub(channel);

    logger.info("JobTelemetry channel created to authority '{}'", channel.authority());
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }

  /**
   * Get the blocking stub to make RPC requests to job telemetry service.
   *
   * @return blocking stub
   */
  public JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub getBlockingStub() {
    return blockingStub;
  }

  /**
   * Get the async stub to make RPC requests to job telemetry service.
   *
   * @return async stub
   */
  public JobTelemetryServiceGrpc.JobTelemetryServiceStub getAsyncStub() {
    return asyncStub;
  }

  /**
   * Get the future stub to make RPC requests to job telemetry service.
   *
   * @return future stub
   */
  public JobTelemetryServiceGrpc.JobTelemetryServiceFutureStub getFutureStub() {
    return futureStub;
  }

  public Retryer getRetryer() {
    return retryer;
  }

  public Retryer getExponentiaRetryer() {
    return exponentiaRetryer;
  }

  private boolean isRetriableException(Throwable e) {
    if (e instanceof StatusRuntimeException) {
      if (retriableGrpcStatuses.contains(((StatusRuntimeException) e).getStatus().getCode())) {
        return true;
      }
    }
    if (e instanceof CompletionException) {
      if (e.getCause() instanceof TimeoutException) {
        return true;
      }

      if (e.getCause() instanceof StatusRuntimeException) {
        if (retriableGrpcStatuses.contains(
            ((StatusRuntimeException) e.getCause()).getStatus().getCode())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the counter for Suppressed Error Metric
   *
   * @return counter
   */
  public Meter.MeterProvider<Counter> getSuppressedErrorCounter() {
    return suppressedErrorCounter;
  }
}
