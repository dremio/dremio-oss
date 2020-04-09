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
package com.dremio.service.jobs;

import static com.dremio.service.jobs.JobsRpcUtils.getJobsHostname;
import static com.dremio.service.jobs.JobsRpcUtils.getJobsPort;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.ChronicleGrpc.ChronicleBlockingStub;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceBlockingStub;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Client that maintains the lifecycle of a channel over which job RPC requests are made to the jobs service. After the
 * client is {@link #start started}, the {@link #getBlockingStub blocking stub} and {@link #getAsyncStub async stub}
 * are available.
 */
public class JobsClient implements Service {
  private static final Logger logger = LoggerFactory.getLogger(JobsClient.class);

  private final GrpcChannelBuilderFactory grpcFactory;
  private final BufferAllocator allocator;

  private ManagedChannel channel;
  private JobsServiceBlockingStub blockingStub;
  private JobsServiceStub asyncStub;
  private ChronicleBlockingStub chronicleBlockingStub;
  private FlightClient flightClient;
  private Provider<Integer> portProvider;

  JobsClient(GrpcChannelBuilderFactory grpcFactory, Provider<BufferAllocator> allocator, Provider<Integer> portProvider) {
    this.grpcFactory = grpcFactory;
    this.allocator = allocator.get().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    this.portProvider = portProvider;
  }

  @Override
  public void start() {
    ManagedChannelBuilder<?> builder;
    int port = portProvider == null ? getJobsPort() : portProvider.get();
    if (getJobsHostname() == null || getJobsHostname().isEmpty()) {
        builder = JobsRpcUtils.newLocalChannelBuilder(grpcFactory, port);
    } else {
      builder = grpcFactory.newManagedChannelBuilder(getJobsHostname(), port);
    }

    builder.maxInboundMetadataSize(Integer.MAX_VALUE) // Accomodate large error messages like OOM
      .maxInboundMessageSize(Integer.MAX_VALUE) //Since FlightClient shares the channel we need to accommodate FlightServer.MAX_GRPC_MESSAGE_SIZE
      .usePlaintext();

    channel = builder.build();

    blockingStub = JobsServiceGrpc.newBlockingStub(channel);
    asyncStub = JobsServiceGrpc.newStub(channel);
    flightClient = FlightGrpcUtils.createFlightClient(allocator, channel);
    chronicleBlockingStub = ChronicleGrpc.newBlockingStub(channel);

    logger.info("Job channel created to authority '{}'", channel.authority());
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(flightClient, allocator);
    if (channel != null) {
      channel.shutdown();
    }
  }

  /**
   * Get the blocking stub to make RPC requests to jobs service.
   *
   * @return blocking stub
   */
  public JobsServiceGrpc.JobsServiceBlockingStub getBlockingStub() {
    return blockingStub;
  }

  /**
   * Get the async stub to make RPC requests to jobs service.
   *
   * @return async stub
   */
  public JobsServiceGrpc.JobsServiceStub getAsyncStub() {
    return asyncStub;
  }

  /**
   * Get the Arrow Flight Client for data requests to jobs service.
   *
   * @return Flight Client
   */
  public FlightClient getFlightClient() {
    return flightClient;
  }

  /**
   * Get the blocking stub to make RPC requests to Chronicle service
   * @return blocking stub
   */
  public ChronicleGrpc.ChronicleBlockingStub getChronicleBlockingStub() {
    return chronicleBlockingStub;
  }
}
