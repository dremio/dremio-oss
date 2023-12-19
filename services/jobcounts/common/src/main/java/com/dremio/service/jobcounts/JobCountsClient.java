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
package com.dremio.service.jobcounts;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Client that redirects requests to either the local job service instance (software), or
 * the remote one (service).
 */
public class JobCountsClient implements Service {
  private static final Logger logger = LoggerFactory.getLogger(JobCountsClient.class);

  private final GrpcChannelBuilderFactory grpcFactory;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpoint;

  private ManagedChannel channel;
  private JobCountsServiceGrpc.JobCountsServiceBlockingStub blockingStub;
  private JobCountsServiceGrpc.JobCountsServiceStub asyncStub;
  private JobCountsServiceGrpc.JobCountsServiceFutureStub futureStub;

  public JobCountsClient(GrpcChannelBuilderFactory grpcFactory,
                         Provider<CoordinationProtos.NodeEndpoint> selfEndpoint) {
    this.grpcFactory = grpcFactory;
    this.selfEndpoint = selfEndpoint;
  }

  @Override
  public void start() {
    ManagedChannelBuilder<?> builder;

    if (JobCountsRpcUtils.getJobCountsHostname() == null) {
      builder = JobCountsRpcUtils.newLocalChannelBuilder(grpcFactory,
        selfEndpoint.get().getFabricPort());
    } else {
      builder = grpcFactory.newManagedChannelBuilder(
        JobCountsRpcUtils.getJobCountsHostname(),
        JobCountsRpcUtils.getJobCountsPort());
    }

    channel = builder.maxInboundMetadataSize(Integer.MAX_VALUE)
      .maxInboundMessageSize(Integer.MAX_VALUE)
      .usePlaintext()
      .build();

    blockingStub = JobCountsServiceGrpc.newBlockingStub(channel);
    asyncStub = JobCountsServiceGrpc.newStub(channel);
    futureStub = JobCountsServiceGrpc.newFutureStub(channel);

    logger.info("JobCounts channel created to authority '{}'", channel.authority());
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }

  /**
   * Get the blocking stub to make RPC requests to job Counts service.
   *
   * @return blocking stub
   */
  public JobCountsServiceGrpc.JobCountsServiceBlockingStub getBlockingStub() {
    return blockingStub;
  }

  /**
   * Get the async stub to make RPC requests to job Counts service.
   *
   * @return async stub
   */
  public JobCountsServiceGrpc.JobCountsServiceStub getAsyncStub() {
    return asyncStub;
  }

  /**
   * Get the future stub to make RPC requests to job Counts service.
   *
   * @return future stub
   */
  public JobCountsServiceGrpc.JobCountsServiceFutureStub getFutureStub() {
    return futureStub;
  }
}
