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

package com.dremio.service.jobresults.server;

import java.util.concurrent.atomic.AtomicBoolean;

import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.service.jobresults.JobResultsResponse;

import io.grpc.stub.StreamObserver;

class JobResultsGrpcLocalResponseSender implements ResponseSender {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsGrpcLocalResponseSender.class);
  private StreamObserver<JobResultsResponse> responseStreamObserver;
  private long sequenceId;
  private String queryId;
  private AtomicBoolean sentFailure = new AtomicBoolean(false);

  JobResultsGrpcLocalResponseSender(StreamObserver<JobResultsResponse> responseStreamObserver, long sequenceId,
                                    String queryId) {
    this.responseStreamObserver = responseStreamObserver;
    this.sequenceId = sequenceId;
    this.queryId = queryId;
  }

  @Override
  public void send(Response r) {
    JobResultsResponse.Builder builder = JobResultsResponse.newBuilder();
    builder.setAck(Acks.OK).setSequenceId(sequenceId);
    responseStreamObserver.onNext(builder.build());
  }

  @Override
  public void sendFailure(UserRpcException e) {
    if (sentFailure.compareAndSet(false, true)) {
      logger.error("JobResultsService stream failed with error from result forwarder for queryId {}", queryId, e);
      responseStreamObserver.onError(e);
    }
  }
}
