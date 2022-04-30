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
package com.dremio.exec.maestro;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsRequest;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsResponse;
import com.dremio.service.jobtelemetry.JobTelemetryClient;

import io.grpc.stub.StreamObserver;

/**
 * Gets progress updates from JTS, and notifies the observer.
 */
class ProgressTracker implements AutoCloseable {
  private final UserBitShared.QueryId queryId;
  private final JobTelemetryClient jobTelemetryClient;
  private final MaestroObserver observer;
  private final CountDownLatch responseLatch = new CountDownLatch(1);
  private volatile long prevRecordsProcessed = -1;
  private volatile long prevOutputRecords = -1;

  private volatile StreamObserver<GetQueryProgressMetricsRequest> requestObserver;

  ProgressTracker(UserBitShared.QueryId queryId, JobTelemetryClient jobTelemetryClient,
                  MaestroObserver observer) {
    this.queryId = queryId;
    this.jobTelemetryClient = jobTelemetryClient;
    this.observer = observer;

    subscribeForUpdates();
  }

  private void subscribeForUpdates() {
    StreamObserver<GetQueryProgressMetricsResponse> responseObserver =
      new StreamObserver<GetQueryProgressMetricsResponse>() {
      @Override
      public void onNext(GetQueryProgressMetricsResponse metricsResponse) {
        long recordsProcessed = metricsResponse.getMetrics().getRowsProcessed();
        long outputRecordsCount = metricsResponse.getMetrics().getOutputRecords();
        if (recordsProcessed > prevRecordsProcessed) {
          observer.recordsProcessed(recordsProcessed);
          prevRecordsProcessed = recordsProcessed;
        }
        if (outputRecordsCount > prevOutputRecords) {
          observer.recordsOutput(recordsProcessed);
          prevOutputRecords = outputRecordsCount;
        }
      }

      @Override
      public void onError(Throwable throwable) {
        // TODO: retry ?
        responseLatch.countDown();
      }

      @Override
      public void onCompleted() {
        responseLatch.countDown();
      }
    };

    // send metrics request
    requestObserver =
      jobTelemetryClient.getAsyncStub().getQueryProgressMetrics(responseObserver);
    requestObserver.onNext(
      GetQueryProgressMetricsRequest.newBuilder()
        .setQueryId(queryId)
        .build()
    );
  }

  @Override
  public void close() throws Exception {
    if (requestObserver != null) {
      // terminate request.
      requestObserver.onCompleted();

      // wait on response completion.
      responseLatch.await(1, TimeUnit.SECONDS);
      requestObserver = null;
    }
  }
}
