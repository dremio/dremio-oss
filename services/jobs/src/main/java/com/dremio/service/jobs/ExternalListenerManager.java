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

import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobSummary;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages status listeners for a particular job. Is thread safe, ensuring that any register
 * listener is informed of a completion event, whether or not there is a race.
 */
class ExternalListenerManager {
  private final List<StreamObserver<JobEvent>> statusObservers = new CopyOnWriteArrayList<>();

  private volatile boolean active = true;
  private volatile QueryMetadata queryMetadata;
  private volatile JobSummary jobSummary;

  public synchronized void register(StreamObserver<JobEvent> observer, JobSummary jobSummary) {
    if (!active) {
      sendQueryCompletedEvent(this.jobSummary, observer);
    } else {
      if (queryMetadata != null) {
        observer.onNext(JobEvent.newBuilder().setQueryMetadata(queryMetadata).build());
      }
      statusObservers.add(observer);
      sendQueryProgressedEvent(jobSummary, observer);
    }
  }

  private void sendQueryProgressedEvent(JobSummary jobSummary, StreamObserver<JobEvent> observer) {
    observer.onNext(JobEvent.newBuilder().setProgressJobSummary(jobSummary).build());
  }

  private void sendQueryCompletedEvent(JobSummary jobSummary, StreamObserver<JobEvent> observer) {
    observer.onNext(JobEvent.newBuilder().setFinalJobSummary(jobSummary).build());
    observer.onCompleted();
  }

  public synchronized void queryProgressed(JobSummary jobSummary) {
    if (active) {
      for (StreamObserver<JobEvent> observer : statusObservers) {
        sendQueryProgressedEvent(jobSummary, observer);
      }
    }
  }

  public synchronized void metadataAvailable(QueryMetadata metadata) {
    if (active) {
      for (StreamObserver<JobEvent> observer : statusObservers) {
        observer.onNext(JobEvent.newBuilder().setQueryMetadata(metadata).build());
      }
      queryMetadata = metadata;
    }
  }

  public synchronized void close(JobSummary jobSummary) {
    active = false;
    this.jobSummary = jobSummary;
    for (StreamObserver<JobEvent> observer : statusObservers) {
      sendQueryCompletedEvent(jobSummary, observer);
    }
  }
}
