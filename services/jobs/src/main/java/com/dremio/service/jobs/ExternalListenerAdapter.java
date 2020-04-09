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

import io.grpc.stub.StreamObserver;

/**
 * Adapts {@link ExternalStatusListener} to {@link StreamObserver}
 */
public class ExternalListenerAdapter implements StreamObserver<JobEvent> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalListenerAdapter.class);
  private final ExternalStatusListener externalStatusListener;

  ExternalListenerAdapter(ExternalStatusListener externalStatusListener) {
    this.externalStatusListener = externalStatusListener;
  }

  @Override
  public void onNext(JobEvent value) {
    switch (value.getEventCase()) {
      case PROGRESS_JOB_SUMMARY:
        externalStatusListener.queryProgressed(value.getProgressJobSummary());
        break;

      case FINAL_JOB_SUMMARY:
        externalStatusListener.queryCompleted(value.getFinalJobSummary());
        break;

      default:
        logger.debug("Unrecognized event: {}", value.getEventCase());
        break;
    }
  }

  @Override
  public void onError(Throwable t) {

  }

  @Override
  public void onCompleted() {

  }
}
