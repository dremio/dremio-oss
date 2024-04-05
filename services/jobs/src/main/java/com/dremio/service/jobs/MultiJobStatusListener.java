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

import com.dremio.service.jobs.metadata.proto.QueryMetadata;

/** Implementation of {@link JobStatusListener} that wraps multiple listeners */
public class MultiJobStatusListener implements JobStatusListener {

  private final JobStatusListener[] listeners;

  public MultiJobStatusListener(JobStatusListener... listeners) {
    this.listeners = listeners;
  }

  @Override
  public void jobSubmitted() {
    for (JobStatusListener listener : listeners) {
      listener.jobSubmitted();
    }
  }

  @Override
  public void submissionFailed(RuntimeException e) {
    for (JobStatusListener listener : listeners) {
      listener.submissionFailed(e);
    }
  }

  @Override
  public void metadataCollected(QueryMetadata metadata) {
    for (JobStatusListener listener : listeners) {
      listener.metadataCollected(metadata);
    }
  }

  @Override
  public void jobFailed(Exception e) {
    for (JobStatusListener listener : listeners) {
      listener.jobFailed(e);
    }
  }

  @Override
  public void jobCompleted() {
    for (JobStatusListener listener : listeners) {
      listener.jobCompleted();
    }
  }

  @Override
  public void jobCancelled(String reason) {
    for (JobStatusListener listener : listeners) {
      listener.jobCancelled(reason);
    }
  }
}
