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

import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;

/**
 * A listener which notifies user about the progress of a job
 *
 * TODO: This interface is a subset of {@link AttemptObserver} except the method {@link #jobStarted(JobId)}.
 * If we end up taking more methods from {@link AttemptObserver}, change this to a subclass of {@link AttemptObserver}.
 */
public interface JobStatusListener {

  /**
   * A job status listener that does nothing.
   */
  JobStatusListener NO_OP = new JobStatusListener() {
  };

  /**
   * Called when the {@link JobRequest} has been handled by the service. A {@link Job} entry is now present in the kvstore.
   */
  default void jobSubmitted() {}

  /**
   * Called if {@link #jobSubmitted()} failed
   * @param e the exception thrown
   */
  default void submissionFailed(RuntimeException e) {}

  /**
   * Called when all query metadata has been collected.
   * @param metadata
   */
  default void metadataCollected(QueryMetadata metadata) {}

  /**
   * Called when the job has failed
   *
   * @param e the exception thrown by the job
   */
  default void jobFailed(Exception e) {}

  /**
   * Called when the job has completed successfully
   */
  default void jobCompleted() {}

  /**
   * Called when the job was cancelled
   */
  default void jobCancelled(String reason) {}
}
