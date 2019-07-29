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

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.metadata.QueryMetadata;
/**
 * A listener which notifies user about the progress of a job
 *
 * TODO: This interface is a subset of {@link AttemptObserver} except the method {@link #jobSubmitted(JobId)}.
 * If we end up taking more methods from {@link AttemptObserver}, change this to a subclass of {@link AttemptObserver}.
 */
public interface JobStatusListener {

  /**
   * Called when the job is submitted to the query engine
   * @param jobId the job id
   */
  void jobSubmitted(JobId jobId);

  void planRelTransform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken);

  /**
   * Called when all query metadata has been collected.
   * @param metadata
   */
  void metadataCollected(QueryMetadata metadata);

  /**
   * Called when the job has failed
   *
   * @param e the exception thrown by the job
   */
  void jobFailed(Exception e);

  /**
   * Called when the job has completed successfully
   */
  void jobCompleted();

  /**
   * Called when the job was cancelled
   */
  void jobCancelled(String reason);
}
