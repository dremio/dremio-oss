/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.explore;

import java.util.concurrent.CountDownLatch;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.google.common.base.Throwables;

class MetadataCollectingJobStatusListener implements JobStatusListener {

  /**
   * Need to wait for {@link #metadataCollectected(QueryMetadata)}
   */
  private final CountDownLatch planningCompleteLatch = new CountDownLatch(1);

  private volatile Exception error;
  private volatile boolean cancelled;
  private volatile QueryMetadata metadata;

  /**
   * Get query metadata.
   * @return
   */
  public QueryMetadata getMetadata() {
    waitForPlanningInfo();
    return metadata;
  }

  private void waitForPlanningInfo() {
    try {
      planningCompleteLatch.await();
    } catch (final InterruptedException ex) {
      Throwables.propagate(ex);
    }

    if (error != null) {
      Throwables.propagate(error);
    }

    if (cancelled) {
      throw new RuntimeException("Query is cancelled.");
    }
  }

  @Override
  public void jobSubmitted(JobId jobId) {
  }

  @Override
  public void planRelTansform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
  }

  @Override
  public void metadataCollected(QueryMetadata metadata) {
    this.metadata = metadata;
    planningCompleteLatch.countDown();
  }

  @Override
  public void jobFailed(Exception e) {
    error = e;
    releaseLatch();
  }

  @Override
  public void jobCompleted() {
    releaseLatch();
  }

  @Override
  public void jobCancelled() {
    cancelled = true;
    releaseLatch();
  }

  private void releaseLatch() {
    planningCompleteLatch.countDown();
  }
}
