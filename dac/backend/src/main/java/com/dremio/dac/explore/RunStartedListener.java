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
package com.dremio.dac.explore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.NoOpJobStatusListener;
/**
 * A listener that waits until a run has started to return. Used to ensure History includes all items.
 * Collects {@link JobId} of submitted job.
 *
 * We'll catch failures as well as successes so we always countdown (even if job submitted is skipped).
 */
public class RunStartedListener extends NoOpJobStatusListener {
  private final CountDownLatch latch = new CountDownLatch(1);
  private JobId jobId;

  public boolean await(long timeout, TimeUnit unit) throws InterruptedException{
    return latch.await(timeout, unit);
  }

  @Override
  public void jobSubmitted(JobId jobId) {
    this.jobId = jobId;
    latch.countDown();
  }

  @Override
  public void planRelTransform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
  }

  @Override
  public void jobFailed(Exception e) {
    latch.countDown();
  }

  @Override
  public void jobCompleted() {
    latch.countDown();
  }

  @Override
  public void jobCancelled(String reason) {
    latch.countDown();
  }

  public JobId getJobId() {
    return jobId;
  }
}
