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

import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.google.common.base.Throwables;
import java.util.concurrent.CountDownLatch;

class MetadataCollectingJobStatusListener implements JobStatusListener {

  /** Need to wait for {@link #metadataCollected(QueryMetadata)} */
  private final CountDownLatch planningCompleteLatch = new CountDownLatch(1);

  private volatile Exception error;
  private volatile boolean cancelled;
  private volatile QueryMetadata metadata;

  /**
   * @return query metadata.
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
      throw Throwables.propagate(error);
    }

    if (cancelled) {
      throw new RuntimeException("Query is cancelled.");
    }
  }

  @Override
  public void metadataCollected(QueryMetadata metadata) {
    this.metadata = metadata;
    releaseLatch();
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
  public void jobCancelled(String reason) {
    cancelled = true;
    releaseLatch();
  }

  private void releaseLatch() {
    planningCompleteLatch.countDown();
  }
}
