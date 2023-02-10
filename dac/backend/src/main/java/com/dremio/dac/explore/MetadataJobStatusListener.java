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

import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

class MetadataJobStatusListener implements JobStatusListener {

  /**
   * Need to wait for jobId being set and {@link #metadataCollected(QueryMetadata)}
   */
  private final CountDownLatch latch = new CountDownLatch(2);

  private final DatasetTool datasetTool;
  private final VirtualDatasetUI newDataset;
  private final FromBase from;
  private volatile JobId jobId;
  private volatile QueryMetadata metadata;
  private volatile Exception error;
  private volatile boolean cancelled;

  MetadataJobStatusListener(DatasetTool datasetTool, VirtualDatasetUI newDataset, FromBase from) {
    Preconditions.checkArgument(datasetTool != null, "datasetTool can't be null.");
    Preconditions.checkArgument(newDataset != null, "newDataset can't be null.");
    Preconditions.checkArgument(from != null, "from can't be null.");
    this.datasetTool = datasetTool;
    this.newDataset = newDataset;
    this.from = from;
  }

  /**
   * Create a thread to wait for the job Id set and {@link #metadataCollected(QueryMetadata)}.
   * Apply metadata and save the dataset one the wait it done.
   */
  public void waitToApplyMetadataAndSaveDataset() {
    Thread t = new Thread(() -> applyMetadataAndSaveDataset());
    t.start();
  }

  private void applyMetadataAndSaveDataset() {
    try {
      latch.await();
    } catch (final InterruptedException ex) {
      Throwables.propagate(ex);
    }

    if (error != null) {
      throw Throwables.propagate(error);
    }

    if (cancelled) {
      throw new RuntimeException("Query is cancelled.");
    }

    try {
      if (jobId != null && metadata != null) {
        datasetTool.applyQueryMetaToDatasetAndSave(jobId, metadata, newDataset, from);
      }
    } catch (NamespaceException | JobNotFoundException e) {
      // This is tmp.UNTITLED dataset, ignore exceptions
    }
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
    latch.countDown();
  }

  @Override
  public void metadataCollected(QueryMetadata metadata) {
    this.metadata = metadata;
    latch.countDown();
  }

  @Override
  public void jobFailed(Exception e) {
    error = e;
    latch.notifyAll();
  }

  @Override
  public void jobCompleted() {
    latch.notifyAll();
  }

  @Override
  public void jobCancelled(String reason) {
    cancelled = true;
    latch.notifyAll();
  }
}
