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

import java.util.concurrent.CountDownLatch;

import com.dremio.common.DeferredException;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;

/**
 * Internal Job Loader
 */
public class InternalJobLoader implements JobLoader {

  private final DeferredException exception;
  private final CountDownLatch completionLatch;
  private final JobId id;
  private final JobResultsStore jobResultsStore;
  private final LegacyIndexedStore<JobId, JobResult> store;

  public InternalJobLoader(DeferredException exception, CountDownLatch completionLatch, JobId id,
                           JobResultsStore jobResultsStore, LegacyIndexedStore<JobId, JobResult> store) {
    super();
    this.exception = exception;
    this.completionLatch = completionLatch;
    this.id = id;
    this.jobResultsStore = jobResultsStore;
    this.store = store;
  }

  @Override
  public RecordBatches load(int offset, int limit) {
    try {
      completionLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      exception.addException(ex);
    }

    exception.throwNoClearRuntime();
    return jobResultsStore.loadJobData(id, store.get(id), offset, limit);
  }

  @Override
  public void waitForCompletion() {
    try {
      completionLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      exception.addException(ex);
    }
    exception.throwNoClearRuntime();
  }

  @Override
  public String getJobResultsTable() {
    return jobResultsStore.getJobResultsTableName(id);
  }
}
