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
import com.google.common.base.Preconditions;

/**
 * Implements {@link JobData} that holds complete job results
 */
public class JobDataImpl implements JobData {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobDataImpl.class);

  private final JobLoader dataLoader;
  private final JobId jobId;

  private boolean closed;

  /**
   * Create an instance with {@link JobLoader}
   * @param dataLoader
   * @param jobId
   */
  public JobDataImpl(JobLoader dataLoader, JobId jobId) {
    this.dataLoader = Preconditions.checkNotNull(dataLoader);
    this.jobId = Preconditions.checkNotNull(jobId);
  }

  @Override
  public JobDataFragment range(int offset, int limit) {
    loadIfNecessary();
    checkNotClosed();
    return new JobDataFragmentImpl(dataLoader.load(offset, limit), offset, jobId);
  }

  @Override
  public JobDataFragment truncate(int maxRows) {
    loadIfNecessary();
    checkNotClosed();
    return new JobDataFragmentImpl(dataLoader.load(0, maxRows), 0, jobId);
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException(String.format("JobData object for job %s is already closed.", jobId.getId()));
    }
  }

  @Override
  public void loadIfNecessary() {
    dataLoader.waitForCompletion();
  }

  @Override
  public JobId getJobId() {
    return jobId;
  }

  @Override
  public String getJobResultsTable() {
    loadIfNecessary();
    return dataLoader.getJobResultsTable();
  }

  @Override
  public void close() throws Exception {
    closed = true;
  }
}
