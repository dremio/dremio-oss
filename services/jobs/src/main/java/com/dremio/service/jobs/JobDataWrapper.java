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
package com.dremio.service.jobs;

import com.dremio.service.job.proto.JobId;

/**
 * Wrap {@code JobData} instance into another instance
 */
public class JobDataWrapper implements JobData {
  private final JobData delegate;

  public JobDataWrapper(JobData delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public JobDataFragment range(int offset, int limit) {
    return delegate.range(offset, limit);
  }

  @Override
  public JobDataFragment truncate(int maxRows) {
    return delegate.truncate(maxRows);
  }

  @Override
  public JobId getJobId() {
    return delegate.getJobId();
  }

  @Override
  public String getJobResultsTable() {
    return delegate.getJobResultsTable();
  }

  @Override
  public void loadIfNecessary() {
    delegate.loadIfNecessary();
  }
}
