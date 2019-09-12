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

import java.util.Objects;

import com.dremio.service.job.proto.JobId;

/**
 * Request for getJob.
 */
public final class GetJobRequest {

  private final JobId jobId;
  private final String userName;
  private final boolean fromStore;

  private GetJobRequest(JobId jobId, String userName, boolean fromStore) {
    this.jobId = jobId;
    this.userName = userName;
    this.fromStore = fromStore;
  }

  JobId getJobId() {
    return jobId;
  }

  String getUserName() {
    return userName;
  }

  boolean isFromStore() {
    return fromStore;
  }

  /**
   * getJob Request builder.
   */
  public static final class Builder {
    private JobId jobId;
    private String userName = null;
    private boolean fromStore = false;

    private Builder() {
    }

    public Builder setJobId(JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public Builder setFromStore(boolean fromStore) {
      this.fromStore = fromStore;
      return this;
    }

    public GetJobRequest build() {
      return new GetJobRequest(jobId, userName, fromStore);
    }
  }

 /**
  * Create a new getJobs request builder.
  *
  * @return new builder
  */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GetJobRequest)) {
      return false;
    }
    GetJobRequest request = (GetJobRequest)obj;
    return Objects.equals(this.getJobId(), request.getJobId())
      && Objects.equals(this.getUserName(), request.getUserName())
      && Objects.equals(this.isFromStore(), request.isFromStore());
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, userName, fromStore);
  }
}
