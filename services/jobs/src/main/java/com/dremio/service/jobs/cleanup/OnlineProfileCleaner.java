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
package com.dremio.service.jobs.cleanup;

import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import javax.inject.Provider;

/** Online profile deletion using Job Telemetry Service Schedule in the background */
public class OnlineProfileCleaner extends ExternalCleaner {
  private JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub jobTelemetryServiceStub;

  public OnlineProfileCleaner(final Provider<JobTelemetryClient> jobTelemetryClientProvider) {
    this.jobTelemetryServiceStub = jobTelemetryClientProvider.get().getBlockingStub();
  }

  @Override
  public void doGo(JobAttempt jobAttempt) {
    jobTelemetryServiceStub.deleteProfile(
        DeleteProfileRequest.newBuilder()
            .setQueryId(AttemptIdUtils.fromString(jobAttempt.getAttemptId()).toQueryId())
            .build());
  }
}
