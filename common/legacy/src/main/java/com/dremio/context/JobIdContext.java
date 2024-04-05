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
package com.dremio.context;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;

/** Job ID context. */
public class JobIdContext implements SerializableContext {
  public static final RequestContext.Key<JobIdContext> CTX_KEY =
      RequestContext.newKey("job_ctx_key");

  // Note: These are public for use in annotating traces.
  public static final Metadata.Key<String> JOB_ID_HEADER_KEY =
      Metadata.Key.of("x-dremio-job-id", Metadata.ASCII_STRING_MARSHALLER);

  @Deprecated // Will be removed as part of DX-67499
  public static final Metadata.Key<String> DEPRECATED_JOB_ID_HEADER_KEY =
      Metadata.Key.of("jobId", Metadata.ASCII_STRING_MARSHALLER);

  private final String jobId;

  public JobIdContext(String jobId) {
    this.jobId = jobId;
  }

  public String getJobId() {
    return jobId;
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(JOB_ID_HEADER_KEY.name(), jobId);
  }
}
