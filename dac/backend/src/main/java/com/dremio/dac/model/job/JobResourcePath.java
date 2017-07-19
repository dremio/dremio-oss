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
package com.dremio.dac.model.job;

import static java.util.Arrays.asList;

import java.util.List;

import com.dremio.dac.model.common.ResourcePath;
import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Resource Path for job
 */
public class JobResourcePath extends ResourcePath {
  private final JobId jobId;

  public JobResourcePath(JobId path) {
    this.jobId = path;
  }

  @JsonCreator
  public JobResourcePath(String jobId) {
    List<String> path = parse(jobId, "job");
    if (path.size() != 1) {
      throw new IllegalArgumentException("path should be of form: /job/{jobId}, found " + jobId);
    }
    this.jobId = new JobId(path.get(0));
  }

  @Override
  public List<String> asPath() {
    return asList("job", jobId.getId());
  }

  public JobId getJob() {
    return jobId;
  }
}
