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
package com.dremio.exec.server;

import java.util.List;
import java.util.Optional;

import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * Provides Job Result info for jobs.
 */
public interface JobResultInfoProvider {

  /**
   * Get Job Result info for given job id if job is complete.
   *
   * @param jobId    job id
   * @param username username
   * @return optional batch schema
   */
  Optional<JobResultInfo> getJobResultInfo(String jobId, String username);

  JobResultInfoProvider NOOP = (jobId, username) -> Optional.empty();

  class JobResultInfo {
    private final List<String> resultDatasetPath;
    private final BatchSchema batchSchema;

    public JobResultInfo(List<String> resultDatasetPath, BatchSchema batchSchema) {
      this.resultDatasetPath = Preconditions.checkNotNull(resultDatasetPath);
      this.batchSchema = Preconditions.checkNotNull(batchSchema);
    }

    public List<String> getResultDatasetPath() {
      return resultDatasetPath;
    }

    public BatchSchema getBatchSchema() {
      return batchSchema;
    }
  }
}
