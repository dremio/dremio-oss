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

import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;

/** Provides Job Result info for jobs. */
public interface JobResultInfoProvider {

  String JOB_RESULTS = "job_results";

  JobResultInfoProvider NOOP =
      new JobResultInfoProvider() {
        @Override
        public Optional<JobResultInfo> getJobResultInfo(String jobId, String username) {
          return Optional.empty();
        }

        @Override
        public Optional<QueryProfile> getProfile(String jobId, String username) {
          return Optional.empty();
        }
      };

  /**
   * Get Job Result info for given job id if job is complete.
   *
   * @param jobId job id
   * @param username username
   * @return optional batch schema
   */
  Optional<JobResultInfo> getJobResultInfo(String jobId, String username);

  /**
   * Get the QueryProfile for the last attempt of the given jobId.
   *
   * @param jobId job id
   * @param username username to run under
   * @return optional QueryProfile
   */
  Optional<QueryProfile> getProfile(String jobId, String username);

  /**
   * Check if a table is job results table.
   *
   * @param tableSchemaPath path components of the table
   * @return ture if the table is a job results table
   */
  static boolean isJobResultsTable(List<String> tableSchemaPath) {
    return tableSchemaPath.size() == 3
        && "sys".equalsIgnoreCase(tableSchemaPath.get(0))
        && JOB_RESULTS.equalsIgnoreCase(tableSchemaPath.get(1));
  }

  /**
   * Check if a user with username is the owner of a job results table.
   *
   * @param tableSchemaPath path components of the table
   * @param username username
   * @return ture if the user is the owner of the job
   */
  default boolean isJobResultsTableOwner(List<String> tableSchemaPath, String username) {
    Preconditions.checkArgument(
        tableSchemaPath.size() == 3, "Job results table path must have 3 components");
    Optional<JobResultInfo> jobResultInfo = getJobResultInfo(tableSchemaPath.get(2), username);
    return jobResultInfo.isPresent();
  }

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
