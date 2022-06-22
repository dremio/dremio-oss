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
package com.dremio.dac.model.job;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.SessionId;

/**
 * Holds job results. Could be partial or complete job results.
 */
public interface JobData extends AutoCloseable {

  /**
   * Approximate maximum getReturnedRowCount of the JSON serialized value of a cell.
   */
  String MAX_CELL_SIZE_KEY = "cellSizeLimit";

  /**
   * Create a data object that contains the results in given range. If the range contains no values, a JobData object
   * containing no results is returned.
   *
   * @param allocator Allocator to accept data
   * @param offset Number of starting row to include in output
   * @param limit Max number of rows starting from offset.
   * @return
   */
  JobDataFragment range(BufferAllocator allocator, int offset, int limit);

  /**
   * Create a new data object that truncates the results to at max given rows.
   * @param allocator Allocator to accept data
   * @param maxRows
   */
  JobDataFragment truncate(BufferAllocator allocator, int maxRows);

  /**
   * Get the {@link JobId} of job that produced the results in this object.
   * @return
   */
  JobId getJobId();

  /**
   * Get the {@link SessionId} of job that produced the results in this object.
   * @return
   */
  SessionId getSessionId();

  /**
   * Return the table path where the job results are stored.
   * @return
   */
  String getJobResultsTable();
}
