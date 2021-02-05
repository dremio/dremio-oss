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

/**
 * JobData holds a given job partial or complete results.
 */
public interface JobData extends AutoCloseable {

  /**
   * Defines the system property key for the maximum size in bytes for a given JSON serialized value of a cell.
   */
  String MAX_CELL_SIZE_KEY = "cellSizeLimit";

  /**
   * Creates a {@link JobDataFragment} object containing the results in a defined offset and limit row range.
   * <p>
   * If the defined range contains no values, an empty JobDataFragment object
   * containing no results is returned.
   *
   * @param allocator a buffer allocator to accept the result data
   * @param offset    the number of the starting row to include in the output result
   * @param limit     the maximum number of rows to consider in the output result
   *                  based on the starting offset.
   * @return the job results defined in a given offset and limit range
   * @see JobDataFragment
   * @see JobDataWrapper
   */
  JobDataFragment range(BufferAllocator allocator, int offset, int limit);

  /**
   * Create a new data object that truncates the results to at max given rows.
   *
   * @param allocator Allocator to accept data
   * @param maxRows
   */
  JobDataFragment truncate(BufferAllocator allocator, int maxRows);

  /**
   * Gets the job related {@link JobId} that produced the results in this object.
   *
   * @return the job identifier related to the produced results
   */
  JobId getJobId();

  /**
   * Gets the table related path where the job results are stored.
   *
   * @return the path of the table containing this job results
   */
  String getJobResultsTable();
}
