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

/** Interface that allows JobData object to load Job. */
public interface JobLoader {

  /**
   * Return {@link RecordBatches} containing the records for given offset and limit.
   *
   * @param offset Starting record number in query results (starts with 0)
   * @param limit Number of records starting from the offset.
   * @return
   */
  RecordBatches load(int offset, int limit);

  /** Wait for the job to complete. */
  void waitForCompletion();

  /**
   * Return the table path where the job results are store.
   *
   * @return
   */
  String getJobResultsTable();
}
