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

import java.util.List;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.service.job.proto.JobId;

/**
 * Holds job results. Could be partial or complete job results.
 */
public interface JobDataFragment extends AutoCloseable {
  /**
   * Get the {@link JobId} of job that produced the results in this object.
   * @return
   */
  JobId getJobId();

  /**
   * Get the number of records.
   * @return
   */
  int getReturnedRowCount();

  /**
   * Get the data fragment schema
   *
   * @return the batch schema
   */
  BatchSchema getSchema();

  /**
   * Get list of record batches
   *
   * @return the list of record batches
   */
  List<RecordBatchHolder> getRecordBatches();

  /**
   * Get the value for a specific column/row
   *
   * @param columnName the column name
   * @param row the row index
   * @return the cell value
   */
  Object extractValue(String columnName, int row);

  @Override
  void close();
}
