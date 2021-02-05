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

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.dac.explore.model.Column;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * JobDataFragment holds a given job results fragments.
 */
public interface JobDataFragment extends AutoCloseable {

  /**
   * Defines the system property key for the maximum size in bytes for a given JSON serialized value of a cell.
   */
  String MAX_CELL_SIZE_KEY = "cellSizeLimit";

  /**
   * Gets the job related {@link JobId} that produced the results in this object.
   *
   * @return the job identifier related to the produced results
   */
  @JsonIgnore
  JobId getJobId();

  /**
   * Gets the list of {@link Column} objects considered in this job fragment results.
   *
   * @return the list of columns considered in this job fragment results
   * @see Column
   */
  List<Column> getColumns();

  /**
   * Gets the number of returned rows in this job fragment results.
   *
   * @return the number of returned rows in this job fragment results.
   */
  int getReturnedRowCount();

  /**
   * Gets the {@link Column} object containing all its metadata based on a given column name.
   *
   * @param name a column name
   * @return a {@link Column} object containing all its metadata
   * @see Column
   */
  @JsonIgnore
  Column getColumn(String name);

  /**
   * Retrieves the specific string column value for a given row in this job result fragment,
   * based on the defined column name.
   * <p>
   * For complex types such as Date, Time and Datetime, the column string value will be serialized
   * into their JSON representation format {@link JobDataFragmentWrapper#extractString(String, int)}.
   *
   * @param column a column name
   * @param index  a row index in the job fragment result
   * @return the specific string value for a given column contained in a defined row.
   * Notice that this value can be null
   * @see JobDataFragmentWrapper
   */
  @JsonIgnore
  String extractString(String column, int index);

  /**
   * Grab a value from the dataset out of the provided column in the given row index.
   *
   * @param column - name of column
   * @param index  - row index in dataset
   * @return - value contained in this cell of the dataset, note this can be null
   */
  @JsonIgnore
  Object extractValue(String column, int index);

  @JsonIgnore
  DataType extractType(String column, int index);

  /**
   * Gets the list of arrow schema fields held in this job fragment results.
   *
   * @return the list of arrow schema fields held in this job fragment results.
   * @see Field
   */
  @JsonIgnore
  List<Field> getFields();

  /**
   * Gets the list of record batches holders held in this job fragment results.
   *
   * @return the list of record batches holders held in this job fragment results.
   * @see RecordBatchHolder
   */
  @JsonIgnore
  List<RecordBatchHolder> getRecordBatches();

  /**
   * Closes and releases all resources held by this JobDataFragment object.
   */
  @Override
  void close();
}
