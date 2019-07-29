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

import com.dremio.dac.explore.model.Column;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Holds job results. Could be partial or complete job results.
 */
public interface JobDataFragment extends AutoCloseable {

  /**
   * Approximate maximum getReturnedRowCount of the JSON serialized value of a cell.
   */
  String MAX_CELL_SIZE_KEY = "cellSizeLimit";

  /**
   * Get the {@link JobId} of job that produced the results in this object.
   * @return
   */
  @JsonIgnore
  JobId getJobId();

  /**
   * Get the list of columns in job results.
   * @return
   */
  List<Column> getColumns();

  /**
   * Get the number of records.
   * @return
   */
  int getReturnedRowCount();

  /**
   * Get metadata of column with given name.
   * @param name
   * @return
   */
  @JsonIgnore
  Column getColumn(String name);

  /**
   * Grab a value from the dataset out of the provided column in the given row index.
   * For complex types, they will be serialized into their JSON representation.
   *
   * @param column - name of column
   * @param index - row index in dataset
   * @return - value contained in this cell of the dataset, note this can be null
   */
  @JsonIgnore
  String extractString(String column, int index);

  /**
   * Grab a value from the dataset out of the provided column in the given row index.
   *
   * @param column - name of column
   * @param index - row index in dataset
   * @return - value contained in this cell of the dataset, note this can be null
   */
  @JsonIgnore
  Object extractValue(String column, int index);

  @JsonIgnore
  DataType extractType(String column, int index);
}
