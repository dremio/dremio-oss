/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.explore.model;

import java.util.List;

import com.dremio.dac.model.job.QueryError;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;


/**
 * SQL Analyze API Response object to return for SQL query validation.
 * Wrapper for a list of QueryError objects.
 */
public class ValidationResponse {

  private ImmutableList<QueryError> sqlErrors;

  @JsonCreator
  public ValidationResponse(@JsonProperty("errors") List<QueryError> sqlErrors) {
    this.sqlErrors = ImmutableList.copyOf(sqlErrors);
  }

  /**
   * Get list of errors.
   */
  public ImmutableList<QueryError> getErrors() {
    return sqlErrors;
  }

  @Override
  public String toString() {
    return "ValidationResponse{" +
      "sqlErrors=" + sqlErrors +
      '}';
  }
}
