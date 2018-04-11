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
package com.dremio.dac.service.errors;

import java.util.List;

import com.dremio.dac.model.job.QueryError;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception for invalid queries
 *
 */
public class InvalidQueryException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  private final Details details;

  public InvalidQueryException(Details details, Throwable cause) {
    super(cause.getMessage(), cause);
    this.details = details;
  }

  public Details getDetails() {
    return details;
  }

  /**
   * Basic information needed to populate the explore page to retry a failed initial preview.
   */
  public static final class Details {
    private final String sql;
    private final List<String> context;
    private final List<QueryError> errors;

    @JsonCreator
    public Details(
        @JsonProperty("sql") String sql,
        @JsonProperty("context") List<String> context,
        @JsonProperty("errors") List<QueryError> errors) {
      this.sql = sql;
      this.context = context;
      this.errors = errors;
    }

    public String getSql() {
      return sql;
    }

    public List<String> getContext() {
      return context;
    }

    public List<QueryError> getErrors() {
      return errors;
    }
  }
}
