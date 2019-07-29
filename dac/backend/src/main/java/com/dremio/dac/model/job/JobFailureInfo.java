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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * Expose job failure info
 */
public class JobFailureInfo {
  private final String message;
  private final JobFailureType type;
  private final List<QueryError> errors;

  @JsonCreator
  public JobFailureInfo(
      @JsonProperty("message") String message,
      @JsonProperty("code") JobFailureType type,
      @JsonProperty("errors") List<QueryError> errors) {
    this.message = message;
    this.type = type;
    this.errors = errors != null ? ImmutableList.copyOf(errors) : ImmutableList.<QueryError> of();
  }

  public String getMessage() {
    return message;
  }

  public JobFailureType getType() {
    return type;
  }

  public List<QueryError> getErrors() {
    return errors;
  }

  @Override
  public String toString() {
    return "JobFailureInfo [message=" + message + ", type=" + type + ", errors=" + errors + "]";
  }
}
