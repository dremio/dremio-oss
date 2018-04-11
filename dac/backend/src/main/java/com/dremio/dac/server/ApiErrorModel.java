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
package com.dremio.dac.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Structured type for including details about an error for the Rest API.
 *
 */
public class ApiErrorModel<T> extends GenericErrorMessage {
  /**
   * Various error codes
   */
  public enum ErrorType {
    INITIAL_PREVIEW_ERROR,
    NEW_DATASET_QUERY_EXCEPTION,
    INVALID_QUERY
  }

  // error code
  private ErrorType code;
  // extra data specific to the particular code string added to this error
  private T details;

  @JsonCreator
  public ApiErrorModel(
      @JsonProperty("code") ErrorType code,
      @JsonProperty("message") String message,
      @JsonProperty("stackTrace") String[] stackTrace,
      @JsonProperty("details") T details) {
    // for null 2nd param, no generic moreInfo string as there is structured details
    super(message, null, stackTrace);
    this.code = Preconditions.checkNotNull(code);
    this.details = details;
  }

  public ErrorType getCode() {
    return code;
  }

  public T getDetails() {
    return details;
  }
}
