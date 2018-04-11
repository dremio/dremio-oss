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
package com.dremio.common.exceptions;

import java.util.List;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Contextual information needed to recover from {@link UserException} of {@link ErrorType#INVALID_DATASET_METADATA}
 * type.
 */
@JsonTypeName("invalid-metadata")
public class InvalidMetadataErrorContext implements AdditionalExceptionContext {

  private final List<List<String>> pathsToRefresh;

  public InvalidMetadataErrorContext(@JsonProperty("pathsToRefresh") List<List<String>> pathsToRefresh) {
    this.pathsToRefresh = pathsToRefresh;
  }

  /**
   * Get the list of dataset schema paths that should be refreshed.
   *
   * @return list of dataset schema paths that should be refreshed
   */
  public List<List<String>> getPathsToRefresh() {
    return pathsToRefresh;
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.INVALID_DATASET_METADATA;
  }
}
