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
package com.dremio.common.exceptions;

import com.dremio.exec.proto.UserBitShared;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Contextual information for both OUT_OF_MEMORY or RESOURCE error types {@link UserException} of
 * {@link UserBitShared.DremioPBError.ErrorType#OUT_OF_MEMORY} type.
 */
@JsonTypeName("OutOfMemoryOrResource")
public class OutOfMemoryOrResourceExceptionContext extends JsonAdditionalExceptionContext {
  public enum MemoryType {
    UNKNOWN_TYPE,
    DIRECT_MEMORY,
    HEAP_MEMORY
  }

  private final MemoryType memoryType;
  private final String additionalInfo;

  public OutOfMemoryOrResourceExceptionContext(
      @JsonProperty("memoryType") MemoryType memoryType,
      @JsonProperty("additionalInfo") String additionalInfo) {
    this.memoryType = memoryType;
    this.additionalInfo = additionalInfo;
  }

  @Override
  public UserBitShared.DremioPBError.ErrorType getErrorType() {
    return UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY;
  }

  public MemoryType getMemoryType() {
    return this.memoryType;
  }

  public String getAdditionalInfo() {
    return this.additionalInfo;
  }

  public static OutOfMemoryOrResourceExceptionContext fromUserException(UserException ex) {
    Preconditions.checkState(
        (ex.getErrorType() == UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY
            || ex.getErrorType() == UserBitShared.DremioPBError.ErrorType.RESOURCE),
        "exception type mismatch");
    return JsonAdditionalExceptionContext.fromUserException(
        OutOfMemoryOrResourceExceptionContext.class, ex);
  }
}
