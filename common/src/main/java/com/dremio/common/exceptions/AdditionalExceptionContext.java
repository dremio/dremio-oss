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

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.ByteString;

/**
 * Contextual information specific to a certain error type. This is carried with a user exception.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface AdditionalExceptionContext {
  /**
   * Get error type of this context.
   *
   * @return error type
   */
  @JsonIgnore
  ErrorType getErrorType();

  /**
   * Serialize this additional context information.
   *
   * @return serialized bytes
   */
  ByteString toByteString();
}
