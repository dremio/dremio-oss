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
package com.dremio.exec.exception;

import java.util.List;

import com.dremio.common.exceptions.JsonAdditionalExceptionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Contextual information needed to transfer schema change information in a {@link UserException} of
 * {@link ErrorType#JSON_FIELD_CHANGE} type.
 */
@JsonTypeName("json-field-change")
public class JsonFieldChangeExceptionContext extends JsonAdditionalExceptionContext {

  private final List<String> originTablePath;
  private final String fieldName;
  private final CompleteType fieldSchema;

  public JsonFieldChangeExceptionContext(@JsonProperty("originTablePath") List<String> originTablePath,
                                         @JsonProperty("fieldName") String fieldName,
                                         @JsonProperty("fieldSchema") CompleteType fieldSchema) {
    this.originTablePath = originTablePath;
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
  }

  /**
   * Get the origin table path.
   *
   * @return A List of Strings representing the origin table path
   */
  public List<String> getOriginTablePath() {
    return originTablePath;
  }

  /**
   * Get the field name.
   *
   * @return The field name as a String.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Get the field schema.
   *
   * @return The field schema as a CompleteType
   */
  public CompleteType getFieldSchema() {
    return fieldSchema;
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.JSON_FIELD_CHANGE;
  }

  /**
   * Deserialize the rawAdditionalContext from a UserException into a new AdditionalExceptionContext.
   *
   * @param ex A UserException containing serialized AdditionalExceptionContext data.
   * @return A new AdditionalExceptionContext of the serialized type.
   */
  public static JsonFieldChangeExceptionContext fromUserException(UserException ex) {
    Preconditions.checkState(ex.getErrorType() == ErrorType.JSON_FIELD_CHANGE, "exception type mismatch");
    return JsonAdditionalExceptionContext.fromUserException(JsonFieldChangeExceptionContext.class, ex);
  }
}
