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
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Contextual information needed to transfer schema change information in a {@link UserException} of
 * {@link ErrorType#SCHEMA_CHANGE} type.
 */
@JsonTypeName("schema-change")
public class SchemaChangeExceptionContext extends JsonAdditionalExceptionContext {

  final private List<String> tableSchemaPath;
  final private BatchSchema newSchema;

  public SchemaChangeExceptionContext(@JsonProperty("tableSchemaPath") List<String> tableSchemaPath,
                                      @JsonProperty("newSchema") BatchSchema newSchema) {
    this.tableSchemaPath = tableSchemaPath;
    this.newSchema = newSchema;
  }

  /**
   * Get the table schema path.
   *
   * @return A List of Strings representing the table schema path
   */
  public List<String> getTableSchemaPath() {
    return tableSchemaPath;
  }

  /**
   * Get the new schema.
   *
   * @return BatchSchema of new schema
   */
  public BatchSchema getNewSchema() {
    return newSchema;
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.SCHEMA_CHANGE;
  }

  /**
   * Deserialize the rawAdditionalContext from a UserException into a new AdditionalExceptionContext.
   *
   * @param ex A UserException containing serialized AdditionalExceptionContext data.
   * @return A new AdditionalExceptionContext of the serialized type.
   */
  public static SchemaChangeExceptionContext fromUserException(UserException ex) {
    Preconditions.checkState(ex.getErrorType() == ErrorType.SCHEMA_CHANGE, "exception type mismatch");
    return JsonAdditionalExceptionContext.fromUserException(SchemaChangeExceptionContext.class, ex);
  }
}
