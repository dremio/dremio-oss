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
package com.dremio.sabot.op.project;

import com.dremio.common.exceptions.UserException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * Utility class for the handling (serialization, deserialization) of errors occurred during
 * expression evaluation in a projector.
 */
public final class ProjectErrorUtils {

  public static final String OUTPUT_FIELD_ID = "PROJECTION_OUTPUT_FIELD_ID";
  private static final ObjectReader READER = new ObjectMapper().readerFor(ErrorDetails.class);
  private static final ObjectMapper WRITER = new ObjectMapper();

  // Internal representation of an entry in an error vector
  private static class ErrorDetails {

    @JsonProperty(value = "m", required = true)
    private String message;

    @JsonProperty("i")
    @JsonInclude(Include.NON_EMPTY)
    private List<Integer> fieldIds = new ArrayList<>();

    @JsonProperty("n")
    @JsonInclude(Include.NON_EMPTY)
    private List<String> fieldNames = new ArrayList<>();
  }

  private static Text serialize(ErrorDetails errorDetails) {
    try {
      return new Text(WRITER.writeValueAsString(errorDetails));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize an internal ErrorDetails instance", e);
    }
  }

  private static ErrorDetails deserialize(Text text) {
    try {
      return READER.readValue(text.toString());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to deserialize an internal ErrorDetails instance", e);
    }
  }

  /**
   * Writes an Arrow Text value containing the projection evaluation exception details in a
   * serialized format
   *
   * @param exception the Exception to be serialized
   * @return Text value containing the serialized string
   */
  public static Text serializeEvaluationException(Exception exception) {
    final ErrorDetails errorDetails = new ErrorDetails();
    errorDetails.message = exception.getMessage();

    if (exception instanceof UserException) {
      int fieldId = fieldIdFromException(((UserException) exception));
      if (fieldId != -1) {
        errorDetails.fieldIds.add(fieldId);
      }
    }
    return serialize(errorDetails);
  }

  private static int fieldIdFromException(UserException exception) {
    String fieldIdContextString =
        exception.getContextStrings().stream()
            .filter(s -> s.startsWith(OUTPUT_FIELD_ID))
            .findFirst()
            .orElse(OUTPUT_FIELD_ID);

    fieldIdContextString = fieldIdContextString.substring(OUTPUT_FIELD_ID.length()).trim();
    if (!fieldIdContextString.isEmpty()) {
      return Integer.parseInt(fieldIdContextString);
    }

    return -1;
  }

  /**
   * Resolves the field ids saved in the serialized error into string field names and creates
   * another serialized instance with the new error. It removes the resolved field ids from the
   * error.
   *
   * @param error the original serialized error containing the field ids
   * @param fieldIdResolver the resolver function that turns the field ids into string field names
   * @return the newly created serialized error containing the resolved field names
   */
  public static Text resolveFieldIds(Text error, Function<Integer, String> fieldIdResolver) {
    ErrorDetails errorDetails = deserialize(error);
    errorDetails.fieldIds.stream().map(fieldIdResolver).forEach(errorDetails.fieldNames::add);
    errorDetails.fieldIds.clear();
    return serialize(errorDetails);
  }

  /**
   * Overwrites the list of field ids in the serialized error with the specified field id and
   * creates another serialized instance with it
   *
   * @param error the original serialized error
   * @param fieldId the new field id to be used in the new serialized error
   * @return the newly created serialized error containing the specified field id
   */
  public static Text overwriteFieldId(Text error, int fieldId) {
    ErrorDetails errorDetails = deserialize(error);
    errorDetails.fieldIds.clear();
    errorDetails.fieldIds.add(fieldId);
    return serialize(errorDetails);
  }

  /**
   * Concatenates the specified serialized errors by concatenating the messages, lists of field ids
   * and lists of field names in the order of {@code firstError} + {@code secondError}.
   *
   * @param firstError the first error to be used for the concatenation; might be {@code null}
   * @param secondError the second error to be used for the concatenation; must not be {@code null}
   * @return {@code firstError} + {@code secondError} or {@code secondError} if {@code firstError}
   *     is {@code null}
   */
  public static Text concatErrors(Text firstError, Text secondError) {
    if (firstError == null) {
      return secondError;
    }
    ErrorDetails existingErrorDetails = deserialize(firstError);
    ErrorDetails newErrorDetails = deserialize(secondError);
    existingErrorDetails.message += ", " + newErrorDetails.message;
    existingErrorDetails.fieldIds.addAll(newErrorDetails.fieldIds);
    existingErrorDetails.fieldNames.addAll(newErrorDetails.fieldNames);
    return serialize(existingErrorDetails);
  }

  /**
   * Returns an iterator of {@link ProjectionError}s parsed from the specified error vector.
   *
   * @param errorVector the error vector which the {@link ProjectionError}s are parsed from
   * @param fieldIdResolver used for resolving the remaining field ids to field names
   * @return the iterator of the {@link ProjectionError} objects parsed from the error vector
   */
  public static Iterator<ProjectionError> parseErrors(
      VarCharVector errorVector, Function<Integer, String> fieldIdResolver) {
    return new Iterator<>() {
      private int remainingErrors = errorVector.getValueCount() - errorVector.getNullCount();
      private int index = 0;
      private final int valueCount = errorVector.getValueCount();

      @Override
      public boolean hasNext() {
        return remainingErrors > 0;
      }

      @Override
      public ProjectionError next() {
        for (; index < valueCount; ++index) {
          Text errorValue = errorVector.getObject(index);
          if (errorValue != null) {
            --remainingErrors;
            ErrorDetails errorDetails = deserialize(errorValue);
            errorDetails.fieldIds.stream()
                .map(fieldIdResolver)
                .forEach(errorDetails.fieldNames::add);
            return new ProjectionError(errorDetails.message, index, errorDetails.fieldNames);
          }
        }
        throw new NoSuchElementException("No more errors found in the error vector");
      }
    };
  }

  /**
   * The throwable object representing an error happened during a projection. The original
   * projection error message can be retrieved by using {@link ProjectionError#getMessage()}. (This
   * exception object has no stack trace and cannot be suppressed.)
   */
  public static class ProjectionError extends Exception {

    private final int idxInBatch;
    private final List<String> fieldNames;

    private ProjectionError(String message, int idxBatch, List<String> fieldNames) {
      super(message, null, false, false);
      this.idxInBatch = idxBatch;
      this.fieldNames = fieldNames;
    }

    /**
     * @return the index of the error in the current batch
     */
    public int getIdxInBatch() {
      return idxInBatch;
    }

    /**
     * @return the list of field names which the error is related to
     */
    public List<String> getFieldNames() {
      return fieldNames;
    }
  }

  private ProjectErrorUtils() {}
}
