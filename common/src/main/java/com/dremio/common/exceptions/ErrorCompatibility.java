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

import java.util.EnumSet;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;

/**
 * Converts newly added error types to types compatible with the existing C or ODBC clients.
 * <p>
 * <ul>
 * <li>{@link ErrorType#JSON_FIELD_CHANGE} to {@link ErrorType#DATA_READ}</li>
 * <li>{@link ErrorType#SCHEMA_CHANGE} to {@link ErrorType#DATA_READ}</li>
 * <li>{@link ErrorType#OUT_OF_MEMORY} to {@link ErrorType#RESOURCE}</li>
 * <li>{@link ErrorType#IO_EXCEPTION} to {@link ErrorType#RESOURCE}</li>
 * <li>{@link ErrorType#CONCURRENT_MODIFICATION} to {@link ErrorType#RESOURCE}</li>
 * <li>{@link ErrorType#INVALID_DATASET_METADATA} to {@link ErrorType#RESOURCE}</li>
 * <li>{@link ErrorType#REFLECTION_ERROR} to {@link ErrorType#SYSTEM}</li>
 * </ul>
 */
public class ErrorCompatibility {

  private static final EnumSet<ErrorType> incompatibleErrorTypes =
      EnumSet.of(ErrorType.SCHEMA_CHANGE,
          ErrorType.OUT_OF_MEMORY,
          ErrorType.IO_EXCEPTION,
          ErrorType.CONCURRENT_MODIFICATION,
          ErrorType.INVALID_DATASET_METADATA,
          ErrorType.REFLECTION_ERROR,
          ErrorType.SOURCE_BAD_STATE,
          ErrorType.JSON_FIELD_CHANGE);

  private static boolean needsConversion(final DremioPBError error) {
    return incompatibleErrorTypes.contains(error.getErrorType());
  }

  public static DremioPBError convertIfNecessary(final DremioPBError error) {
    if (!needsConversion(error)) {
      return error;
    }

    switch (error.getErrorType()) {
      case JSON_FIELD_CHANGE:
      case SCHEMA_CHANGE:
      case SOURCE_BAD_STATE:
        return DremioPBError.newBuilder(error).setErrorType(ErrorType.DATA_READ).build();
      case REFLECTION_ERROR:
        return DremioPBError.newBuilder(error).setErrorType(ErrorType.SYSTEM).build();
      case OUT_OF_MEMORY:
      case IO_EXCEPTION:
      case CONCURRENT_MODIFICATION:
      case INVALID_DATASET_METADATA:
        return DremioPBError.newBuilder(error).setErrorType(ErrorType.RESOURCE).build();
      default:
        // we most likely forgot to add the type to this switch block
        throw new IllegalStateException("Unhandled error type: " + error.getErrorType());
    }
  }

  /**
   * Goes through all error objects and converts them when necessary
   * @param result query result
   * @return new result object if any conversion was done, or the original if no conversion was needed
   */
  public static UserBitShared.QueryResult convertIfNecessary(final QueryResult result) {
    if (result.getErrorCount() == 0) {
      return result; // nothing to convert
    }

    final int numErrors = result.getErrorCount();
    boolean convert = false;
    for (int i = 0; i < numErrors && !convert; i++) {
      if (needsConversion(result.getError(i))) {
        convert = true;
      }
    }

    if (!convert) {
      return result; // nothing to convert
    }

    final QueryResult.Builder resultBuilder = QueryResult.newBuilder(result);
    for (int i = 0; i < numErrors; i++) {
      resultBuilder.setError(i, convertIfNecessary(result.getError(i)));
    }

    return resultBuilder.build();
  }

}
