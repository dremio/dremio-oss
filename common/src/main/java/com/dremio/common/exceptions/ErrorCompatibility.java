/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;

/**
 * converts newly added error types to types compatible with the existing c/odbc clients.<br>
 * SCHEMA_CHANGE -> DATA_READ
 * OUT_OF_MEMORY -> RESOURCE
 */
public class ErrorCompatibility {

  private static boolean needsConversion(final DremioPBError error) {
    final ErrorType type = error.getErrorType();

    return type == ErrorType.SCHEMA_CHANGE || type == ErrorType.OUT_OF_MEMORY
        || type == ErrorType.IO_EXCEPTION;
  }

  public static DremioPBError convertIfNecessary(final DremioPBError error) {
    if (!needsConversion(error)) {
      return error;
    }

    final ErrorType type = error.getErrorType();
    if (type == ErrorType.SCHEMA_CHANGE) {
      return DremioPBError.newBuilder(error).setErrorType(ErrorType.DATA_READ).build();
    } else {
      return DremioPBError.newBuilder(error).setErrorType(ErrorType.RESOURCE).build();
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
