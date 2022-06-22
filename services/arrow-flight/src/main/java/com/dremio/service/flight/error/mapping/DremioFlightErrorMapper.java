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
package com.dremio.service.flight.error.mapping;

import java.util.LinkedHashSet;
import java.util.List;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;

import com.dremio.common.exceptions.UserException;

/**
 * Error mapper to map Dremio Rpc ErrorType to Arrow Flight CallStatus.
 */
public final class DremioFlightErrorMapper {
  private DremioFlightErrorMapper() {}

  /**
   * Inspects the ErrorType of a UserException and returns a corresponding FlightRuntimeException
   * with the correct FlightStatusCode in the CallStatus.
   *
   * @param userException the UserException from Dremio.
   * @return a FlightRuntimeException with a corresponding CallStatus, the original UserException
   *         and the message from the UserException.
   */
  public static FlightRuntimeException toFlightRuntimeException(UserException userException) {
    CallStatus status;

    switch(userException.getErrorType()) {
      case PARSE:
      case VALIDATION:
        status = CallStatus.INVALID_ARGUMENT;
        break;
      case PERMISSION:
        status = CallStatus.UNAUTHORIZED;
        break;
      case RESOURCE:
      case OUT_OF_MEMORY:
      case IO_EXCEPTION:
      case CONCURRENT_MODIFICATION:
        status = CallStatus.UNAVAILABLE;
        break;
      case RESOURCE_TIMEOUT:
        status = CallStatus.TIMED_OUT;
        break;
      default:
        status = CallStatus.INTERNAL;
    }

    // We add the context inside #withDescription to provide the additional information that exists in certain scenarios
    // like the start/end of a line/column in an invalid query when a validation/parse error happens.
    final List<String> userExceptionContext = userException.getContextStrings();
    if (userExceptionContext != null && !userExceptionContext.isEmpty()) {
      return status.withCause(userException)
        .withDescription(userException.getMessage() +
          "\n" +  // better readability
          String.join("\n", new LinkedHashSet<>(userException.getContextStrings())))  // avoid repeated K=Vs
        .toRuntimeException();
    } else {
      return status.withCause(userException)
        .withDescription(userException.getMessage())
        .toRuntimeException();
    }
  }
}
