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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.common.exceptions.UserException;

/**
 * Unit tests for DremioFlightErrorMapper.
 */
@RunWith(Parameterized.class)
public class TestDremioFlightErrorMapper {
  private UserException.Builder builder;
  private CallStatus expectedCallStatus;

  @Parameterized.Parameters
  public static List<Object[]> testCases() {
    return Arrays.asList(
      new Object[] {UserException.connectionError(), CallStatus.INTERNAL},
      new Object[] {UserException.dataReadError(), CallStatus.INTERNAL},
      new Object[] {UserException.dataWriteError(), CallStatus.INTERNAL},
      new Object[] {UserException.functionError(), CallStatus.INTERNAL},
      new Object[] {UserException.parseError(), CallStatus.INVALID_ARGUMENT},
      new Object[] {UserException.permissionError(), CallStatus.UNAUTHORIZED},
      new Object[] {UserException.planError(), CallStatus.INTERNAL},
      new Object[] {UserException.resourceError(), CallStatus.UNAVAILABLE},
      new Object[] {UserException.systemError(new Exception("Test exception")), CallStatus.INTERNAL},
      new Object[] {UserException.unsupportedError(), CallStatus.INTERNAL},
      new Object[] {UserException.validationError(), CallStatus.INVALID_ARGUMENT},
      new Object[] {UserException.memoryError(), CallStatus.UNAVAILABLE},
      new Object[] {UserException.schemaChangeError(), CallStatus.INTERNAL},
      new Object[] {UserException.ioExceptionError(), CallStatus.UNAVAILABLE},
      new Object[] {UserException.concurrentModificationError(), CallStatus.UNAVAILABLE},
      new Object[] {UserException.invalidMetadataError(), CallStatus.INTERNAL},
      new Object[] {UserException.reflectionError(), CallStatus.INTERNAL},
      new Object[] {UserException.sourceInBadState(), CallStatus.INTERNAL},
      new Object[] {UserException.jsonFieldChangeError(), CallStatus.INTERNAL},
      new Object[] {UserException.resourceTimeoutError(), CallStatus.TIMED_OUT}
    );
  }

  public TestDremioFlightErrorMapper(UserException.Builder builder, CallStatus expectedCallStatus) {
    this.builder = builder;
    this.expectedCallStatus = expectedCallStatus;
  }

  @Test
  public void testDremioParseError() {
    final UserException userEx = builder.buildSilently();
    final FlightRuntimeException flightRuntimeEx =
      DremioFlightErrorMapper.toFlightRuntimeException(userEx);

    assertEquals(flightRuntimeEx.status().code(), expectedCallStatus.code());
  }
}
