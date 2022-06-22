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
package com.dremio.exec.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.dremio.exec.proto.UserBitShared;

/**
 * Test cases for the Utilities class.
 */
public class UtilitiesTest {
  private static Stream<Arguments> testGetWorkloadTypeFromClientInfo() {
    return Stream.of(
      Arguments.of("",              UserBitShared.WorkloadType.UNKNOWN),
      Arguments.of("jdbC",          UserBitShared.WorkloadType.JDBC),
      Arguments.of("JDBC",          UserBitShared.WorkloadType.JDBC),
      Arguments.of("Java",          UserBitShared.WorkloadType.JDBC),
      Arguments.of("JAVA",          UserBitShared.WorkloadType.JDBC),
      Arguments.of("oDbc",          UserBitShared.WorkloadType.ODBC),
      Arguments.of("ODBC",          UserBitShared.WorkloadType.ODBC),
      Arguments.of("C++",           UserBitShared.WorkloadType.ODBC),
      Arguments.of("c++",           UserBitShared.WorkloadType.ODBC),
      Arguments.of("Arrow Flight",  UserBitShared.WorkloadType.FLIGHT),
      Arguments.of("ARROW FLIGHT",  UserBitShared.WorkloadType.FLIGHT)
    );
  }

  /**
   * Test getting the workload type from the client RPC info.
   */
  @ParameterizedTest(name = "ClientInfo name: {0}, WorkloadType: {1}")
  @MethodSource
  public void testGetWorkloadTypeFromClientInfo(String clientInfoName, UserBitShared.WorkloadType workloadType) {
    final UserBitShared.RpcEndpointInfos clientInfo = UserBitShared.RpcEndpointInfos.newBuilder()
      .setName(clientInfoName)
      .build();

    assertEquals(workloadType, Utilities.getByClientType(clientInfo));
  }

  private static Stream<Arguments> testHumanReadableWorkloadName() {
    return Stream.of(
      Arguments.of(UserBitShared.WorkloadType.UNKNOWN, "Other"),
      Arguments.of(UserBitShared.WorkloadType.DDL, "DDL"),
      Arguments.of(UserBitShared.WorkloadType.INTERNAL_RUN, "Internal Run"),
      Arguments.of(UserBitShared.WorkloadType.JDBC, "JDBC"),
      Arguments.of(UserBitShared.WorkloadType.ODBC, "ODBC"),
      Arguments.of(UserBitShared.WorkloadType.ACCELERATOR, "Reflections"),
      Arguments.of(UserBitShared.WorkloadType.REST, "REST"),
      Arguments.of(UserBitShared.WorkloadType.UI_PREVIEW, "UI Preview"),
      Arguments.of(UserBitShared.WorkloadType.UI_RUN, "UI Run"),
      Arguments.of(UserBitShared.WorkloadType.UI_DOWNLOAD, "UI Download"),
      Arguments.of(UserBitShared.WorkloadType.FLIGHT, "Flight"));
  }

  @ParameterizedTest(name = "WorkloadType: {0}, Human Readable Name: {1}")
  @MethodSource
  public void testHumanReadableWorkloadName(UserBitShared.WorkloadType workloadType, String humanReadableName) {
    assertEquals(humanReadableName, Utilities.getHumanReadableWorkloadType(workloadType));
  }
}
