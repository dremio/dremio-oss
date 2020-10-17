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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.proto.UserBitShared;

/**
 * Test cases for the Utilities class.
 */
@RunWith(Enclosed.class)
public class UtilitiesTest {

  /**
   * Test getting the workload type from the client RPC info.
   */
  @RunWith(Parameterized.class)
  public static class ClientInfoWorkloadTypeTest {

    @Parameterized.Parameters(name = "ClientInfo name: {0}, WorkloadType: {1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
        new Object[]{"",              UserBitShared.WorkloadType.UNKNOWN},
        new Object[]{"jdbC",          UserBitShared.WorkloadType.JDBC},
        new Object[]{"JDBC",          UserBitShared.WorkloadType.JDBC},
        new Object[]{"Java",          UserBitShared.WorkloadType.JDBC},
        new Object[]{"JAVA",          UserBitShared.WorkloadType.JDBC},
        new Object[]{"oDbc",          UserBitShared.WorkloadType.ODBC},
        new Object[]{"ODBC",          UserBitShared.WorkloadType.ODBC},
        new Object[]{"C++",           UserBitShared.WorkloadType.ODBC},
        new Object[]{"c++",           UserBitShared.WorkloadType.ODBC},
        new Object[]{"Arrow Flight",  UserBitShared.WorkloadType.FLIGHT},
        new Object[]{"ARROW FLIGHT",  UserBitShared.WorkloadType.FLIGHT}
      );
    }

    private final String clientInfoName;
    private final UserBitShared.WorkloadType workloadType;

    public ClientInfoWorkloadTypeTest(String clientInfoName, UserBitShared.WorkloadType workloadType) {
      this.clientInfoName = clientInfoName;
      this.workloadType = workloadType;
    }

    @Test
    public void testGetWorkloadTypeFromClientInfo() {
      final UserBitShared.RpcEndpointInfos clientInfo = UserBitShared.RpcEndpointInfos.newBuilder()
        .setName(clientInfoName)
        .build();

      assertEquals(workloadType, Utilities.getByClientType(clientInfo));
    }
  }

  @RunWith(Parameterized.class)
  public static class HumanReadableWorkloadTypeTest {
    @Parameterized.Parameters(name = "WorkloadType: {0}, Human Readable Name: {1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
        new Object[] {UserBitShared.WorkloadType.UNKNOWN, "Other"},
        new Object[] {UserBitShared.WorkloadType.DDL, "DDL"},
        new Object[] {UserBitShared.WorkloadType.INTERNAL_RUN, "Internal Run"},
        new Object[] {UserBitShared.WorkloadType.JDBC, "JDBC"},
        new Object[] {UserBitShared.WorkloadType.ODBC, "ODBC"},
        new Object[] {UserBitShared.WorkloadType.ACCELERATOR, "Reflections"},
        new Object[] {UserBitShared.WorkloadType.REST, "REST"},
        new Object[] {UserBitShared.WorkloadType.UI_PREVIEW, "UI Preview"},
        new Object[] {UserBitShared.WorkloadType.UI_RUN, "UI Run"},
        new Object[] {UserBitShared.WorkloadType.UI_DOWNLOAD, "UI Download"},
        new Object[] {UserBitShared.WorkloadType.FLIGHT, "Flight"});
    }

    private final UserBitShared.WorkloadType workloadType;
    private final String humanReadableName;

    public HumanReadableWorkloadTypeTest(UserBitShared.WorkloadType workloadType, String humanReadableName) {
      this.workloadType = workloadType;
      this.humanReadableName = humanReadableName;
    }

    @Test
    public void testHumanReadableWorkloadName() {
      assertEquals(humanReadableName, Utilities.getHumanReadableWorkloadType(workloadType));
    }
  }

}
