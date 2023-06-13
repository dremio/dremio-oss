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
package com.dremio.service.jobs;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.QueryType;


/**
 * Tests for Utilities classes in the Jobs module.
 */
@RunWith(Enclosed.class)
public class TestJobUtils {

  /**
   * Tests for mapping QueryType between Protobuf 2 and 3.
   */
  @RunWith(Parameterized.class)
  public static class QueryTypeBufStuffMappingTest {
    private final com.dremio.service.job.proto.QueryType pb2Type;
    private final QueryType pb3Type;

    @Parameterized.Parameters(name = "PB2 QueryType:{0}, PB3 QueryType:{1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
        new Object[] {com.dremio.service.job.proto.QueryType.UNKNOWN,             QueryType.UNKNOWN},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_RUN,              QueryType.UI_RUN},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_PREVIEW,          QueryType.UI_PREVIEW},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_INTERNAL_PREVIEW, QueryType.UI_INTERNAL_PREVIEW},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_INTERNAL_RUN,     QueryType.UI_INTERNAL_RUN},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_EXPORT,           QueryType.UI_EXPORT},
        new Object[] {com.dremio.service.job.proto.QueryType.ODBC,                QueryType.ODBC},
        new Object[] {com.dremio.service.job.proto.QueryType.JDBC,                QueryType.JDBC},
        new Object[] {com.dremio.service.job.proto.QueryType.REST,                QueryType.REST},
        new Object[] {com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE,  QueryType.ACCELERATOR_CREATE},
        new Object[] {com.dremio.service.job.proto.QueryType.ACCELERATOR_DROP,    QueryType.ACCELERATOR_DROP},
        new Object[] {com.dremio.service.job.proto.QueryType.PREPARE_INTERNAL,    QueryType.PREPARE_INTERNAL},
        new Object[] {com.dremio.service.job.proto.QueryType.UI_INITIAL_PREVIEW,  QueryType.UI_INITIAL_PREVIEW},
        new Object[] {com.dremio.service.job.proto.QueryType.FLIGHT,              QueryType.FLIGHT},
        new Object[] {com.dremio.service.job.proto.QueryType.D2D,                 QueryType.D2D});
    }

    public QueryTypeBufStuffMappingTest(com.dremio.service.job.proto.QueryType pb2Type, QueryType pb3Type) {
      this.pb2Type = pb2Type;
      this.pb3Type = pb3Type;
    }

    @Test
    public void testToBuf() {
      assertEquals(pb3Type, JobsProtoUtil.toBuf(pb2Type));
    }

    @Test
    public void testToStuff() {
      assertEquals(pb2Type, JobsProtoUtil.toStuff(pb3Type));
    }
  }

  /**
   * Test getting the query type from the client RPC info.
   */
  @RunWith(Parameterized.class)
  public static class ClientInfoQueryTypeTest {

    @Parameterized.Parameters(name = "ClientInfo name: {0}, QueryType: {1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
        new Object[]{"",              com.dremio.service.job.proto.QueryType.UNKNOWN},
        new Object[]{"jdbC",          com.dremio.service.job.proto.QueryType.JDBC},
        new Object[]{"JDBC",          com.dremio.service.job.proto.QueryType.JDBC},
        new Object[]{"Java",          com.dremio.service.job.proto.QueryType.JDBC},
        new Object[]{"JAVA",          com.dremio.service.job.proto.QueryType.JDBC},
        new Object[]{"oDbc",          com.dremio.service.job.proto.QueryType.ODBC},
        new Object[]{"ODBC",          com.dremio.service.job.proto.QueryType.ODBC},
        new Object[]{"C++",           com.dremio.service.job.proto.QueryType.ODBC},
        new Object[]{"c++",           com.dremio.service.job.proto.QueryType.ODBC},
        new Object[]{"Arrow Flight",  com.dremio.service.job.proto.QueryType.FLIGHT},
        new Object[]{"ARROW FLIGHT",  com.dremio.service.job.proto.QueryType.FLIGHT},
        new Object[]{"Dremio-to-Dremio",           com.dremio.service.job.proto.QueryType.D2D}
      );
    }

    private final String clientInfoName;
    private final com.dremio.service.job.proto.QueryType queryType;

    public ClientInfoQueryTypeTest(String clientInfoName, com.dremio.service.job.proto.QueryType queryType) {
      this.clientInfoName = clientInfoName;
      this.queryType = queryType;
    }

    @Test
    public void testGetQueryTypeFromClientInfo() {
      final UserBitShared.RpcEndpointInfos clientInfo = UserBitShared.RpcEndpointInfos.newBuilder()
        .setName(clientInfoName)
        .build();

      assertEquals(queryType, QueryTypeUtils.getQueryType(clientInfo));
    }
  }

  /**
   * Test getting the query type from the workload type.
   */
  @RunWith(Parameterized.class)
  public static class WorkloadromQueryTypeTest {

    @Parameterized.Parameters(name = "QueryType name: {0}, WorkloadType: {1}, WorkloadClass: {2}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
        new Object[]{com.dremio.service.job.proto.QueryType.UNKNOWN,              UserBitShared.WorkloadType.UNKNOWN,           UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_PREVIEW,           UserBitShared.WorkloadType.UI_PREVIEW,        UserBitShared.WorkloadClass.NRT},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_INITIAL_PREVIEW,   UserBitShared.WorkloadType.INTERNAL_PREVIEW,  UserBitShared.WorkloadClass.NRT},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_INTERNAL_PREVIEW,  UserBitShared.WorkloadType.INTERNAL_PREVIEW,  UserBitShared.WorkloadClass.NRT},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_RUN,               UserBitShared.WorkloadType.UI_RUN,            UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_EXPORT,            UserBitShared.WorkloadType.UI_DOWNLOAD,       UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.UI_INTERNAL_RUN,      UserBitShared.WorkloadType.INTERNAL_RUN,      UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.ODBC,                 UserBitShared.WorkloadType.ODBC,              UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.JDBC,                 UserBitShared.WorkloadType.JDBC,              UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.REST,                 UserBitShared.WorkloadType.REST,              UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.ACCELERATOR_DROP,     UserBitShared.WorkloadType.DDL,               UserBitShared.WorkloadClass.BACKGROUND},
        new Object[]{com.dremio.service.job.proto.QueryType.PREPARE_INTERNAL,     UserBitShared.WorkloadType.DDL,               UserBitShared.WorkloadClass.NRT},
        new Object[]{com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE,   UserBitShared.WorkloadType.ACCELERATOR,       UserBitShared.WorkloadClass.BACKGROUND},
        new Object[]{com.dremio.service.job.proto.QueryType.ACCELERATOR_EXPLAIN,  UserBitShared.WorkloadType.ACCELERATOR,       UserBitShared.WorkloadClass.BACKGROUND},
        new Object[]{com.dremio.service.job.proto.QueryType.FLIGHT,               UserBitShared.WorkloadType.FLIGHT,            UserBitShared.WorkloadClass.GENERAL},
        new Object[]{com.dremio.service.job.proto.QueryType.D2D,                  UserBitShared.WorkloadType.D2D,               UserBitShared.WorkloadClass.GENERAL}
      );
    }

    private final com.dremio.service.job.proto.QueryType queryType;
    private final UserBitShared.WorkloadType workloadType;
    private final UserBitShared.WorkloadClass workloadClass;

    public WorkloadromQueryTypeTest(com.dremio.service.job.proto.QueryType queryType,
                                    UserBitShared.WorkloadType workloadType, UserBitShared.WorkloadClass workloadClass) {
      this.queryType = queryType;
      this.workloadType = workloadType;
      this.workloadClass = workloadClass;
    }

    @Test
    public void testGetWorkloadTypeFromQueryType() {
      assertEquals(workloadType, QueryTypeUtils.getWorkloadType(queryType));
    }

    @Test
    public void testGetWorkloadCLassFromQueryType() {
      assertEquals(workloadClass, QueryTypeUtils.getWorkloadClassFor(queryType));
    }
  }
}
