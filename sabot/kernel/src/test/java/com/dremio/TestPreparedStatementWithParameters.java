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
package com.dremio;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.PreparedStatementParameterValue;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestPreparedStatementWithParameters extends BaseTestQuery {

  @Test
  public void testBooleanParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      String query = "select * from (values (true), (false), (null)) as a(id) where id=?";

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setStringValue("10").build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(e.getMessage().contains("Invalid value for boolean: '10'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setDateValue(1234L).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage().contains("Cannot apply '=' to arguments of type '<BOOLEAN> = <DATE>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void testIntParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      String query = "select * from (values (1), (2), (null)) as a(id) where id=?";
      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setStringValue("a").build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(e.getMessage().contains("Failed to cast the string a to int32_t"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setBoolValue(true).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("Dremio does not support casting or coercing boolean to bigint"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void testDateParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      String query = "select * from (values (DATE '2024-02-20'), (null)) as a(id) where id=?";
      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setIntValue(123456).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage().contains("Cannot apply '=' to arguments of type '<DATE> = <INTEGER>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setBoolValue(true).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage().contains("Cannot apply '=' to arguments of type '<DATE> = <BOOLEAN>'."));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void testTimeParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      String query = "select * from (values (TIME '12:34:56'), (null)) as a(id) where id=?";
      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setLongValue(1234567891L).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("Cannot apply '=' to arguments of type '<TIME(3)> = <INTEGER>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setBoolValue(false).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("Cannot apply '=' to arguments of type '<TIME(3)> = <BOOLEAN>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void testVarBinaryParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      String query =
          "select * from (values (CAST('Hello, Dremio!' AS VARBINARY)), (null)) as a(id) where id=?";
      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setLongValue(1234567891L).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Cannot apply '=' to arguments of type '<VARBINARY(65536)> = <INTEGER>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }

      try {
        ImmutableList<PreparedStatementParameterValue> preparedStatementParameterValues =
            ImmutableList.of(
                PreparedStatementParameterValue.newBuilder().setBoolValue(false).build());
        runPreparedStatement(query, preparedStatementParameterValues);
        Assert.fail();
      } catch (RpcException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Cannot apply '=' to arguments of type '<VARBINARY(65536)> = <BOOLEAN>'"));
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  private List<QueryDataBatch> runPreparedStatement(
      String query, ImmutableList<PreparedStatementParameterValue> parameterValues)
      throws RpcException {

    final ServerPreparedStatementState serverPreparedStatementState =
        ServerPreparedStatementState.newBuilder().setHandle(1).setSqlQuery(query).build();

    final PreparedStatementHandle preparedStatementHandle =
        PreparedStatementHandle.newBuilder()
            .setServerInfo(serverPreparedStatementState.toByteString())
            .build();

    return client.executePreparedStatement(preparedStatementHandle, parameterValues);
  }
}
