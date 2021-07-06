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

package com.dremio.exec.planner.logical;

import static com.dremio.exec.record.BatchSchema.MIXED_TYPES_ERROR;

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.test.UserExceptionMatcher;

public class TestComplexToJsonMixedTypes extends BaseTestQuery {

  @Test
  public void testMixedTypes() throws Exception {
    final String sql = "SELECT * from cp.\"jsoninput/union/d.json\"";
    testQuery(sql);
  }

  private void testQuery(String sql) throws Exception {
    final SabotContext context = getSabotContext();

    final QueryContext queryContext = new QueryContext(session(), context, UserBitShared.QueryId.getDefaultInstance());
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter = new SqlConverter(
      queryContext.getPlannerSettings(),
      queryContext.getOperatorTable(),
      queryContext,
      queryContext.getMaterializationProvider(),
      queryContext.getFunctionRegistry(),
      queryContext.getSession(),
      observer,
      queryContext.getCatalog(),
      queryContext.getSubstitutionProviderFactory(),
      queryContext.getConfig(),
      queryContext.getScanResult(),
      queryContext.getRelMetadataQuerySupplier());
    final SqlNode node = converter.parse(sql);
    SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);
    NormalHandler handler = new NormalHandler();
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION,
      MIXED_TYPES_ERROR));
    handler.getPlan(config, sql, node);
  }

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .setSupportComplexTypes(false)
      .build();
  }
}
