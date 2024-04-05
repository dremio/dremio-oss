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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class TestQueryExceptionHandling extends PlanTestBase {

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
        .withSessionOptionManager(
            new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
            getSabotContext().getOptionManager())
        .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
        .withCredentials(
            UserBitShared.UserCredentials.newBuilder()
                .setUserName(UserServiceTestImpl.ANONYMOUS)
                .build())
        .setSupportComplexTypes(true)
        .build();
  }

  /**
   * Verifies StackOverflowError thrown during planning is caught and re-thrown as PLAN ERROR
   * UserException.
   */
  @Test
  public void testStackOverflowErrorDuringPlanning() {
    final String sql = "select 1";

    final SabotContext context = getSabotContext();
    final QueryContext queryContext =
        new QueryContext(session(), context, UserBitShared.QueryId.getDefaultInstance());
    final AttemptObserver observer =
        new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter =
        spy(
            new SqlConverter(
                queryContext.getPlannerSettings(),
                queryContext.getOperatorTable(),
                queryContext,
                queryContext.getMaterializationProvider(),
                queryContext.getFunctionRegistry(),
                queryContext.getSession(),
                observer,
                queryContext.getSubstitutionProviderFactory(),
                queryContext.getConfig(),
                queryContext.getScanResult(),
                queryContext.getRelMetadataQuerySupplier()));
    final SqlNode node = converter.parse(sql);
    final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);
    final SqlToPlanHandler handler = new NormalHandler();

    // Simulates unexpected StackOverflowError is thrown during planning.
    when(converter.getSettings()).thenThrow(new StackOverflowError());

    // Verifies when unexpected StackOverflowError is thrown during planning, it is caught and
    // re-thrown as PLAN ERROR
    // UserException with the error message of SqlExceptionHelper.PLANNING_STACK_OVERFLOW_ERROR.
    UserException exception =
        assertThrows(UserException.class, () -> handler.getPlan(config, sql, node));
    assertThat(exception.getErrorType().name()).isEqualTo("PLAN");
    assertThat(exception.getMessage()).isEqualTo(SqlExceptionHelper.PLANNING_STACK_OVERFLOW_ERROR);
  }
}
