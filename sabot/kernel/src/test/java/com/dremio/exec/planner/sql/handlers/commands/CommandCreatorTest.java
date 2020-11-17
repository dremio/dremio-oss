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
package com.dremio.exec.planner.sql.handlers.commands;

import static com.dremio.exec.planner.physical.PlannerSettings.REUSE_PREPARE_HANDLES;
import static com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import static com.dremio.exec.proto.UserProtos.RpcType;
import static com.dremio.exec.proto.UserProtos.RunQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * CommandCreator tests.
 */
public class CommandCreatorTest {

  private static final String USERNAME1 = "testuser1";
  private static final String USERNAME2 = "testuser2";
  private static final String QUERY = "q";
  private final CommandRunner expectedCommand = mock(CommandRunner.class);

  private final ServerPreparedStatementState serverPreparedStatementState = ServerPreparedStatementState.newBuilder()
    .setHandle(1)
    .setSqlQuery(QUERY)
    .build();

  private final PreparedStatementHandle preparedStatementHandle = PreparedStatementHandle.newBuilder()
    .setServerInfo(serverPreparedStatementState.toByteString())
    .build();

  private final RunQuery runQueryRequest = RunQuery.newBuilder()
    .setType(UserBitShared.QueryType.PREPARED_STATEMENT)
    .setPreparedStatementHandle(preparedStatementHandle)
    .build();

  private final UserRequest request = new UserRequest(RpcType.RUN_QUERY, runQueryRequest);

  private final UserBitShared.QueryId prepareId = UserBitShared.QueryId.newBuilder().setPart1(1).setPart2(1).build();
  private final UserSession userSession = mock(UserSession.class);
  private final QueryContext queryContext = mock(QueryContext.class);
  private final OptionManager optionManager = mock(OptionManager.class);
  private final AttemptObserver attemptObserver = mock(AttemptObserver.class);
  private final Cache<Long, PreparedPlan> plans = CacheBuilder.newBuilder().build();

  @Before
  public void setup() {
    when(queryContext.getExecutionControls()).thenReturn(mock(ExecutionControls.class));
    when(queryContext.getSession()).thenReturn(userSession);
    when(queryContext.getOptions()).thenReturn(optionManager);
  }

  @After
  public void cleanUp() {
    plans.invalidateAll();
  }

  @Test
  public void testNoCachedPlanExecutesSqlCommand() throws ForemanException {
    // Arrange
    setSessionUser1();
    final CommandCreator commandCreator = spy(buildCommandCreator(0));
    // Override getSqlCommand, expecting that permissions checking of the underlying source would happen as normal
    doReturn(expectedCommand).when(commandCreator).getSqlCommand(any(), any());

    // Act
    CommandRunner<?> actualCommand = commandCreator.toCommand();

    // Assert
    assertEquals(expectedCommand, actualCommand);
  }

  @Test
  public void testFoundCachedPlanExecutesPreparedPlan() throws ForemanException {
    // Arrange
    setSessionUser1();
    setReusePreparedHandles();

    final PreparedPlan preparedPlan = new PreparedPlan(prepareId, USERNAME1, QUERY, null, null);
    plans.put(serverPreparedStatementState.getHandle(), preparedPlan);

    final PrepareToExecution expectedCommand = new PrepareToExecution(preparedPlan, attemptObserver);

    final CommandCreator commandCreator = buildCommandCreator(0);

    // Act
    PrepareToExecution actualCommand = (PrepareToExecution) commandCreator.toCommand();

    // Assert
    assertEquals(expectedCommand.getCommandType(), actualCommand.getCommandType());
    assertEquals(expectedCommand.getPhysicalPlan(), actualCommand.getPhysicalPlan());
    assertEquals(expectedCommand.getDescription(), actualCommand.getDescription());
    assertNotNull(plans.getIfPresent(serverPreparedStatementState.getHandle()));
  }

  @Test
  public void testFoundCachedPlanInvalidatedAfterOneUse() throws ForemanException {
    // Arrange
    setSessionUser1();
    setNotReusePreparedHandles();

    final PreparedPlan preparedPlan = new PreparedPlan(prepareId, USERNAME1, QUERY, null, null);
    plans.put(serverPreparedStatementState.getHandle(), preparedPlan);

    final PrepareToExecution expectedCommand = new PrepareToExecution(preparedPlan, attemptObserver);

    final CommandCreator commandCreator = buildCommandCreator(0);

    // Act
    PrepareToExecution actualCommand = (PrepareToExecution) commandCreator.toCommand();

    // Assert
    assertEquals(expectedCommand.getCommandType(), actualCommand.getCommandType());
    assertEquals(expectedCommand.getPhysicalPlan(), actualCommand.getPhysicalPlan());
    assertEquals(expectedCommand.getDescription(), actualCommand.getDescription());
    assertNull(plans.getIfPresent(serverPreparedStatementState.getHandle()));
  }

  @Test
  public void testCachedPlanExistsButNotUsedOnAttempt2() throws ForemanException {
    // Arrange
    setSessionUser1();
    setReusePreparedHandles();

    final PreparedPlan preparedPlan = new PreparedPlan(prepareId, USERNAME1, QUERY, null, null);
    plans.put(serverPreparedStatementState.getHandle(), preparedPlan);

    final PrepareToExecution expectedCommand = new PrepareToExecution(preparedPlan, attemptObserver);

    final CommandCreator commandCreator = spy(buildCommandCreator(1));
    // Override getSqlCommand, expecting that permissions checking of the underlying source would happen as normal
    doReturn(expectedCommand).when(commandCreator).getSqlCommand(any(), any());

    // Act
    CommandRunner<?> actualCommand = commandCreator.toCommand();

    // Assert
    assertEquals(expectedCommand, actualCommand);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFoundCachedPlanWrongUser() throws ForemanException {
    // Arrange
    setSessionUser2();
    setReusePreparedHandles();

    final PreparedPlan preparedPlan = new PreparedPlan(prepareId, USERNAME1, QUERY, null, null);
    plans.put(serverPreparedStatementState.getHandle(), preparedPlan);

    final CommandCreator commandCreator = buildCommandCreator(0);

    // Act
    commandCreator.toCommand();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFoundCachedPlanWrongUserNotReuseStatements() throws ForemanException {
    // Arrange
    setSessionUser2();
    setNotReusePreparedHandles();

    final PreparedPlan preparedPlan = new PreparedPlan(prepareId, USERNAME1, QUERY, null, null);
    plans.put(serverPreparedStatementState.getHandle(), preparedPlan);

    final CommandCreator commandCreator = buildCommandCreator(0);

    // Act
    commandCreator.toCommand();
  }

  private void setSessionUser1() {
    when(userSession.getCredentials()).thenReturn(
      UserBitShared.UserCredentials.newBuilder().setUserName(USERNAME1).build());
  }

  private void setSessionUser2() {
    when(userSession.getCredentials()).thenReturn(
      UserBitShared.UserCredentials.newBuilder().setUserName(USERNAME2).build());
  }

  private void setReusePreparedHandles() {
    when(optionManager.getOption(eq(REUSE_PREPARE_HANDLES))).thenReturn(true);
  }

  private void setNotReusePreparedHandles() {
    when(optionManager.getOption(eq(REUSE_PREPARE_HANDLES))).thenReturn(false);
  }

  private CommandCreator buildCommandCreator(int attemptNumber) {
    return new CommandCreator(
      null,
      queryContext,
      request,
      attemptObserver,
      plans,
      new Pointer<>(prepareId),
      attemptNumber
    );
  }
}
