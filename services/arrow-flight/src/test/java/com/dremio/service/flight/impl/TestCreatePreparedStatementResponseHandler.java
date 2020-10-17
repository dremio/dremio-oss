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
package com.dremio.service.flight.impl;

import static com.dremio.exec.proto.UserBitShared.ExternalId;
import static com.dremio.exec.proto.UserBitShared.QueryId;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementArrowResp;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Tests for CreatePreparedStatementResponseHandler.
 */
public class TestCreatePreparedStatementResponseHandler {

  private final QueryId queryId = QueryId.getDefaultInstance();
  private final QueryProfile resultProfile = QueryProfile.getDefaultInstance();
  private final ExternalId externalId = ExternalIdHelper.generateExternalId();
  private final UserSession userSession = mock(UserSession.class);
  private final Provider<UserWorker> mockedUserWorkerProvider = mock(Provider.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSuccessfulResultPropagation() {
    // Arrange
    final CreatePreparedStatementArrowResp expected = CreatePreparedStatementArrowResp.newBuilder().build();

    final CreatePreparedStatementResponseHandler cancellableUserResponseHandler =
      new CreatePreparedStatementResponseHandler(externalId, userSession, mockedUserWorkerProvider, () -> false);

    final UserResult result = new UserResult(expected, queryId,
      QueryState.COMPLETED,
      resultProfile,
      null, // UserException
      null, // cancelReason
      false // clientCancelled
    );

    // Act
    cancellableUserResponseHandler.completed(result);

    // Assert
    CreatePreparedStatementArrowResp actual = cancellableUserResponseHandler.get();
    assertEquals(expected, actual);
  }

  @Test
  public void testCancelledExceptionResultPropagation() {
    testExceptionResultPropagation(CallStatus.CANCELLED, QueryState.CANCELED);
  }

  @Test
  public void testFailedExceptionResultPropagation() {
    testExceptionResultPropagation(CallStatus.INTERNAL, QueryState.FAILED);
  }

  public void testExceptionResultPropagation(CallStatus callStatus, QueryState queryState) {
    // Arrange
    final CreatePreparedStatementResponseHandler cancellableUserResponseHandler =
      new CreatePreparedStatementResponseHandler(externalId, userSession, mockedUserWorkerProvider, () -> false);

    final Exception original = new TestException("Dummy Exception");
    final UserException cause = UserException.parseError(original).buildSilently();

    final Throwable expected = callStatus
      .withCause(original)
      .withDescription(original.getLocalizedMessage())
      .toRuntimeException();

    thrown.expectMessage(expected.getLocalizedMessage());
    thrown.expect(FlightRuntimeException.class);

    final UserResult result = new UserResult(
      expected,
      queryId,
      queryState,
      resultProfile,
      cause, // UserException
      null, // cancelReason
      false // clientCancelled
    );
    cancellableUserResponseHandler.completed(result);

    // Act
    cancellableUserResponseHandler.get();
  }

  private static class TestException extends Exception {
    public TestException(String message) {
      super(message);
    }
  }
}
