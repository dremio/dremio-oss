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

package com.dremio.service.flight.protector;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Tests for CancellableUserResponseHandler
 */
public class TestCancellableUserResponseHandler {

  private static final String DUMMY_USER = "dummy_user";

  private final UserBitShared.ExternalId externalId = ExternalIdHelper.generateExternalId();
  private final UserSession userSession = mock(UserSession.class);
  private final UserWorker userWorker = mock(UserWorker.class);
  private final Provider<UserWorker> mockedUserWorkerProvider = mock(Provider.class);
  private final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();
  private final UserBitShared.QueryProfile resultProfile = UserBitShared.QueryProfile.getDefaultInstance();

  @Before
  public void setup() {
    when(mockedUserWorkerProvider.get()).thenReturn(userWorker);
    when(userSession.getTargetUserName()).thenReturn(DUMMY_USER);
  }

  @Test
  public void testSuccessfulResultPropagation() {
    final BigDecimal expected = new BigDecimal(1);
    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<>(externalId, userSession, mockedUserWorkerProvider, () -> false,
        BigDecimal.class);

    // Act
    UserResult userResult =
      new UserResult(expected, queryId, UserBitShared.QueryResult.QueryState.COMPLETED, resultProfile, null, null,
        false);
    cancellableUserResponseHandler.completed(userResult);

    // Assert
    final BigDecimal actual = cancellableUserResponseHandler.get();
    assertEquals(expected, actual);
  }

  @Test
  public void testExceptionalPropagation() {

    // Arrange
    final Throwable thrownRootException = new TestException("Dummy Exception");

    final Throwable expected = CallStatus.INTERNAL
      .withCause(thrownRootException)
      .withDescription(thrownRootException.getLocalizedMessage())
      .toRuntimeException();

    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<>(externalId, userSession, mockedUserWorkerProvider, () -> false,
        BigDecimal.class);

    UserException userException = UserException.parseError(expected).buildSilently();
    UserResult userResult =
      new UserResult(null, queryId, UserBitShared.QueryResult.QueryState.FAILED, resultProfile, userException, null,
        false);
    cancellableUserResponseHandler.completed(
      userResult);

    // Act
    assertThatThrownBy(cancellableUserResponseHandler::get)
      .isInstanceOf(FlightRuntimeException.class)
      .hasMessageContaining(expected.getLocalizedMessage());
  }

  @Test
  public void testClientCancelCaughtAndPropagatedToServer() {
    testClientCancelCaughtAndPropagatedToServer(() -> true);
  }

  @Test
  public void testClientCancelCaughtAfter3ChecksAndPropagatedToServer() {
    final CountingSupplier countingSupplier = new CountingSupplier();

    testClientCancelCaughtAndPropagatedToServer(countingSupplier);
    assertTrue(countingSupplier.isReachedMaxCount());
  }

  private void testClientCancelCaughtAndPropagatedToServer(Supplier<Boolean> isCancelled) {
    // Arrange
    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<BigDecimal>(externalId, userSession, mockedUserWorkerProvider, isCancelled,
        BigDecimal.class) {
        @Override
        public void completed(UserResult result) {
          fail();
        }
      };

    try {
      // Act
      assertThatThrownBy(cancellableUserResponseHandler::get)
        .isInstanceOf(FlightRuntimeException.class)
        .hasMessageContaining("Call cancelled by client application.");
    } finally {
      // Assert
      verify(userWorker).cancelQuery(eq(externalId), eq(DUMMY_USER));
    }
  }

  private static final class CountingSupplier implements Supplier<Boolean> {
    private static final int MAX_COUNT = 3;
    private int counter = 0;

    @Override
    public Boolean get() {
      if (counter < MAX_COUNT) {
        counter++;
        return false;
      } else {
        return true;
      }
    }

    public boolean isReachedMaxCount() {
      return MAX_COUNT == counter;
    }
  }

  private static class TestException extends Exception {
    public TestException(String message) {
      super(message);
    }
  }
}
