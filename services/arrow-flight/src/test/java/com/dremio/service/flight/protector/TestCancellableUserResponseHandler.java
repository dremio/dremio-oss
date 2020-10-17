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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcOutcomeListener;
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

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    when(mockedUserWorkerProvider.get()).thenReturn(userWorker);
    when(userSession.getTargetUserName()).thenReturn(DUMMY_USER);
  }

  @Test
  public void testSuccessfulResultPropagation() {
    final BigDecimal expected = new BigDecimal(1);
    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<BigDecimal>(externalId, userSession, mockedUserWorkerProvider, () -> false) {
        @Override
        public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
          fail();
        }

        @Override
        public void completed(UserResult result) {
          getCompletableFuture().complete(expected);
        }
      };

    // Act
    cancellableUserResponseHandler.completed(null);

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

    thrown.expectMessage(expected.getLocalizedMessage());
    thrown.expect(FlightRuntimeException.class);

    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<BigDecimal>(externalId, userSession, mockedUserWorkerProvider, () -> false) {
        @Override
        public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
          fail();
        }

        @Override
        public void completed(UserResult result) {
          getCompletableFuture().completeExceptionally(thrownRootException);
        }
      };
    cancellableUserResponseHandler.completed(null);

    // Act
    cancellableUserResponseHandler.get();
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
    thrown.expectMessage("Call cancelled by client application.");
    thrown.expect(FlightRuntimeException.class);

    final CancellableUserResponseHandler<BigDecimal> cancellableUserResponseHandler =
      new CancellableUserResponseHandler<BigDecimal>(externalId, userSession, mockedUserWorkerProvider, isCancelled) {
        @Override
        public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
          fail();
        }

        @Override
        public void completed(UserResult result) {
          fail();
        }
      };

    try {
      // Act
      cancellableUserResponseHandler.get();
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
