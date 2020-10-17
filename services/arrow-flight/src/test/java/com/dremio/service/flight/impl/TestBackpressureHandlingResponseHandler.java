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

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BackpressureHandlingResponseHandler;

/**
 * Unit test class for BackpressureHandlingResponseHandler.
 */
public class TestBackpressureHandlingResponseHandler extends BaseTestRunQueryResponseHandler {
  private static final class TestException extends Exception {
    TestException(String message) {
      super(message);
    }
  }

  protected BackpressureHandlingResponseHandler createHandler() {
    return new BackpressureHandlingResponseHandler(getExternalId(), getUserSession(), getWorkerProvider(),
      getListener(), getAllocator());
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testClientIsReady() throws Exception {
    // Arrange
    when(getListener().isCancelled()).thenReturn(false);
    when(getListener().isReady()).thenReturn(true);

    // Act
    try {
      assertEquals(createHandler().clientIsReadyForData(),
        RunQueryResponseHandler.FlightClientDataRetrievalStatus.READY);
    } catch (RpcException ex) {
      testFailed("Unexpected RpcException thrown.");
    }
  }

  @Test
  public void testCancelWhenPollingClientReadiness() throws Exception {
    // Arrange
    when(getListener().isCancelled()).thenReturn(true);
    when(getListener().isReady()).thenReturn(false);

    // Act
    try {
      assertEquals(createHandler().clientIsReadyForData(),
        RunQueryResponseHandler.FlightClientDataRetrievalStatus.CANCELLED);
    } catch (RpcException ex) {
      testFailed("Unexpected RpcException thrown.");
    }
  }

  @Test
  public void testTimeoutExceptionHandling() throws RpcException {
    // Arrange
    final ExecutorService executor =
      Executors.newSingleThreadExecutor(new NamedThreadFactory(Thread.currentThread().getName()
        + ":test-flight-client-exception"));
    final CountDownLatch latch = new CountDownLatch(1);

    final Future<RunQueryResponseHandler.FlightClientDataRetrievalStatus> future = executor.submit(() ->
    {
      latch.await();
      return null;
    });

    thrown.expect(RpcException.class);
    thrown.expectCause(isA(TimeoutException.class));
    thrown.expectMessage("Timeout while polling for readiness of the Flight client.");

    // Act
    try {
      createHandler().handleFuture(future, executor, 500);
    } finally {
      latch.countDown();
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  public void testExecutionExceptionHandling() throws RpcException {
    // Arrange
    final ExecutorService executor =
      Executors.newSingleThreadExecutor(new NamedThreadFactory(Thread.currentThread().getName()
        + ":test-flight-client-exception"));

    final Future<RunQueryResponseHandler.FlightClientDataRetrievalStatus> future =
      executor.submit(() -> {
        throw new TestException("Testing ExecutionException wrapping.");
      });

    thrown.expect(RpcException.class);
    thrown.expectCause(isA(TestException.class));
    thrown.expectMessage("Encountered error while polling for readiness of the Flight client.");

    // Act
    try {
      createHandler().handleFuture(future, executor, 500);
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test
  public void testIsCancelledTrue() throws Exception {
    super.testIsCancelledTrue();
  }

  @Test
  public void testIsCancelledFalse() {
    super.testIsCancelledFalse();
  }

  @Test
  public void testUserResultStateFailedWithNoException() {
    super.testUserResultStateFailedWithNoException();
  }

  @Test
  public void testUserResultStateFailedWithException() {
    super.testUserResultStateFailedWithException();
  }

  @Test
  public void testUserResultStateCancelledWithReason() {
    super.testUserResultStateCancelledWithReason();
  }

  @Test
  public void testUserResultStateCancelledWithNoException() {
    super.testUserResultStateCancelledWithNoException();
  }

  @Test
  public void testUserResultStateCancelledWithException() {
    super.testUserResultStateCancelledWithException();
  }

  @Test
  public void testUserResultStateCompleted() {
    super.testUserResultStateCompleted();
  }
}
