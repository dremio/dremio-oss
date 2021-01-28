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

import static com.dremio.exec.proto.UserBitShared.QueryResult.QueryState.CANCELED;
import static com.dremio.exec.proto.UserBitShared.QueryResult.QueryState.COMPLETED;
import static com.dremio.exec.proto.UserBitShared.QueryResult.QueryState.FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Unit test class for RunQueryResponseHandler.
 */
public abstract class BaseTestRunQueryResponseHandler {
  private static UserBitShared.ExternalId externalId;
  private static UserSession userSession;
  private static Provider<UserWorker> workerProvider;
  private static FlightProducer.ServerStreamListener listener;
  private static BufferAllocator allocator;

  protected abstract RunQueryResponseHandler createHandler();

  protected void testFailed(String message) throws Exception {
    throw new Exception("Test failed. " + message);
  }

  public void setUp() {
    externalId = UserBitShared.ExternalId.getDefaultInstance();
    userSession = mock(UserSession.class);
    workerProvider = mock(Provider.class);

    listener = mock(FlightProducer.ServerStreamListener.class);
    allocator = mock(BufferAllocator.class);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public static UserBitShared.ExternalId getExternalId() {
    return externalId;
  }

  public static UserSession getUserSession() {
    return userSession;
  }

  public static Provider<UserWorker> getWorkerProvider() {
    return workerProvider;
  }

  public static FlightProducer.ServerStreamListener getListener() {
    return listener;
  }

  public static BufferAllocator getAllocator() {
    return allocator;
  }

  @Test
  public void testIsCancelledTrue() throws Exception {
    // Arrange
    when(listener.isCancelled()).thenReturn(true);

    final RunQueryResponseHandler handler = createHandler();

    // Act
    assertTrue(handler.isCancelled());
  }

  @Test
  public void testIsCancelledDuringWaitTrue() throws Exception {
    // Arrange
    when(listener.isCancelled()).thenReturn(true);
    final RunQueryResponseHandler handler = createHandler();
    Thread.sleep(1000L);

    // Act
    assertTrue(handler.isCancelled());
  }

  @Test
  public void testIsCancelledFalse() {
    // Arrange
    when(listener.isCancelled()).thenReturn(false);

    // Act
    assertFalse(createHandler().isCancelled());
  }

  @Test
  public void testUserResultStateFailedWithNoException() {
    // Arrange
    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(FAILED);
    when(result.hasException()).thenReturn(false);
    when(result.getException()).thenReturn(null);
    doAnswer((InvocationOnMock invocationOnMock) -> {
      final FlightRuntimeException exception = (FlightRuntimeException) invocationOnMock.getArguments()[0];
      final CallStatus status = exception.status();
      assertEquals(FlightStatusCode.UNKNOWN, status.code());
      assertEquals("Query failed but no exception was thrown.", status.description());
      return null;
    }).when(listener).error(any(FlightRuntimeException.class));

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).error(any(FlightRuntimeException.class));
  }

  @Test
  public void testUserResultStateFailedWithException() {
    // Arrange
    final String expectedMessage = "Test UserException data read error.";
    final UserException expectedException = UserException.dataReadError().message(expectedMessage).buildSilently();

    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(FAILED);
    when(result.hasException()).thenReturn(true);
    when(result.getException()).thenReturn(expectedException);
    doAnswer((InvocationOnMock invocationOnMock) -> {
      final FlightRuntimeException exception = (FlightRuntimeException) invocationOnMock.getArguments()[0];
      final CallStatus status = exception.status();
      assertEquals(FlightStatusCode.INTERNAL, status.code());
      assertEquals(expectedMessage, status.description());
      assertEquals(expectedException, exception.getCause());
      return null;
    }).when(listener).error(any(FlightRuntimeException.class));

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).error(any(FlightRuntimeException.class));
  }

  @Test
  public void testUserResultStateCancelledWithReason() {
    // Arrange
    final String expectedMessage = "Test cancellation reason";
    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(CANCELED);
    when(result.hasException()).thenReturn(false);
    when(result.getException()).thenReturn(null);
    when(result.getCancelReason()).thenReturn(expectedMessage);
    doAnswer((InvocationOnMock invocationOnMock) -> {
      final FlightRuntimeException exception = (FlightRuntimeException) invocationOnMock.getArguments()[0];
      final CallStatus status = exception.status();
      assertEquals(FlightStatusCode.CANCELLED, status.code());
      assertEquals(expectedMessage, status.description());
      return null;
    }).when(listener).error(any(FlightRuntimeException.class));

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).error(any(FlightRuntimeException.class));
  }

  @Test
  public void testUserResultStateCancelledWithNoException() {
    // Arrange
    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(CANCELED);
    when(result.hasException()).thenReturn(false);
    when(result.getException()).thenReturn(null);
    when(result.getCancelReason()).thenReturn(null);
    doAnswer((InvocationOnMock invocationOnMock) -> {
      final FlightRuntimeException exception = (FlightRuntimeException) invocationOnMock.getArguments()[0];
      final CallStatus status = exception.status();
      assertEquals(FlightStatusCode.CANCELLED, status.code());
      assertEquals("Query is cancelled by the server.", status.description());
      return null;
    }).when(listener).error(any(FlightRuntimeException.class));

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).error(any(FlightRuntimeException.class));
  }

  @Test
  public void testUserResultStateCancelledWithException() {
    // Arrange
    final String expectedMessage = "Test UserException data read error.";
    final UserException expectedException = UserException.dataReadError().message(expectedMessage).buildSilently();

    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(CANCELED);
    when(result.hasException()).thenReturn(true);
    when(result.getException()).thenReturn(expectedException);
    doAnswer((InvocationOnMock invocationOnMock) -> {
      final FlightRuntimeException exception = (FlightRuntimeException) invocationOnMock.getArguments()[0];
      final CallStatus status = exception.status();
      assertEquals(FlightStatusCode.CANCELLED, status.code());
      assertEquals(expectedMessage, status.description());
      assertEquals(expectedException, exception.getCause());
      return null;
    }).when(listener).error(any(FlightRuntimeException.class));

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).error(any(FlightRuntimeException.class));
  }

  @Test
  public void testUserResultStateCompleted() {
    // Arrange
    final UserResult result = mock(UserResult.class);
    when(result.getState()).thenReturn(COMPLETED);

    // Act
    createHandler().handleUserResultState(result);

    // Verify
    verify(listener, times(1)).completed();
  }

  @Test
  public void testUsernameOnCancel() {
    // Arrange
    final String username = "testUser";
    final String impersonationName = "testImpersonationName";
    final UserBitShared.UserCredentials credentials =
      UserBitShared.UserCredentials.newBuilder().setUserName(username).build();
    final UserWorker worker = mock(UserWorker.class);

    when(userSession.getCredentials()).thenReturn(credentials);
    when(userSession.getTargetUserName()).thenReturn(impersonationName);
    when(workerProvider.get()).thenReturn(worker);

    // Act
    createHandler().serverStreamListenerOnCancelledCallback();

    // Verify
    verify(worker, times(1)).cancelQuery(eq(externalId), eq(username));
  }
}
