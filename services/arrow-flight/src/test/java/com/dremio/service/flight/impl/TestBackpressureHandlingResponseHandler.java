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

import static com.dremio.service.flight.DremioFlightServiceOptions.CLIENT_READINESS_TIMEOUT_MILLIS;
import static org.apache.arrow.flight.BackpressureStrategy.WaitResult.CANCELLED;
import static org.apache.arrow.flight.BackpressureStrategy.WaitResult.READY;
import static org.apache.arrow.flight.BackpressureStrategy.WaitResult.TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.service.flight.DremioFlightServiceOptions;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BackpressureHandlingResponseHandler;

/**
 * Unit test class for BackpressureHandlingResponseHandler.
 */
public class TestBackpressureHandlingResponseHandler extends BaseTestRunQueryResponseHandler {

  protected BackpressureHandlingResponseHandler createHandler() {
    return new BackpressureHandlingResponseHandler(
      getExternalId(), getUserSession(), getWorkerProvider(),
      getListener(), getAllocator(), () -> {});
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testClientIsReady() {
    // Arrange
    when(getListener().isCancelled()).thenReturn(false);
    when(getListener().isReady()).thenReturn(true);

    // Act
    assertEquals(READY, createHandler().clientIsReadyForData());
  }

  @Test
  public void testClientIsReadyTimedOut() {
    // Arrange
    when(getListener().isCancelled()).thenReturn(false);
    when(getListener().isReady()).thenReturn(false);

    // Act
    assertEquals(TIMEOUT, createHandler().clientIsReadyForData());
  }

  @Test
  public void testCancelWhenWaitingForClientReadiness() {
    // Arrange
    when(getListener().isCancelled()).thenReturn(true);
    when(getListener().isReady()).thenReturn(false);

    // Act
    assertEquals(CANCELLED, createHandler().clientIsReadyForData());
  }

  @Test
  public void testTimeoutExceptionHandling() {
    // Arrange
    when(getListener().isReady()).thenReturn(false);
    when(getListener().isCancelled()).thenReturn(false);
    RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener = mock(RpcOutcomeListener.class);

    // Act
    createHandler().putNextWhenClientReady(outcomeListener);

    // Assert
    ArgumentCaptor<RpcException> argument = ArgumentCaptor.forClass(RpcException.class);
    verify(outcomeListener, times(1)).failed(argument.capture());
    assertEquals("Timeout while waiting for client to be in ready state.", argument.getValue().getMessage());
    verifyNoMoreInteractions(outcomeListener);
  }

  @Test
  public void testOverridingClientReadinessTimeout() throws Exception {
    // Arrange
    when(getOptionManager().getOption(DremioFlightServiceOptions.CLIENT_READINESS_TIMEOUT_MILLIS)).thenReturn(100L);
    when(getListener().isCancelled()).thenReturn(false);
    when(getListener().isReady()).thenReturn(false);
    final long startTime = System.currentTimeMillis();

    // Act
    assertEquals(TIMEOUT, createHandler().clientIsReadyForData());
    assertTrue(System.currentTimeMillis() < startTime + CLIENT_READINESS_TIMEOUT_MILLIS.getDefault().getNumVal());
  }
}
