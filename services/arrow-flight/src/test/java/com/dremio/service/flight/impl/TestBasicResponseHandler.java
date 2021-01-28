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

import static org.apache.arrow.flight.BackpressureStrategy.WaitResult.READY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.exec.rpc.RpcException;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BasicResponseHandler;

/**
 * Unit test class for BasicResponseHandler.
 */
public class TestBasicResponseHandler extends BaseTestRunQueryResponseHandler {

  protected RunQueryResponseHandler createHandler() {
    return new BasicResponseHandler(getExternalId(), getUserSession(), getWorkerProvider(),
      getListener(), getAllocator());
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testClientIsReadyForDataWhenListenerIsNotReady() throws Exception {
    // Arrange
    when(getListener().isReady()).thenReturn(false);

    // Act
    try {
      assertEquals(createHandler().clientIsReadyForData(), READY);
    } catch (RpcException ex) {
      testFailed("Unexpected RpcException thrown.");
    }
  }

  @Test
  public void testNoCancelSensitivityClientNotReady() throws Exception {
    // Arrange
    when(getListener().isCancelled()).thenReturn(true);
    when(getListener().isReady()).thenReturn(false);

    // Act
    try {
      assertEquals(createHandler().clientIsReadyForData(), READY);
    } catch (RpcException ex) {
      testFailed("Unexpected RpcException thrown.");
    }
  }
}
