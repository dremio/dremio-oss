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
package com.dremio.service.flight;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;

import org.apache.arrow.flight.FlightProducer;
import org.junit.Before;
import org.junit.Test;

import com.dremio.context.RequestContext;
import com.google.inject.util.Providers;

/**
 * Test that Flight RPC handlers are utilizing the request context.
 */
public class TestFlightWithRequestContext {

  private static final class DummyFlightRequestContextDecorator implements FlightRequestContextDecorator {

    private int callCount = 0;

    @Override
    public RequestContext apply(RequestContext requestContext, FlightProducer.CallContext flightContext) {
      ++callCount;
      return requestContext;
    }
  }

  private DummyFlightRequestContextDecorator decorator;

  // Note: FlightProducer interface is used to intentional limit testing to Flight (not FlightSql) RPC calls.
  private FlightProducer producer;

  @Before
  public void setup() {
    decorator = new DummyFlightRequestContextDecorator();
    producer = new DremioFlightProducer(
      null, null, null, null, null,
      Providers.of(decorator), null);
  }

  @Test
  public void testGetStream() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.getStream(null, null, null);
      return null;
    });
  }

  @Test
  public void testListFlights() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.listFlights(null, null, null);
      return null;
    });
  }

  @Test
  public void testGetFlightInfo() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.getFlightInfo(null, null);
      return null;
    });
  }

  @Test
  public void testGetSchema() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.getSchema(null, null);
      return null;
    });
  }

  @Test
  public void testAcceptPut() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.acceptPut(null, null, null);
      return null;
    });
  }

  @Test
  public void testDoExchange() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.doExchange(null, null, null);
      return null;
    });
  }

  @Test
  public void testDoAction() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.doAction(null, null, null);
      return null;
    });
  }

  @Test
  public void testListActions() {
    ignoreExceptionsAndValidateCallCount(() -> {
      producer.listActions(null, null);
      return null;
    });
  }

  private <V> void ignoreExceptionsAndValidateCallCount(Callable<V> rpcHandlerBody) {
    try {
      rpcHandlerBody.call();
    } catch (Exception ex) {
      // Suppress exceptions thrown from the RPC handler, since the point of this test
      // is to just verify the RequestContext was invoked correctly.
    }
    assertEquals(decorator.callCount, 1);
  }
}
