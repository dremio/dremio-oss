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
package com.dremio.dac.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.dac.service.errors.ConflictException;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.telemetry.api.metrics.SimpleCounter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.UriInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGenericExceptionMapper {
  private static GenericExceptionMapper gem;

  private static SimpleMeterRegistry simpleRegistry;

  @BeforeClass
  public static void setUp() throws URISyntaxException {
    final UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getRequestUri()).thenReturn(new URI("http://dremio.foo"));
    final Request request = mock(Request.class);
    when(request.getMethod()).thenReturn("PUT");
    gem = new GenericExceptionMapper(uriInfo, request);

    simpleRegistry = new SimpleMeterRegistry();
    Metrics.addRegistry(simpleRegistry);
  }

  @AfterClass
  public static void tearDown() {
    Metrics.removeRegistry(simpleRegistry);
  }

  @After
  public void reset() {
    Metrics.globalRegistry.forEachMeter(
        meter -> {
          if (meter.getId().getName().equals("resetapi.generic_errors")) {
            Metrics.globalRegistry.remove(meter);
          }
        });
  }

  @Test
  public void testUnHandledException() {
    // Exception we do not do special handling for in GenericExceptionMapper
    assertEquals(
        "foo",
        ((GenericErrorMessage) gem.toResponse(new RuntimeException("foo")).getEntity())
            .getErrorMessage());
    assertEquals(
        GenericErrorMessage.GENERIC_ERROR_MSG,
        ((GenericErrorMessage) gem.toResponse(new RuntimeException("")).getEntity())
            .getErrorMessage());

    Assert.assertEquals(2, SimpleCounter.of("resetapi.generic_errors").count());
  }

  @Test
  public void testHandledException() {
    // Exception we do special handling for in GenericExceptionMapper
    assertEquals(
        "foo",
        ((GenericErrorMessage) gem.toResponse(new WebApplicationException("foo")).getEntity())
            .getErrorMessage());
    assertEquals(
        "foo",
        ((GenericErrorMessage) gem.toResponse(new ConflictException("foo")).getEntity())
            .getErrorMessage());
    assertEquals(
        "User 'foo' not found",
        ((GenericErrorMessage) gem.toResponse(new UserNotFoundException("foo")).getEntity())
            .getErrorMessage());
    assertEquals(
        "foo",
        ((GenericErrorMessage) gem.toResponse(new AccessControlException("foo")).getEntity())
            .getErrorMessage());
    assertEquals(
        "PERMISSION_DENIED: foo",
        ((GenericErrorMessage)
                gem.toResponse(
                        new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("foo")))
                    .getEntity())
            .getErrorMessage());
    assertEquals(
        "NOT_FOUND: foo",
        ((GenericErrorMessage)
                gem.toResponse(new StatusRuntimeException(Status.NOT_FOUND.withDescription("foo")))
                    .getEntity())
            .getErrorMessage());

    Assert.assertEquals(4, SimpleCounter.of("resetapi.generic_errors").count());
  }
}
