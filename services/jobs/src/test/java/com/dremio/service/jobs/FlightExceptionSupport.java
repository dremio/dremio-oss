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
package com.dremio.service.jobs;

import static com.dremio.common.exceptions.GrpcExceptionUtil.toStatusRuntimeException;

import java.io.IOException;
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;

/**
 * test to ensure grpc trailers are being passed through FlightRuntimeException and we are able to rehydrate UserException
 */
public class FlightExceptionSupport {

  private static FlightServer server;
  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private static final Location location = Location.forGrpcInsecure("localhost", 12543);
  private static String expectedMessage = "Mixed types decimal(10,10), decmial(10,0) for field dcol are not supported.";

  private FlightClient client;

  @BeforeClass
  public static void setup() throws IOException {
    server = FlightServer.builder()
      .allocator(allocator)
      .producer(new SimpleErrorProducer())
      .location(location)
      .build();
    server.start();
  }

  @Before
  public void setupClient() {
    client = FlightClient.builder().allocator(allocator)
      .location(location)
      .build();
  }

  @Test
  public void testException() {
    try {
      FlightStream results = client.getStream(new Ticket("test".getBytes()));
      results.getDescriptor();
      Assert.fail();
    } catch (FlightRuntimeException e) {
      Optional<UserException> ue = JobsRpcUtils.fromFlightRuntimeException(e);
      Assert.assertTrue(ue.isPresent());
      Assert.assertEquals(expectedMessage, ue.get().getOriginalMessage());
    }
  }

  @After
  public void closeClient() throws InterruptedException {
    client.close();
  }

  @AfterClass
  public static void close() throws InterruptedException {
    server.close();
    allocator.close();
  }

  private static class SimpleErrorProducer extends NoOpFlightProducer {
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      UserException e = UserException.unsupportedError().message(expectedMessage).buildSilently();
      listener.error(toStatusRuntimeException(e));
    }
  }
}
