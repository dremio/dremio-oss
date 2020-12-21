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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.BaseFlightQueryTest;
import com.dremio.service.flight.FlightClientUtils;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.dremio.service.flight.impl.RunQueryResponseHandler.BasicResponseHandler;

/**
 * Integration test that 2 streams can be active, and the second stream can be completely consumed
 * while the first stream has stopped returning results.
 */
public class ITIndependentStreams extends BaseFlightQueryTest {

  private static final byte[] QUERY = "select * from cp.\"/10k_rows.parquet\"".getBytes(StandardCharsets.UTF_8);
  private static final int TOTAL_ROWS = 10001;

  private static final DelegatingFirstRunQueryResponseHandlerFactory runQueryResponseHandlerFactory
    = new DelegatingFirstRunQueryResponseHandlerFactory();

  @AfterClass
  public static void ensureLatchWasReleased() {
    runQueryResponseHandlerFactory.releaseLatch();
  }

  @BeforeClass
  public static void setup() throws Exception {
    setupBaseFlightQueryTest(
      false,
      true,
      "independent.streams.flight.endpoint.port",
      runQueryResponseHandlerFactory);
  }

  @Test
  public void testIndependentStreams() throws Exception {
    final FlightClientUtils.FlightClientWrapper wrapper = this.getFlightClientWrapper();
    final FlightClient client = wrapper.getClient();
    final CredentialCallOption callOption = wrapper.getTokenCallOption();
    final FlightInfo flightInfo = client.getInfo(FlightDescriptor.command(QUERY), callOption);

    int stream1rowcount = 0;
    int stream2rowcount = 0;

    // Assumption: flightInfo only has one endpoint and the location in the
    // flightInfo is the same as the original endpoint.
    try (FlightStream flightStream1 =
           client.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOption)) {

      stream1rowcount += consumeStream(flightStream1);

      /**
       * The current query and data set is designed to cause the worker to call
       * {@link UserResponseHandler#sendData(RpcOutcomeListener, QueryWritableBatch)}
       * 3 times.
       */
      assertNotEquals(TOTAL_ROWS, stream1rowcount);

      try (FlightStream flightStream2 = client.getStream(client.getInfo(
        FlightDescriptor.command(QUERY), callOption).getEndpoints().get(0).getTicket(), callOption)) {
        stream2rowcount += consumeStream(flightStream2);
        assertNotEquals(TOTAL_ROWS, stream2rowcount);
        stream2rowcount += consumeStream(flightStream2);
        assertNotEquals(TOTAL_ROWS, stream2rowcount);
        stream2rowcount += consumeStream(flightStream2);
      }
      runQueryResponseHandlerFactory.releaseLatch();
      stream1rowcount += consumeStream(flightStream1);
      assertNotEquals(TOTAL_ROWS, stream1rowcount);
      stream1rowcount += consumeStream(flightStream1);
    }

    assertEquals(TOTAL_ROWS, stream1rowcount);
    assertEquals(TOTAL_ROWS, stream2rowcount);
    runQueryResponseHandlerFactory.assertLatchCountedDown();
  }

  /**
   * The first instance created by this factory is a UserResponseHandler which delegates to a
   * {@link RunQueryResponseHandler}. All further instances are normal {@link RunQueryResponseHandler}.
   * The first instance will wait on a latch after the first call to
   * {@link DelegatingRunQueryResponseHandler#sendData(RpcOutcomeListener, QueryWritableBatch)}
   * to pause the server from sending the second batch and so on. The latch can be released by
   * calling {@link #releaseLatch()}. Tests can validate the latch released because of countDown()
   * by calling {@link #assertLatchCountedDown}.
   */
  private static final class DelegatingFirstRunQueryResponseHandlerFactory implements RunQueryResponseHandlerFactory {
    private final CountDownLatch firstSendDataLatch;
    private final AtomicBoolean didLatchCountDown;
    private boolean isFirstHandlerCreated;

    private DelegatingFirstRunQueryResponseHandlerFactory() {
      this.firstSendDataLatch = new CountDownLatch(1);
      this.isFirstHandlerCreated = false;
      this.didLatchCountDown = new AtomicBoolean(false);
    }

    void releaseLatch() {
      firstSendDataLatch.countDown();
    }

    void assertLatchCountedDown() {
      assertTrue("Test may be getting flaky, or resources are low. " +
        "Try extending the timeout of the latch here.", didLatchCountDown.get());
    }

    @Override
    public UserResponseHandler getHandler(UserBitShared.ExternalId runExternalId, UserSession userSession,
                                          Provider<UserWorker> workerProvider,
                                          Provider<OptionManager> optionManagerProvider,
                                          FlightProducer.ServerStreamListener clientListener,
                                          BufferAllocator allocator) {
      if (!isFirstHandlerCreated) {
        isFirstHandlerCreated = true;
        return new DelegatingRunQueryResponseHandler(runExternalId, userSession, workerProvider, optionManagerProvider,
          clientListener, allocator, firstSendDataLatch, didLatchCountDown);
      } else {
        return RunQueryResponseHandlerFactory.DEFAULT.getHandler(runExternalId, userSession, workerProvider,
          optionManagerProvider, clientListener, allocator);
      }
    }

    private static final class DelegatingRunQueryResponseHandler implements UserResponseHandler {

      private final RunQueryResponseHandler delegate;
      private final CountDownLatch firstSendDataLatch;
      private final AtomicBoolean didLatchCountDown;
      private final AtomicBoolean hasLatched = new AtomicBoolean(false);

      DelegatingRunQueryResponseHandler(UserBitShared.ExternalId runExternalId,
                                        UserSession userSession,
                                        Provider<UserWorker> workerProvider,
                                        Provider<OptionManager> optionManagerProvider,
                                        FlightProducer.ServerStreamListener clientListener,
                                        BufferAllocator allocator,
                                        CountDownLatch firstSendDataLatch,
                                        AtomicBoolean didLatchCountDown) {
        this.delegate = new BasicResponseHandler(runExternalId, userSession, workerProvider, clientListener, allocator);
        this.firstSendDataLatch = firstSendDataLatch;
        this.didLatchCountDown = didLatchCountDown;
      }

      @Override
      public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
        delegate.sendData(outcomeListener, result);
        if (hasLatched.compareAndSet(false, true)) {
          try {
            firstSendDataLatch.await(20, TimeUnit.SECONDS);
            if (firstSendDataLatch.getCount() == 0) {
              didLatchCountDown.set(true);
            }
          } catch (InterruptedException e) {
            didLatchCountDown.set(false);
          }
        }
      }

      @Override
      public void completed(UserResult result) {
        delegate.completed(result);
      }
    }
  }

  private int consumeStream(FlightStream stream) {
    final VectorSchemaRoot root = stream.getRoot();
    stream.next();
    final int rowCount = stream.getRoot().getRowCount();
    root.clear();
    return rowCount;
  }
}
