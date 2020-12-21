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

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Provider;

import org.apache.arrow.flight.BackpressureStrategy.WaitResult;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import com.dremio.service.flight.impl.RunQueryResponseHandler.BackpressureHandlingResponseHandler;

/**
 * This test measures to determine that signals from a Flight Client which is not consuming data fast
 * enough will be recognized by the server, causing the server stop adding data to the stream.
 * {@link DelegatingWaitCountingResponseHandlerFactory} creates a
 * {@link DelegatingWaitCountingResponseHandlerFactory.DelegatingWaitCountingResponseHandler}
 * which extends {@link RunQueryResponseHandler} to add instrumentation.
 * The test executes the query with {@link FlightClient#getInfo} and {@link FlightClient#getStream}
 * then waits on a latch. When the server calls {@link UserResponseHandler#sendData}, the overridden
 * method in DelegatingSleepCountingResponseHandler will check if the
 * {@link ServerStreamListener#isReady()} is false (which it won't be initially). After several
 * buffers of data, {@link ServerStreamListener#isReady()} is expected to report as false. The test
 * will then wait, which should cause the server to also wait. The waits are captured by
 * DelegatingSleepCountingResponseHandler. After the wait expires, consumption resumes.
 * Finally the test validates itself by ensuring an appropriate amount of waiting took place, and
 * that the server did see a false value from {@link ServerStreamListener#isReady()}
 */
public class ITBackPressure extends BaseFlightQueryTest {
  private static final byte[] QUERY = ("select * from " +
    "cp.\"/10k_rows.parquet\" A , " +
    "cp.\"/10k_rows.parquet\" B " +
    "limit 250000")
    .getBytes(StandardCharsets.UTF_8);

  private static final DelegatingWaitCountingResponseHandlerFactory waitCountingResponseHandlerFactory
    = new DelegatingWaitCountingResponseHandlerFactory();

  @BeforeClass
  public static void setup() throws Exception {
    setupBaseFlightQueryTest(
      false,
      true,
      "backpressure.flight.endpoint.port",
      waitCountingResponseHandlerFactory);
  }

  @Test
  public void ensureWaitUntilProceed() throws Exception {
    final FlightClientUtils.FlightClientWrapper wrapper = this.getFlightClientWrapper();
    final CredentialCallOption callOption = wrapper.getTokenCallOption();
    final FlightClient client = wrapper.getClient();

    final FlightInfo flightInfo = client.getInfo(FlightDescriptor.command(QUERY), callOption);

    final long waitMS = 3000L;

    try (FlightStream flightStream = client.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOption)) {

      final VectorSchemaRoot root = flightStream.getRoot();
      root.clear();

      waitCountingResponseHandlerFactory.waitOnLatch();
      Thread.sleep(waitMS);

      while (flightStream.next()) {
        root.clear();
      }

      final long waitThresholdMS = 1000L;
      final long expectedMS = waitMS - waitThresholdMS;
      assertTrue("The query above may have two few results so that the client never " +
          "reports as not-ready. Or this test is now flaky for some other reason.",
        waitCountingResponseHandlerFactory.wasClientNotReady());

      final long actualSleepMS = waitCountingResponseHandlerFactory.getWaitMS();
      assertTrue(
        String.format("Expected a wait of at least %dms but only waited for %d", expectedMS, actualSleepMS),
        actualSleepMS > expectedMS);
    }
  }

  /**
   * The instance created by this factory extends {@link RunQueryResponseHandler} in order to add
   * test instrumentation to ensure the test is working.
   */
  private static final class DelegatingWaitCountingResponseHandlerFactory implements RunQueryResponseHandlerFactory {

    private final AtomicLong waitMSCounter = new AtomicLong(0);
    private final CountDownLatch releasesWhenClientNotReadyOrQueryComplete = new CountDownLatch(1);
    private final AtomicBoolean wasClientNotReady = new AtomicBoolean(false);
    private boolean hasOneHandlerBeenCreated = false;

    private DelegatingWaitCountingResponseHandlerFactory() {
    }

    @Override
    public UserResponseHandler getHandler(UserBitShared.ExternalId runExternalId, UserSession userSession,
                                          Provider<UserWorker> workerProvider,
                                          Provider<OptionManager> optionManagerProvider,
                                          ServerStreamListener clientListener,
                                          BufferAllocator allocator) {
      if (!hasOneHandlerBeenCreated) {
        hasOneHandlerBeenCreated = true;
        return new DelegatingWaitCountingResponseHandler(runExternalId, userSession, workerProvider,
          clientListener, allocator, waitMSCounter, releasesWhenClientNotReadyOrQueryComplete, wasClientNotReady);
      } else {
        throw new RuntimeException("Test case is only valid for 1 sleep counter");
      }
    }

    long getWaitMS() {
      return waitMSCounter.get();
    }

    void waitOnLatch() throws InterruptedException {
      releasesWhenClientNotReadyOrQueryComplete.await(30, TimeUnit.SECONDS);
    }

    boolean wasClientNotReady() {
      return wasClientNotReady.get();
    }

    private static final class DelegatingWaitCountingResponseHandler extends BackpressureHandlingResponseHandler {
      private final ServerStreamListener clientListener;
      private final AtomicLong waitMSCounter;
      private final AtomicBoolean wasClientNotReady;
      private final CountDownLatch releasesWhenClientNotReadyOrQueryComplete;

      DelegatingWaitCountingResponseHandler(UserBitShared.ExternalId runExternalId,
                                            UserSession userSession,
                                            Provider<UserWorker> workerProvider,
                                            ServerStreamListener clientListener,
                                            BufferAllocator allocator,
                                            AtomicLong waitMSCounter,
                                            CountDownLatch releasesWhenClientNotReadyOrQueryComplete,
                                            AtomicBoolean wasClientNotReady) {
        super(runExternalId, userSession, workerProvider, clientListener, allocator);
        this.clientListener = clientListener;
        this.waitMSCounter = waitMSCounter;
        this.releasesWhenClientNotReadyOrQueryComplete = releasesWhenClientNotReadyOrQueryComplete;
        this.wasClientNotReady = wasClientNotReady;
      }

      @Override
      public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
        if (!clientListener.isReady()) {
          wasClientNotReady.set(true);
          releasesWhenClientNotReadyOrQueryComplete.countDown();
        }
        super.sendData(outcomeListener, result);
      }

      @Override
      public void completed(UserResult result) {
        releasesWhenClientNotReadyOrQueryComplete.countDown();
        super.completed(result);
      }

      @Override
      WaitResult clientIsReadyForData() {
        final long startTime = System.currentTimeMillis();
        final WaitResult result = super.clientIsReadyForData();
        final long elapsedTime = System.currentTimeMillis() - startTime;

        waitMSCounter.addAndGet(elapsedTime);

        return result;
      }
    }
  }
}
