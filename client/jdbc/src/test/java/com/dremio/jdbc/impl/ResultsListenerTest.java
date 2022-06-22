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
package com.dremio.jdbc.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.Consumer;

import org.junit.Test;

import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.jdbc.impl.DremioCursor.ResultsListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.test.DremioTest;

/**
 * Class-level unit test for {@link ResultsListener}.
 */
public class ResultsListenerTest extends DremioTest {
  private static final QueryDataBatch INSERTED_BATCH = new QueryDataBatch(QueryData.getDefaultInstance(), null);
  // Both the batchQueue.poll() timeout and EXPECTED_TIME_ELAPSED_MS have been
  // increased to 10s and 5s respectively to reduce the test flakiness
  // and to be absolutely sure that we are not waiting the full duration
  // of the poll timeout.
  private static final long BATCH_QUEUE_POLL_TIMEOUT_MS = 10000;
  private static final long EXPECTED_TIME_ELAPSED_MS = 5000;
  private static final int DELAY_MS = 5;
  private static final int THROTTLING_THRESHOLD = 100;

  @Test
  public void testEndOfStreamMessageWithQueryCompleted()
    throws Exception {
    runTest(resultsListener -> resultsListener.queryCompleted(QueryState.COMPLETED));
  }

  @Test
  public void testEndOfStreamMessageWithClose()
    throws Exception {
    runTest(resultsListener -> resultsListener.close());
  }

  private void runTest(Consumer<ResultsListener> resultsListenerConsumer) throws Exception {
    final ResultsListener resultsListener = new ResultsListener(THROTTLING_THRESHOLD, BATCH_QUEUE_POLL_TIMEOUT_MS);

    final Thread resultsListenerThread = new Thread(() -> {
      resultsListener.dataArrived(INSERTED_BATCH, null);
      try {
        // Adding a delay here as getNext() might be returning too quickly
        // as we are setting completed=true before we add the EOS message,
        // which might be getting evaluated in getNext() even before batchQueue.poll().
        Thread.sleep(DELAY_MS);
      } catch (InterruptedException e) {
        // Ignore Exception.
      }
      resultsListenerConsumer.accept(resultsListener);
    });
    resultsListenerThread.start();
    assertEquals(INSERTED_BATCH, resultsListener.getNext());
    final long startTimeMs = System.currentTimeMillis();
    assertEquals(null, resultsListener.getNext());
    final long endTimeMs = System.currentTimeMillis();
    final long actualTimeElapsedMs = endTimeMs - startTimeMs;
    resultsListener.close();
    resultsListenerThread.join();
    assertTrue(String.format("Actual time elapsed [%s]ms, exceeds Expected time [%s]ms elapsed for the call getNext()", actualTimeElapsedMs, EXPECTED_TIME_ELAPSED_MS), actualTimeElapsedMs < EXPECTED_TIME_ELAPSED_MS);
  }
}
