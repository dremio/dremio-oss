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
package com.dremio.service.jobtelemetry.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.Pointer;
import com.dremio.service.jobtelemetry.server.store.LocalMetricsStore;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for ProgressMetricsPublisher. */
public class TestProgressMetricsPublisher {
  private ProgressMetricsPublisher publisher;
  private MetricsStore metricsStore;

  @Before
  public void setUp() throws Exception {
    metricsStore = new LocalMetricsStore();
    metricsStore.start();

    publisher = new ProgressMetricsPublisher(metricsStore, 10);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(publisher, metricsStore);
  }

  @Test
  public void testSingleQuery() throws InterruptedException {
    UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1000).setPart2(20).build();

    Pointer<Long> recordsProcessed = new Pointer<>();
    Consumer<CoordExecRPC.QueryProgressMetrics> consumer =
        x -> recordsProcessed.value = x.getRowsProcessed();
    publisher.addSubscriber(queryId, consumer);

    Thread.sleep(100);
    assertNull(recordsProcessed.value);

    putMetrics(queryId, 2, 100);
    Thread.sleep(100);
    assertEquals(200, recordsProcessed.value.longValue());

    putMetrics(queryId, 3, 100);
    publisher.removeSubscriber(queryId, consumer, true);
    assertEquals(300, recordsProcessed.value.longValue());
  }

  @Test
  public void testSingleQueryMultipleConsumers() throws InterruptedException {
    Map<Integer, Long> recordsProcessedMap = new HashMap<>();
    List<UserBitShared.QueryId> queryIdList = new ArrayList<>();
    List<Consumer<CoordExecRPC.QueryProgressMetrics>> consumers = new ArrayList<>();

    UserBitShared.QueryId queryId =
        UserBitShared.QueryId.newBuilder().setPart1(1000).setPart2(100).build();

    putMetrics(queryId, 2, 100);

    for (int i = 0; i < 4; i++) {
      final int idx = i;
      Consumer<CoordExecRPC.QueryProgressMetrics> consumer =
          x -> recordsProcessedMap.put(idx, x.getRowsProcessed());
      consumers.add(consumer);

      publisher.addSubscriber(queryId, consumer);
    }
    Thread.sleep(100);

    for (int i = 0; i < 4; i++) {
      assertEquals(200, recordsProcessedMap.get(i).longValue());
    }

    for (int i = 0; i < 4; i++) {
      publisher.removeSubscriber(queryId, consumers.get(i), true);
    }
  }

  @Test
  public void testMultipleQueries() throws InterruptedException {
    Map<Integer, Long> recordsProcessedMap = new HashMap<>();
    List<UserBitShared.QueryId> queryIdList = new ArrayList<>();
    List<Consumer<CoordExecRPC.QueryProgressMetrics>> consumers = new ArrayList<>();

    for (int i = 1; i < 5; i++) {
      UserBitShared.QueryId queryId =
          UserBitShared.QueryId.newBuilder().setPart1(1000).setPart2(i).build();
      queryIdList.add(queryId);

      Consumer<CoordExecRPC.QueryProgressMetrics> consumer =
          x -> recordsProcessedMap.put((int) queryId.getPart2(), x.getRowsProcessed());
      consumers.add(consumer);

      publisher.addSubscriber(queryId, consumer);
      putMetrics(queryId, i, 100);
    }
    Thread.sleep(100);

    for (int i = 1; i < 5; i++) {
      assertEquals(i * 100, recordsProcessedMap.get(i).longValue());
      publisher.removeSubscriber(queryIdList.get(i - 1), consumers.get(i - 1), true);
    }
  }

  private void putMetrics(UserBitShared.QueryId queryId, int numNodes, int recordsPerNode) {
    for (int i = 0; i < numNodes; ++i) {
      metricsStore.put(
          queryId,
          "node" + i,
          CoordExecRPC.QueryProgressMetrics.newBuilder().setRowsProcessed(recordsPerNode).build());
    }
  }
}
