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

package com.dremio.service.jobtelemetry.server.store;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;
import com.dremio.exec.proto.UserBitShared.QueryId;

/**
 * Tests the metrics store.
 */
public abstract class AbstractTestMetricsStore {
  private final QueryId queryId1 =
    QueryId.newBuilder().setPart1(1001).setPart2(1).build();
  private final QueryId queryId2 =
    QueryId.newBuilder().setPart1(1001).setPart2(2).build();
  private final QueryId queryIdUnknown =
    QueryId.newBuilder().setPart1(1001).setPart2(0).build();
  private MetricsStore metricsStore;

  @Before
  public void setUp() throws Exception {
    metricsStore = getMetricsStore();
    metricsStore.start();
  }

  protected abstract MetricsStore getMetricsStore() throws Exception;

  @Test
  public void testQueryOneNodeSingle() {
    final String node = "q1ns";

    // single put
    metricsStore.put(queryId1, node,
      QueryProgressMetrics.newBuilder().setRowsProcessed(10).build());
    Assert.assertEquals(1, metricsStore.get(queryId1).get().getMetricsMapCount());
    Assert.assertEquals(10,
      metricsStore.get(queryId1).get().getMetricsMapMap().get(node).getRowsProcessed());
  }

  @Test
  public void testQueryOneNodeMulti() {
    final String node = "q1nm";

    // second put should overwrite first.
    metricsStore.put(queryId1, node,
      QueryProgressMetrics.newBuilder().setRowsProcessed(10).build());
    metricsStore.put(queryId1, node,
      QueryProgressMetrics.newBuilder().setRowsProcessed(30).build());
    Assert.assertEquals(1, metricsStore.get(queryId1).get().getMetricsMapCount());
    Assert.assertEquals(30,
      metricsStore.get(queryId1).get().getMetricsMapMap().get(node).getRowsProcessed());
  }

  @Test
  public void testQueryTwoNodes() {
    final String node1 = "q2nm1";
    final String node2 = "q2nm2";

    // only metrics from same node should get overwritten.
    metricsStore.put(queryId1, node1,
      QueryProgressMetrics.newBuilder().setRowsProcessed(10).build());
    metricsStore.put(queryId1, node2,
      QueryProgressMetrics.newBuilder().setRowsProcessed(30).build());
    metricsStore.put(queryId1, node2,
      QueryProgressMetrics.newBuilder().setRowsProcessed(40).build());
    Assert.assertEquals(2, metricsStore.get(queryId1).get().getMetricsMapMap().size());
    Assert.assertEquals(10,
      metricsStore.get(queryId1).get().getMetricsMapMap().get(node1).getRowsProcessed());
    Assert.assertEquals(40,
      metricsStore.get(queryId1).get().getMetricsMapMap().get(node2).getRowsProcessed());
  }

  @Test
  public void testMultiQuery() {
    final String node1 = "qm2nm1";

    // no overwrites across queries.
    metricsStore.put(queryId1, node1,
      QueryProgressMetrics.newBuilder().setRowsProcessed(10).build());
    metricsStore.put(queryId2, node1,
      QueryProgressMetrics.newBuilder().setRowsProcessed(30).build());
    Assert.assertEquals(10,
      metricsStore.get(queryId1).get().getMetricsMapMap().get(node1).getRowsProcessed());
    Assert.assertEquals(30,
      metricsStore.get(queryId2).get().getMetricsMapMap().get(node1).getRowsProcessed());
  }

  @Test
  public void testNonExistentQuery() {
    assertTrue(!metricsStore.get(queryIdUnknown).isPresent());
  }

  @After
  public void tearDown() throws Exception {
    metricsStore.close();
  }
}
