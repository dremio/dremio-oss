/*
 * Copyright (C) 2017 Dremio Corporation
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

package com.dremio.exec.physical.impl.mergereceiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("DX-3872")
public class TestMergingReceiver extends PopUnitTestBase {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergingReceiver.class);

  @Test
  public void twoBitTwoExchange() throws Exception {
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        final SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator);) {
      bit1.run();
      bit2.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
        Files.toString(FileUtils.getResourceAsFile("/mergerecv/merging_receiver.json"),
          Charsets.UTF_8));
      int count = 0;
      final RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      // print the results
      for (final QueryDataBatch b : results) {
        final QueryData queryData = b.getHeader();
        final int rowCount = queryData.getRowCount();
        count += rowCount;
        batchLoader.load(queryData.getDef(), b.getData()); // loaded but not used, just to test
        b.release();
        batchLoader.clear();
      }
      assertEquals(200000, count);
    }
  }

  @Test
  public void testMultipleProvidersMixedSizes() throws Exception {
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        final SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator);) {

      bit1.run();
      bit2.run();
      client.connect();
      final List<QueryDataBatch> results =
          client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/mergerecv/multiple_providers.json"),
                  Charsets.UTF_8));
      int count = 0;
      final RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      // print the results
      Long lastBlueValue = null;
      for (final QueryDataBatch b : results) {
        final QueryData queryData = b.getHeader();
        final int batchRowCount = queryData.getRowCount();
        count += batchRowCount;
        batchLoader.load(queryData.getDef(), b.getData());
        for (final VectorWrapper vw : batchLoader) {
          final ValueVector vv = vw.getValueVector();
          final ValueVector.Accessor va = vv.getAccessor();
          final Field materializedField = vv.getField();
          final int numValues = va.getValueCount();
          for(int valueIdx = 0; valueIdx < numValues; ++valueIdx) {
            if (materializedField.getName().equals("blue")) {
              Long val = (Long) va.getObject(valueIdx);
              if (val != null) {
                final long longValue = ((Long) va.getObject(valueIdx)).longValue();
                // check that order is ascending
                if (lastBlueValue != null) {
                  assertTrue(longValue >= lastBlueValue);
                }
                lastBlueValue = longValue;
              }
            }
          }
        }
        b.release();
        batchLoader.clear();
      }
      assertEquals(400000, count);
    }
  }

  @Test
  public void handleEmptyBatch() throws Exception {
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        final SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
        final DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator);) {

      bit1.run();
      bit2.run();
      client.connect();
      final List<QueryDataBatch> results =
          client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/mergerecv/empty_batch.json"),
                  Charsets.UTF_8));
      int count = 0;
      final RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      // print the results
      for (final QueryDataBatch b : results) {
        final QueryData queryData = b.getHeader();
        batchLoader.load(queryData.getDef(), b.getData()); // loaded but not used, for testing
        count += queryData.getRowCount();
        b.release();
        batchLoader.clear();
      }
      assertEquals(100000, count);
    }
  }
}
