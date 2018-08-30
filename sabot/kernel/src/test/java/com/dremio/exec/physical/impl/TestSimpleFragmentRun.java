/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSimpleFragmentRun extends PopUnitTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleFragmentRun.class);

  @Test
  public void runNoExchangeFragment() throws Exception {
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         final SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
         final DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

    // run query.
    bit.run();
    client.connect();
    final String path = "/physical_test2.json";
//      String path = "/filter/test1.json";
    final List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile(path), Charsets.UTF_8));

    // look at records
    final RecordBatchLoader batchLoader = new RecordBatchLoader(client.getRecordAllocator());
    int recordCount = 0;
    for (final QueryDataBatch batch : results) {
      final boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
      boolean firstColumn = true;

      // print headers.
      if (schemaChanged) {
        System.out.println("\n\n========NEW SCHEMA=========\n\n");
        for (final VectorWrapper<?> value : batchLoader) {

          if (firstColumn) {
            firstColumn = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(value.getField().getName());
          System.out.print("[");
          System.out.print(value.getField().getType());
          System.out.print("]");
        }
        System.out.println();
      }

      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        boolean first = true;
        recordCount++;
        for (final VectorWrapper<?> value : batchLoader) {
          if (first) {
            first = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(value.getValueVector().getObject(i));
        }
        if (!first) {
          System.out.println();
        }
      }
      batchLoader.clear();
      batch.release();
    }
    logger.debug("Received results {}", results);
    assertEquals(recordCount, 200);
    }
  }

  @Ignore("DX-3872")
  @Test
  public void runJSONScanPopFragment() throws Exception {
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         final SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
         final DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

      // run query.
      bit.run();
      client.connect();
      final List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/physical_json_scan_test1.json"), Charsets.UTF_8)
              .replace("#{TEST_FILE}", FileUtils.getResourceAsFile("/scan_json_test_1.json").toURI().toString())
      );

      // look at records
      final RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
      int recordCount = 0;

      //int expectedBatchCount = 2;

      //assertEquals(expectedBatchCount, results.size());

      for (int i = 0; i < results.size(); ++i) {
        final QueryDataBatch batch = results.get(i);
        if (i == 0) {
          assertTrue(batch.hasData());
        } else {
          assertFalse(batch.hasData());
          batch.release();
          continue;
        }

        assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
        boolean firstColumn = true;

        // print headers.
        System.out.println("\n\n========NEW SCHEMA=========\n\n");
        for (final VectorWrapper<?> v : batchLoader) {

          if (firstColumn) {
            firstColumn = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(v.getField().getName());
          System.out.print("[");
          System.out.print(v.getField().getType());
          System.out.print("]");
        }

        System.out.println();


        for (int r = 0; r < batchLoader.getRecordCount(); r++) {
          boolean first = true;
          recordCount++;
          for (final VectorWrapper<?> v : batchLoader) {
            if (first) {
              first = false;
            } else {
              System.out.print("\t");
            }

            final ValueVector vv = v.getValueVector();
            System.out.print(vv.getObject(r));
          }
          if (!first) {
            System.out.println();
          }
        }
        batchLoader.clear();
        batch.release();
      }

      assertEquals(2, recordCount);
    }
  }
}
