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
package com.dremio.exec.physical.impl.xsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.BigIntVector;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.util.FileUtils;
import com.dremio.common.util.TestTools;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.test.DremioTest;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSimpleExternalSort extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleExternalSort.class);

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(80, TimeUnit.SECONDS);

  @Test
  public void mergeSortWithSv2() throws Exception {
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending_sv2.json");
    int count = 0;
    for(QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() != 0) {
        count += b.getHeader().getRowCount();
      }
    }
    assertEquals(500000, count);

    long previousBigInt = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() == 0) {
        break;
      }
      batchCount++;
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      loader.load(b.getHeader().getDef(),b.getData());
      BigIntVector c1 = loader.getValueAccessorById(BigIntVector.class,
              loader.getValueVectorId(new SchemaPath("blue")).getFieldIds()).getValueVector();


      for (int i =0; i < c1.getValueCount(); i++) {
        recordCount++;
        assertTrue(String.format("%d > %d", previousBigInt, c1.get(i)), previousBigInt >= c1.get(i));
        previousBigInt = c1.get(i);
      }
      loader.clear();
      b.release();
    }

    System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
  }

  @Test
  public void sortOneKeyDescendingMergeSort() throws Throwable{
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending.json");
    int count = 0;
    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() != 0) {
        count += b.getHeader().getRowCount();
      }
    }
    assertEquals(1000000, count);

    long previousBigInt = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() == 0) {
        break;
      }
      batchCount++;
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      loader.load(b.getHeader().getDef(),b.getData());
      BigIntVector c1 = loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue")).getFieldIds()).getValueVector();

      for (int i =0; i < c1.getValueCount(); i++) {
        recordCount++;
        assertTrue(String.format("%d > %d", previousBigInt, c1.get(i)), previousBigInt >= c1.get(i));
        previousBigInt = c1.get(i);
      }
      loader.clear();
      b.release();
    }

    System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
  }

  // TODO: this test does not need fixtures setup in BaseTestQuery, since SabotNode are started below. So move it.
  @Ignore("DX-3872")
  @Test
  public void sortOneKeyDescendingExternalSort() throws Throwable{

    SabotConfig config = SabotConfig.create("dremio-external-sort.conf");
    ScanResult classpathScanResult = ClassPathScanner.fromPrescan(config);

    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit1 = new SabotNode(config, clusterCoordinator, classpathScanResult, true);
         SabotNode bit2 = new SabotNode(config, clusterCoordinator, classpathScanResult, false);
         DremioClient client = new DremioClient(config, clusterCoordinator)) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/one_key_sort_descending.json"),
                      Charsets.UTF_8));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
      }
      assertEquals(1000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() == 0) {
          break;
        }
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue")).getFieldIds()).getValueVector();

        for (int i =0; i < c1.getValueCount(); i++) {
          recordCount++;
          assertTrue(String.format("%d < %d", previousBigInt, c1.get(i)), previousBigInt >= c1.get(i));
          previousBigInt = c1.get(i);
        }
        loader.clear();
        b.release();
      }
      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    }
  }

  // TODO: this test does not need fixtures setup in BaseTestQuery, since SabotNode are started below. So move it.
  @Ignore("DX-9925")
  @Test
  public void outOfMemoryExternalSort() throws Throwable{
    SabotConfig config = SabotConfig.create("dremio-oom-xsort.conf");

    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit1 = new SabotNode(config, clusterCoordinator, DremioTest.CLASSPATH_SCAN_RESULT, true);
         DremioClient client = new DremioClient(config, clusterCoordinator)) {
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/oom_sort_test.json"),
                      Charsets.UTF_8));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
      }
      assertEquals(10000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() == 0) {
          break;
        }
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue")).getFieldIds()).getValueVector();

        for (int i =0; i < c1.getValueCount(); i++) {
          recordCount++;
          assertTrue(String.format("%d < %d", previousBigInt, c1.get(i)), previousBigInt >= c1.get(i));
          previousBigInt = c1.get(i);
        }
        assertTrue(String.format("%d == %d", c1.get(0), c1.get(c1.getValueCount() - 1)), c1.get(0) != c1.get(c1.getValueCount() - 1));
        loader.clear();
        b.release();
      }
      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));

    }
  }

  /**
   * Runs with just 10MB of memory for multiple rounds of spills.
   * Input is in sv2 format.
   * @throws Exception
   */
  @Test
  public void testExternalSortSpillingWithSv2() throws Exception {
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending_with_spill_sv2.json");
    int count = 0;
    for(QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() != 0) {
        count += b.getHeader().getRowCount();
      }
    }
    assertEquals(1000000, count);

    long previousBigInt = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() == 0) {
        break;
      }
      batchCount++;
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      loader.load(b.getHeader().getDef(),b.getData());
      BigIntVector c1 = loader.getValueAccessorById(BigIntVector.class,
        loader.getValueVectorId(new SchemaPath("blue")).getFieldIds()).getValueVector();


      for (int i =0; i < c1.getValueCount(); i++) {
        recordCount++;
        assertTrue(String.format("%d > %d", previousBigInt, c1.get(i)), previousBigInt >= c1.get(i));
        previousBigInt = c1.get(i);
      }
      loader.clear();
      b.release();
    }

    assertEquals(1000000, recordCount);
    logger.info("Sorted {} records in {} batches.", recordCount, batchCount);
  }

}
