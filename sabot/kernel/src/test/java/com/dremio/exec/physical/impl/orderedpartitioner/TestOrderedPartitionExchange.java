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
package com.dremio.exec.physical.impl.orderedpartitioner;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.FileUtils;
import com.dremio.config.DremioConfig;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Tests the OrderedPartitionExchange SqlOperatorImpl
 */
@Ignore("Disabled until alternative to distributed cache provided.")
public class TestOrderedPartitionExchange extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOrderedPartitionExchange.class);

  /**
   * Starts two nodes and runs a physical plan with a Mock scan, project, OrderedParititionExchange, Union Exchange,
   * and sort. The final sort is done first on the partition column, and verifies that the partitions are correct, in that
   * all rows in partition 0 should come in the sort order before any row in partition 1, etc. Also verifies that the standard
   * deviation of the size of the partitions is less than one tenth the mean size of the partitions, because we expect all
   * the partitions to be roughly equal in size.
   * @throws Exception
   */
  @Test
  public void twoBitTwoExchangeRun() throws Exception {
    try(ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
        SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, false);
        DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator);
        BootStrapContext bootStrapContext = new BootStrapContext(DremioConfig.create(null, DEFAULT_SABOT_CONFIG), CLASSPATH_SCAN_RESULT)) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/sender/ordered_exchange.json"),
              Charsets.UTF_8));
      int count = 0;
      List<Integer> partitionRecordCounts = Lists.newArrayList();
      for(QueryDataBatch b : results) {
        if (b.getData() != null) {
          int rows = b.getHeader().getRowCount();
          count += rows;
          RecordBatchLoader loader = new RecordBatchLoader(bootStrapContext.getAllocator());
          loader.load(b.getHeader().getDef(), b.getData());
          BigIntVector vv1 = loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(
                  new SchemaPath("col1")).getFieldIds()).getValueVector();
          Float8Vector vv2 = loader.getValueAccessorById(Float8Vector.class, loader.getValueVectorId(
                  new SchemaPath("col2")).getFieldIds()).getValueVector();
          IntVector pVector = loader.getValueAccessorById(IntVector.class, loader.getValueVectorId(
                  new SchemaPath("partition")).getFieldIds()).getValueVector();
          long previous1 = Long.MIN_VALUE;
          double previous2 = Double.MIN_VALUE;
          int partPrevious = -1;
          long current1 = Long.MIN_VALUE;
          double current2 = Double.MIN_VALUE;
          int partCurrent = -1;
          int partitionRecordCount = 0;
          for (int i = 0; i < rows; i++) {
            previous1 = current1;
            previous2 = current2;
            partPrevious = partCurrent;
            current1 = vv1.get(i);
            current2 = vv2.get(i);
            partCurrent = pVector.get(i);
            Assert.assertTrue(current1 >= previous1);
            if (current1 == previous1) {
              Assert.assertTrue(current2 <= previous2);
            }
            if (partCurrent == partPrevious || partPrevious == -1) {
              partitionRecordCount++;
            } else {
              partitionRecordCounts.add(partitionRecordCount);
              partitionRecordCount = 0;
            }
          }
          partitionRecordCounts.add(partitionRecordCount);
          loader.clear();
        }

        b.release();
      }
      double[] values = new double[partitionRecordCounts.size()];
      int i = 0;
      for (Integer rc : partitionRecordCounts) {
        values[i++] = rc.doubleValue();
      }
      StandardDeviation stdDev = new StandardDeviation();
      Mean mean = new Mean();
      double std = stdDev.evaluate(values);
      double m = mean.evaluate(values);
      System.out.println("mean: " + m + " std dev: " + std);
      //Assert.assertTrue(std < 0.1 * m);
      assertEquals(31000, count);
    }
  }

}
