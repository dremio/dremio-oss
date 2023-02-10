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
package com.dremio.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;

@Ignore("DX-3871")
public class TestDecimal extends PopUnitTestBase{

    @Test
    public void testSimpleDecimal() throws Exception {

        /* Function checks casting from VarChar to Decimal9, Decimal18 and vice versa
         * Also tests instances where the scale might have to truncated when scale provided < input fraction
         */
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                readResourceAsString("/decimal/cast_simple_decimal.json")
                            .replace("#{TEST_FILE}", "/input_simple_decimal.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryDataBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String[] decimal9Output = {"99.0000", "11.1235", "0.1000", "-0.1200", "-123.1234", "-1.0001"};
            String[] decimal18Output = {"123456789.000000000", "11.123456789", "0.100000000", "-0.100400000", "-987654321.123456789", "-2.030100000"};

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Check the output of decimal9
            ValueVector dec9ValueVector = itr.next().getValueVector();
            ValueVector dec18ValueVector = itr.next().getValueVector();


            for (int i = 0; i < dec9ValueVector.getValueCount(); i++) {
                assertEquals(dec9ValueVector.getObject(i).toString(), decimal9Output[i]);
                assertEquals(dec18ValueVector.getObject(i).toString(), decimal18Output[i]);
            }
            assertEquals(6, dec9ValueVector.getValueCount());
            assertEquals(6, dec18ValueVector.getValueCount());

            batchLoader.clear();
            for (QueryDataBatch result : results) {
              result.release();
            }
        }
    }

    @Test
    public void testCastFromFloat() throws Exception {

        // Function checks for casting from Float, Double to Decimal data types
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                readResourceAsString("/decimal/cast_float_decimal.json")
                            .replace("#{TEST_FILE}", "/input_simple_decimal.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryDataBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String[] decimal9Output = {"99.0000", "11.1235", "0.1000", "-0.1200", "-123.1234", "-1.0001"};
            String[] decimal38Output = {"123456789.0000", "11.1235", "0.1000", "-0.1004", "-987654321.1235", "-2.0301"};

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Check the output of decimal9
            ValueVector dec9ValueVector = itr.next().getValueVector();
            ValueVector dec38ValueVector = itr.next().getValueVector();


            for (int i = 0; i < dec9ValueVector.getValueCount(); i++) {
                assertEquals(dec9ValueVector.getObject(i).toString(), decimal9Output[i]);
                assertEquals(dec38ValueVector.getObject(i).toString(), decimal38Output[i]);
            }
            assertEquals(6, dec9ValueVector.getValueCount());
            assertEquals(6, dec38ValueVector.getValueCount());

            batchLoader.clear();
            for (QueryDataBatch result : results) {
              result.release();
            }
        }
    }

    @Test
    @Ignore("decimal")
    public void testSimpleDecimalArithmetic() throws Exception {

        // Function checks arithmetic operations on Decimal18
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                readResourceAsString("/decimal/simple_decimal_arithmetic.json")
                            .replace("#{TEST_FILE}", "/input_simple_decimal.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryDataBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String[] addOutput = {"123456888.0", "22.2", "0.2", "-0.2", "-987654444.2","-3.0"};
            String[] subtractOutput = {"123456690.0", "0.0", "0.0", "0.0", "-987654198.0", "-1.0"};
            String[] multiplyOutput = {"12222222111.00" , "123.21" , "0.01", "0.01",  "121580246927.41", "2.00"};

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Check the output of add
            ValueVector addValueVector = itr.next().getValueVector();
            ValueVector subValueVector = itr.next().getValueVector();
            ValueVector mulValueVector = itr.next().getValueVector();

            for (int i = 0; i < addValueVector.getValueCount(); i++) {
                assertEquals(addValueVector.getObject(i).toString(), addOutput[i]);
                assertEquals(subValueVector.getObject(i).toString(), subtractOutput[i]);
                assertEquals(mulValueVector.getObject(i).toString(), multiplyOutput[i]);

            }
            assertEquals(6, addValueVector.getValueCount());
            assertEquals(6, subValueVector.getValueCount());
            assertEquals(6, mulValueVector.getValueCount());

            batchLoader.clear();
            for (QueryDataBatch result : results) {
              result.release();
            }
        }
    }

    @Test
    @Ignore("decimal")
    public void testComplexDecimal() throws Exception {

        /* Function checks casting between varchar and decimal38sparse
         * Also checks arithmetic on decimal38sparse
         */
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                readResourceAsString("/decimal/test_decimal_complex.json")
                            .replace("#{TEST_FILE}", "/input_complex_decimal.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryDataBatch batch = results.get(0);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String[] addOutput = {"-99999998877.700000000", "11.423456789", "123456789.100000000", "-0.119998000", "100000000112.423456789" , "-99999999879.907000000", "123456789123456801.300000000"};
            String[] subtractOutput = {"-100000001124.300000000", "10.823456789", "-123456788.900000000", "-0.120002000", "99999999889.823456789", "-100000000122.093000000", "123456789123456776.700000000"};

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            ValueVector addValueVector = itr.next().getValueVector();
            ValueVector subValueVector = itr.next().getValueVector();

            for (int i = 0; i < addValueVector.getValueCount(); i++) {
                assertEquals(addValueVector.getObject(i).toString(), addOutput[i]);
                assertEquals(subValueVector.getObject(i).toString(), subtractOutput[i]);
            }
            assertEquals(7, addValueVector.getValueCount());
            assertEquals(7, subValueVector.getValueCount());

            batchLoader.clear();
            for (QueryDataBatch result : results) {
              result.release();
            }
        }
    }

    @Test
    public void testComplexDecimalSort() throws Exception {

        // Function checks if sort output on complex decimal type works
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                readResourceAsString("/decimal/test_decimal_sort_complex.json")
                            .replace("#{TEST_FILE}", "/input_sort_complex_decimal.json")
            );

            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

            QueryDataBatch batch = results.get(1);
            assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

            String[] sortOutput = {"-100000000001.000000000000",
                                   "-100000000001.000000000000",
                                   "-145456789.120123000000",
                                   "-0.120000000000",
                                   "0.100000000001",
                                   "11.123456789012",
                                   "1278789.100000000000",
                                   "145456789.120123000000",
                                   "100000000001.123456789001",
                                   "123456789123456789.000000000000"};

            Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

            // Check the output of sort
            VectorWrapper<?> v = itr.next();
            ValueVector vv = v.getValueVector();

            for (int i = 0; i < vv.getValueCount(); i++) {
                assertEquals(sortOutput[i], vv.getObject(i).toString());
            }
            assertEquals(10, vv.getValueCount());

            batchLoader.clear();
            for (QueryDataBatch result : results) {
              result.release();
            }
        }
    }

  @Test
  @Ignore("decimal")
  public void testSimpleDecimalMathFunc() throws Exception {

    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
         DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

      // run query.
      bit.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
          readResourceAsString("/decimal/simple_decimal_math.json")
              .replace("#{TEST_FILE}", "/input_simple_decimal.json")
      );

      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

      QueryDataBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      // Check the output of decimal18
      ValueVector dec18ValueVector = itr.next().getValueVector();

      assertEquals(6, dec18ValueVector.getValueCount());

      batchLoader.clear();
      for (QueryDataBatch result : results) {
        result.release();
      }
    }
  }

}
