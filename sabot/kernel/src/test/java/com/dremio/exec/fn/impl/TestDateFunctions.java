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
package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("DX-3872")
public class TestDateFunctions extends PopUnitTestBase {
//    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDateFunctions.class);

    private void testCommon(String[] expectedResults, String physicalPlan, String resourceFile) throws Exception {
        try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
             SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
             DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

            // run query.
            bit.run();
            client.connect();
            List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
                    Files.toString(FileUtils.getResourceAsFile(physicalPlan), Charsets.UTF_8)
                            .replace("#{TEST_FILE}", resourceFile));

            try(RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator())) {
              QueryDataBatch batch = results.get(0);
              assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));


              int i = 0;
              for (VectorWrapper<?> v : batchLoader) {

                ValueVector.Accessor accessor = v.getValueVector().getAccessor();
                System.out.println(accessor.getObject(0));
                assertEquals( expectedResults[i++], accessor.getObject(0).toString());
              }
            }
            for(QueryDataBatch b : results){
                b.release();
            }
        }
    }

    @Test
    @Ignore("relies on particular timezone")
    public void testDateIntervalArithmetic() throws Exception {
        String expectedResults[] = {"2009-02-23T00:00:00.000-08:00",
                                    "2008-02-24T00:00:00.000-08:00",
                                    "1970-01-01T13:20:33.000-08:00",
                                    "2008-02-24T12:00:00.000-08:00",
                                    "2009-04-23T12:00:00.000-07:00",
                                    "2008-02-24T12:00:00.000-08:00",
                                    "2009-04-23T12:00:00.000-07:00",
                                    "2009-02-23T00:00:00.000-08:00",
                                    "2008-02-24T00:00:00.000-08:00",
                                    "1970-01-01T13:20:33.000-08:00",
                                    "2008-02-24T12:00:00.000-08:00",
                                    "2009-04-23T12:00:00.000-07:00",
                                    "2008-02-24T12:00:00.000-08:00",
                                    "2009-04-23T12:00:00.000-07:00"};
        testCommon(expectedResults, "/functions/date/date_interval_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    public void testDateDifferenceArithmetic() throws Exception {

        String[] expectedResults = {"365",
                                    "P-366DT-60S"};
        testCommon(expectedResults, "/functions/date/date_difference_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    @Ignore("age")
    public void testAge() throws Exception {
        String[] expectedResults = { "P109M16DT82800S",
                                     "P172M27D",
                                     "P-172M-27D",
                                     "P-39M-18DT-63573S"};
        testCommon(expectedResults, "/functions/date/age.json", "/test_simple_date.json");
    }

    @Test
    @Ignore
    public void testIntervalArithmetic() throws Exception {

      String expectedResults[] = {"P2Y2M",
          "P2DT3723S",
          "P2M",
          "PT3723S",
          "P28M",
          "PT7206S",
          "P7M",
          "PT1801.500S",
          "P33M18D",
          "PT8647.200S",
          "P6M19DT86399.999S",
          "PT1715.714S"};

        testCommon(expectedResults, "/functions/date/interval_arithmetic.json", "/test_simple_date.json");
    }

    @Test
    public void testToChar() throws Exception {

        String expectedResults[] = {(new LocalDate(2008, 2, 23)).toString("yyyy-MMM-dd"),
                                    (new LocalTime(12, 20, 30)).toString("HH mm ss"),
                                    (new LocalDateTime(2008, 2, 23, 12, 0, 0)).toString("yyyy MMM dd HH:mm:ss")};
        testCommon(expectedResults, "/functions/date/to_char.json", "/test_simple_date.json");
    }

    @Test
    @Ignore("relies on particular time zone")
    public void testToDateType() throws Exception {
        String expectedResults[] = {"2008-02-23T00:00:00.000-08:00",
                                    "1970-01-01T12:20:30.000-08:00",
                                    "2008-02-23T12:00:00.000-08:00",
                                    "2008-02-23T12:00:00.000-08:00"};

        testCommon(expectedResults, "/functions/date/to_date_type.json", "/test_simple_date.json");
    }
}
