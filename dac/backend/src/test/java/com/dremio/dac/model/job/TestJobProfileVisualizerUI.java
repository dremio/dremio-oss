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
package com.dremio.dac.model.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.OperatorData;
import com.dremio.service.jobAnalysis.proto.PhaseData;

public class TestJobProfileVisualizerUI extends BaseTestServer {

  private UserBitShared.QueryProfile queryProfile;
  private JobProfileVisualizerUI jobProfileVisualizerUI;
  private static final String tablePath = "cp.parquet/decimals/mixedDecimalsInt32Int64FixedLengthWithStats.parquet";

  @Before
  public void setUp() throws Exception {
    queryProfile = getTestProfile();
    jobProfileVisualizerUI = new JobProfileVisualizerUI(queryProfile);
  }

  @Test
  public void testGetJobProfileInfo() throws Exception {
    List<PhaseData> phaseDataList = jobProfileVisualizerUI.getJobProfileInfo();

    //generic test cases
    assertEquals("00", phaseDataList.get(0).getPhaseId());
    assertTrue("ProcessingTime", phaseDataList.get(0).getProcessingTime() >= 0);
    assertTrue("PeakMemory", phaseDataList.get(0).getPeakMemory() >= 0);
    assertEquals("17", phaseDataList.get(0).getRecordsProcessed().toString());
    assertEquals("-1", phaseDataList.get(0).getNumThreads().toString());

    //test OperatorDataList
    List<OperatorData> operatorData = phaseDataList.get(0).getOperatorDataList();

    assertEquals(9, operatorData.size());
    assertEquals(13, operatorData.get(0).getOperatorType().intValue());
    assertEquals("SCREEN", operatorData.get(0).getOperatorName());
    assertEquals("nodeId", operatorData.get(0).getFieldName(1));
    assertEquals("", operatorData.get(0).getMergeNodeName());
    assertEquals(2, operatorData.get(0).getFieldNumber("operatorName"));
    assertEquals("00", operatorData.get(0).getNodeId());
    assertEquals("SuccessorNodes{successorId=[00-01]}", operatorData.get(0).getSuccessorId().toString());

    // //test  BaseMetrics
    BaseMetrics baseMetrics = operatorData.get(0).getBaseMetrics();

    assertEquals(3, baseMetrics.getFieldNumber("processingTime"));
    assertEquals("ioWaitTime", baseMetrics.getFieldName(1));
    assertEquals(1, baseMetrics.getRecordsProcessed().intValue());
    assertTrue("ProcessingTime", baseMetrics.getProcessingTime().intValue() >= 0);
    assertTrue("getPeakMemory", baseMetrics.getPeakMemory().intValue() >= 0);
    assertEquals(-1, baseMetrics.getNumThreads().intValue());
    assertTrue("tProcessingTime", baseMetrics.getProcessingTime().intValue() >= 0);
    assertTrue("IoWaitTime", baseMetrics.getIoWaitTime().intValue() >= 0);
    assertTrue("SetupTime", baseMetrics.getSetupTime().intValue() >= 0);
    assertEquals(null, baseMetrics.getBytesProcessed());
  }

  @Test
  public void testDatasetAndReflectionDetailsInJobProfileInfo() throws Exception {
    List<PhaseData> phaseDataList = jobProfileVisualizerUI.getJobProfileInfo();
    List<OperatorData> operatorData = phaseDataList.get(0).getOperatorDataList();

    assertEquals("00", phaseDataList.get(0).getPhaseId());
    assertEquals(9, operatorData.size());
    assertEquals(53, operatorData.get(6).getOperatorType().intValue());
    assertEquals("TABLE_FUNCTION", operatorData.get(6).getOperatorName());
    assertEquals(tablePath, operatorData.get(6).getDataSetName());
    assertEquals(null, operatorData.get(6).getReflectionName());
  }
}
