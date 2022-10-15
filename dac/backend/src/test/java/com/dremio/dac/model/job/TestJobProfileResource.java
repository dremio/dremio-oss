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

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;

public class TestJobProfileResource extends BaseTestServer {

  private UserBitShared.QueryProfile queryProfile;

  @Before
  public void setUp() throws Exception {
    queryProfile = getTestProfile();
  }

  @Test
  public void testGetJobProfileOperator() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT",jobProfileOperatorInfo.getOperatorName());
    assertEquals(10,jobProfileOperatorInfo.getOperatorType());
    assertTrue(jobProfileOperatorInfo.getOperatorMetricsMap().size()>0);
  }

  @Test
  public void testOutputRecordsOperatorTableFunction() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,6);
    assertEquals("06", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("TABLE_FUNCTION",jobProfileOperatorInfo.getOperatorName());
    assertEquals(3, jobProfileOperatorInfo.getOutputRecords());
  }

  @Test
  public void testOutputBytesInJobProfileOperator() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,5);
    assertEquals("05", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT",jobProfileOperatorInfo.getOperatorName());
    assertEquals(25, jobProfileOperatorInfo.getOutputBytes());
  }

  @Test
  public void testGetJobProfileOperatorMetricsMap() {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT",jobProfileOperatorInfo.getOperatorName());
    assertEquals(null,jobProfileOperatorInfo.getMetricsDetailsMap().get("Java_Build_Time"));
  }

  @Test
  public void testGetJobProfileOperatorMetricsMapWithDisplayFlagSet() {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT",jobProfileOperatorInfo.getOperatorName());
    assertEquals(true,jobProfileOperatorInfo.getMetricsDetailsMap().get("Java_Expressions").getIsDisplayed());
    assertTrue("Java_Expressions Metric value", Integer.valueOf(jobProfileOperatorInfo.getOperatorMetricsMap().get("Java_Expressions")) >= 0);
  }
}
