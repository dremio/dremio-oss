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

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;

public class TestJobProfileResource extends BaseTestServer {

  UserBitShared.QueryProfile queryProfile;

  @Before
  public void setUp() throws Exception {
    queryProfile = getTestProfile();
  }

  @Test
  public void testGetJobProfileOperator() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo = new JobProfileOperatorInfo(queryProfile,0,0);

    assertEquals("00", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00",jobProfileOperatorInfo.getPhaseId());
    assertEquals("SCREEN",jobProfileOperatorInfo.getOperatorName());
    assertEquals(13,jobProfileOperatorInfo.getOperatorType());
    assertEquals( 1,jobProfileOperatorInfo.getOperatorMetricsMap().size());
  }
}
