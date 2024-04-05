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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.dremio.common.utils.ProtobufUtils;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class TestJobProfileResource extends BaseTestServer {
  private static final String skewedProfilePath =
      "src/test/resources/testfiles/skewness_profile.json";

  @Test
  public void testGetJobProfileOperator() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getTestProfile(), 0, 1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT", jobProfileOperatorInfo.getOperatorName());
    assertEquals(10, jobProfileOperatorInfo.getOperatorType());
    assertTrue(jobProfileOperatorInfo.getOperatorMetricsMap().size() > 0);
  }

  @Test
  public void testOutputRecordsOperatorTableFunction() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getTestProfile(), 0, 6);
    assertEquals("06", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("TABLE_FUNCTION", jobProfileOperatorInfo.getOperatorName());
    assertEquals(3, jobProfileOperatorInfo.getOutputRecords());
  }

  @Test
  public void testOutputBytesInJobProfileOperator() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getTestProfile(), 0, 5);
    assertEquals("05", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT", jobProfileOperatorInfo.getOperatorName());
    assertEquals(25, jobProfileOperatorInfo.getOutputBytes());
  }

  private static byte[] readAll(InputStream is) throws IOException {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    while (is.available() > 0) {
      os.write(is.read());
    }
    return os.toByteArray();
  }

  private static UserBitShared.QueryProfile getProfileWithSkewedData() throws Exception {
    File profile = new File(skewedProfilePath);
    InputStream is = new FileInputStream(profile);
    byte[] b = readAll(is);
    is.close();
    return ProtobufUtils.fromJSONString(UserBitShared.QueryProfile.class, new String(b));
  }

  @Test
  public void testOperatorSkewnessPositive() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getProfileWithSkewedData(), 0, 0);
    assertEquals("00", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("HASH_PARTITION_SENDER", jobProfileOperatorInfo.getOperatorName());
    assertEquals(
        true, jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnRecordsProcessed());
    assertEquals(true, jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnProcessingTime());
    assertEquals(true, jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnPeakMemory());
  }

  @Test
  public void testOperatorHealthWithProcessingTimeLessThanSecond() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getProfileWithSkewedData(), 0, 5);
    assertEquals("05", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT", jobProfileOperatorInfo.getOperatorName());
    assertNull(jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnRecordsProcessed());
    assertNull(jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnProcessingTime());
    assertNull(jobProfileOperatorInfo.getJpOperatorHealth().getIsSkewedOnPeakMemory());
  }

  @Test
  public void testGetJobProfileOperatorMetricsMap() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getTestProfile(), 0, 1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT", jobProfileOperatorInfo.getOperatorName());
    assertNull(jobProfileOperatorInfo.getMetricsDetailsMap().get("Java_Build_Time"));
  }

  @Test
  public void testGetJobProfileOperatorMetricsMapWithDisplayFlagSet() throws Exception {
    JobProfileOperatorInfo jobProfileOperatorInfo =
        new JobProfileOperatorInfo(getTestProfile(), 0, 1);

    assertEquals("01", jobProfileOperatorInfo.getOperatorId());
    assertEquals("00", jobProfileOperatorInfo.getPhaseId());
    assertEquals("PROJECT", jobProfileOperatorInfo.getOperatorName());
    assertTrue(
        jobProfileOperatorInfo.getMetricsDetailsMap().get("Java_Expressions").getIsDisplayed());
    assertTrue(
        "Java_Expressions Metric value",
        Integer.parseInt(jobProfileOperatorInfo.getOperatorMetricsMap().get("Java_Expressions"))
            >= 0);
  }

  @Test
  public void testGetJobProfileOperatorAttributes() throws Exception {
    try (AutoCloseable ignore =
        withSystemOption(PlannerSettings.PRETTY_PLAN_SCRAPING.getOptionName(), "true")) {
      JobProfileOperatorInfo jobProfileOperatorInfo =
          new JobProfileOperatorInfo(getTestProfile(), 0, 6);

      assertEquals("06", jobProfileOperatorInfo.getOperatorId());
      assertEquals("00", jobProfileOperatorInfo.getPhaseId());
      assertEquals("TABLE_FUNCTION", jobProfileOperatorInfo.getOperatorName());
      assertEquals(
          "cp.\"parquet/decimals/mixedDecimalsInt32Int64FixedLengthWithStats.parquet\"",
          jobProfileOperatorInfo.getAttributes().getScanInfo().getTableName());
      try {
        String json = JsonFormat.printer().print(jobProfileOperatorInfo.getAttributes());
        String expectedJson =
            ("{\n"
                + "  \"scanInfo\": {\n"
                + "    \"tableName\": \"cp.\\\"parquet/decimals/mixedDecimalsInt32Int64FixedLengthWithStats.parquet\\\"\"\n"
                + "  }\n"
                + "}");
        assertEquals(expectedJson, json);
      } catch (Exception e) {
        System.out.println("Exception in converting RelNodeInfo protobuff to json " + e);
      }
    }
  }
}
