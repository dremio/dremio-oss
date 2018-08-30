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
package com.dremio.dac.server.admin.profile;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.google.common.collect.ImmutableList;

/**
 * Test cases for @{@link OperatorWrapper}
 */
public class OperatorWrapperTest {

  @Test
  public void testGetMetricsTableHandlesNotRegisteredMetrics() {

    OperatorProfile op = OperatorProfile
      .newBuilder().addMetric(
        UserBitShared.MetricValue.newBuilder()
          .setMetricId(1)
          .setDoubleValue(200))
      .addMetric(
        UserBitShared.MetricValue.newBuilder()
          .setMetricId(3)
          .setDoubleValue(21))
      .setOperatorId(UserBitShared.CoreOperatorType.PARQUET_ROW_GROUP_SCAN.getNumber())
      .build();

    ImmutablePair<OperatorProfile, Integer> pair = new ImmutablePair<>(op, 1);

    OperatorWrapper ow = new OperatorWrapper(1, ImmutableList.of(pair), OperatorMetricRegistry.getCoreOperatorTypeMetricsMap());
    String html = ow.getMetricsTable();
    Pattern columnPattern = Pattern.compile("<td>.*?</td>");
    Matcher matcher = columnPattern.matcher(html);
    int count = 0;
    while (matcher.find()){
      count++;
    }

    // Unregistered metrics should not appear on profiles screen
    assertEquals(2, count);
  }

}
