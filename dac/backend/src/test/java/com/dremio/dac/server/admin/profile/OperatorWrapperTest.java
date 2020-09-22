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
package com.dremio.dac.server.admin.profile;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Test cases for @{@link OperatorWrapper}
 */
public class OperatorWrapperTest {

  @Test
  public void testGetMetricsTableHandlesNotRegisteredMetrics() throws IOException {

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

    OperatorWrapper ow = new OperatorWrapper(1,
                                              ImmutableList.of(pair),
                                              OperatorMetricRegistry.getCoreOperatorTypeMetricsMap(),
                                              HashBasedTable.create(),
                                              new HashSet<>());

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(outputStream);
    jsonGenerator.writeStartObject();
    ow.addMetrics(jsonGenerator);
    jsonGenerator.writeEndObject();
    jsonGenerator.flush();

    final JsonElement element = new JsonParser().parse(outputStream.toString());
    final int size = element.getAsJsonObject().get("metrics").getAsJsonObject().get("data").getAsJsonArray().get(0).getAsJsonArray().size();
    assertEquals(2, size);
  }

}
