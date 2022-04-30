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
package com.dremio.service.jobtelemetry.server;

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;

import org.junit.Test;

import com.dremio.exec.proto.CoordExecRPC;

/**
 * Test for MetricsCombiner.
 */
public class TestMetricsCombiner {
  @Test
  public void testCombine() {
    CoordExecRPC.QueryProgressMetrics metrics1 = CoordExecRPC.QueryProgressMetrics
      .newBuilder()
      .setRowsProcessed(100)
      .setOutputRecords(20)
      .build();
    CoordExecRPC.QueryProgressMetrics metrics2 = CoordExecRPC.QueryProgressMetrics
      .newBuilder()
      .setRowsProcessed(120)
      .setOutputRecords(30)
      .build();

    CoordExecRPC.QueryProgressMetrics output1 = MetricsCombiner.combine(() -> Stream.empty());
    assertEquals(0, output1.getRowsProcessed());
    assertEquals(0, output1.getOutputRecords());

    CoordExecRPC.QueryProgressMetrics output2 = MetricsCombiner.combine(() -> Stream.of(metrics1));
    assertEquals(100, output2.getRowsProcessed());
    assertEquals(20, output2.getOutputRecords());

    CoordExecRPC.QueryProgressMetrics output3 = MetricsCombiner.combine(() -> Stream.of(metrics1, metrics2));
    assertEquals(220, output3.getRowsProcessed());
    assertEquals(50, output3.getOutputRecords());
  }
}
