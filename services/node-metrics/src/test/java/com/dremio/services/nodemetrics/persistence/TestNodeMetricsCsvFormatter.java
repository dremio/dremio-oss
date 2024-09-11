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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.services.nodemetrics.ImmutableNodeMetrics;
import com.dremio.services.nodemetrics.NodeMetrics;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNodeMetricsCsvFormatter {
  private static final NodeMetrics coordinator =
      new ImmutableNodeMetrics.Builder()
          .setName("coordinator_1")
          .setHost("localhost")
          .setIp("192.168.1.1")
          .setPort(1234)
          .setCpu(0.2)
          .setMemory(0.1)
          .setStatus("green")
          .setIsMaster(true)
          .setIsCoordinator(true)
          .setIsExecutor(false)
          .setIsCompatible(true)
          .setNodeTag("tag")
          .setVersion("version")
          .setStart(
              ZonedDateTime.of(2024, 5, 17, 16, 17, 18, 900000000, ZoneId.of("UTC"))
                  .toInstant()
                  .toEpochMilli())
          .setDetails("")
          .build();
  private static final NodeMetrics executor =
      new ImmutableNodeMetrics.Builder()
          .setName("executor_1")
          .setHost("executor1.local")
          .setIp("192.168.1.2")
          .setPort(1234)
          .setCpu(0.5)
          .setMemory(0.25)
          .setStatus("green")
          .setIsMaster(false)
          .setIsCoordinator(false)
          .setIsExecutor(true)
          .setIsCompatible(true)
          .setNodeTag("tag")
          .setVersion("version")
          .setStart(
              ZonedDateTime.of(2024, 5, 17, 12, 31, 2, 100000000, ZoneId.of("UTC"))
                  .toInstant()
                  .toEpochMilli())
          .setDetails("")
          .build();

  @Test
  public void testToCsv() {
    Instant now =
        ZonedDateTime.of(2024, 5, 17, 12, 30, 45, 255000000, ZoneId.of("UTC")).toInstant();
    Clock testClock = Clock.fixed(now, ZoneId.of("UTC"));
    NodeMetricsCsvFormatter csvFormatter = new NodeMetricsCsvFormatter(testClock);
    String expected =
        "2024-05-17 12:30:45.255,\"coordinator_1\",\"localhost\",192.168.1.1,0.200000,0.100000,green,true,true,false,2024-05-17 16:17:18.900"
            + "\n"
            + "2024-05-17 12:30:45.255,\"executor_1\",\"executor1.local\",192.168.1.2,0.500000,0.250000,green,false,false,true,2024-05-17 12:31:02.100"
            + "\n";
    Assertions.assertEquals(expected, csvFormatter.toCsv(List.of(coordinator, executor)));
  }
}
