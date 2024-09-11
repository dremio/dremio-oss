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
package com.dremio.telemetry.api.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class TestTotalAndOutcomeCounters extends TestMetricsBase {
  @Test
  public void test() throws Exception {
    TotalAndOutcomeCounters counters = TotalAndOutcomeCounters.of("my.counter");

    counters.incrementTotal();
    counters.succeeded();
    counters.succeeded();
    counters.errored();
    counters.errored();
    counters.errored();
    counters.userErrored();
    counters.userErrored();
    counters.userErrored();
    counters.userErrored();

    assertEquals(1, counters.countTotal());
    assertEquals(2, counters.countSucceeded());
    assertEquals(3, counters.countErrored());
    assertEquals(4, counters.countUserErrored());

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicBoolean foundTotal = new AtomicBoolean(false);
    AtomicBoolean foundSucceeded = new AtomicBoolean(false);
    AtomicBoolean foundErrored = new AtomicBoolean(false);
    AtomicBoolean foundUserErrored = new AtomicBoolean(false);

    processLines(
        sw.toString(),
        line -> {
          if (line.startsWith("my_counter_outcome_total{outcome=\"error\"")) {
            assertEquals("3.0", line.split(" ")[1]);
            foundErrored.set(true);
          } else if (line.startsWith("my_counter_outcome_total{outcome=\"success\"")) {
            assertEquals("2.0", line.split(" ")[1]);
            foundSucceeded.set(true);
          } else if (line.startsWith("my_counter_outcome_total{outcome=\"user_error\"")) {
            assertEquals("4.0", line.split(" ")[1]);
            foundUserErrored.set(true);
          } else if (line.startsWith("my_counter_total")) {
            assertEquals("1.0", line.split(" ")[1]);
            foundTotal.set(true);
          }
        });

    assertTrue(foundTotal.get());
    assertTrue(foundSucceeded.get());
    assertTrue(foundErrored.get());
    assertTrue(foundUserErrored.get());
  }
}
