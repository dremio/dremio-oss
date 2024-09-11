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

public class TestCounterWithOutcome extends TestMetricsBase {
  @Test
  public void test() throws Exception {
    CounterWithOutcome counter = CounterWithOutcome.of("counter.with.outcome");
    counter.succeeded();
    counter.succeeded();
    counter.succeeded();
    counter.errored();
    counter.errored();
    counter.userErrored();

    assertEquals(3, counter.countSucceeded());
    assertEquals(2, counter.countErrored());
    assertEquals(1, counter.countUserErrored());

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicBoolean foundSucceeded = new AtomicBoolean(false);
    AtomicBoolean foundErrored = new AtomicBoolean(false);
    AtomicBoolean foundUserErrored = new AtomicBoolean(false);

    processLines(
        sw.toString(),
        line -> {
          if (line.startsWith("counter_with_outcome_total{outcome=\"error\"")) {
            assertEquals("2.0", line.split(" ")[1]);
            foundErrored.set(true);
          } else if (line.startsWith("counter_with_outcome_total{outcome=\"success\"")) {
            assertEquals("3.0", line.split(" ")[1]);
            foundSucceeded.set(true);
          } else if (line.startsWith("counter_with_outcome_total{outcome=\"user_error\"")) {
            assertEquals("1.0", line.split(" ")[1]);
            foundUserErrored.set(true);
          }
        });

    assertTrue(foundSucceeded.get());
    assertTrue(foundErrored.get());
    assertTrue(foundUserErrored.get());
  }
}
