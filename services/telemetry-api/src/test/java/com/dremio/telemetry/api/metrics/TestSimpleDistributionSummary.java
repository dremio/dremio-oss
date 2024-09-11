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

import static io.micrometer.core.instrument.Metrics.globalRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class TestSimpleDistributionSummary extends TestMetricsBase {
  @Test
  public void test() throws Exception {
    globalRegistry.remove(Metrics.RegistryHolder.getPrometheusMeterRegistry());
    Metrics.RegistryHolder.initRegistry();

    SimpleDistributionSummary distributionSummary =
        SimpleDistributionSummary.of("simple.distribution.summary");

    distributionSummary.recordAmount(1);
    distributionSummary.recordAmount(2);
    distributionSummary.recordAmount(3);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicBoolean foundOne = new AtomicBoolean(false);
    AtomicBoolean foundTwo = new AtomicBoolean(false);
    AtomicBoolean foundThree = new AtomicBoolean(false);

    processLines(
        sw.toString(),
        line -> {
          if (line.startsWith("simple_distribution_summary_bucket{le=\"1.0\",}")) {
            assertEquals("1.0", line.split(" ")[1]);
            foundOne.set(true);
          } else if (line.startsWith("simple_distribution_summary_bucket{le=\"2.0\",}")) {
            assertEquals("2.0", line.split(" ")[1]);
            foundTwo.set(true);
          } else if (line.startsWith("simple_distribution_summary_bucket{le=\"3.0\",}")) {
            assertEquals("3.0", line.split(" ")[1]);
            foundThree.set(true);
          }
        });

    assertTrue(foundOne.get());
    assertTrue(foundTwo.get());
    assertTrue(foundThree.get());
  }
}
