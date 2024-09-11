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

import static com.dremio.telemetry.api.metrics.MeterProviders.newDistributionSummaryProvider;
import static io.micrometer.core.instrument.Metrics.globalRegistry;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetrics extends TestMetricsBase {
  /**
   * Tests if standard (JVM, prometheus) metrics provided by {@link Metrics} show up in metrics end
   * point
   */
  @Test
  public void testStandardAndSysExports() throws Exception {
    Map<String, Set<String>> metrics = getMetrics();
    Map<String, Set<String>> expMap = new HashMap<>();
    expMap.put("gauge", standardGauges);
    expMap.put("counter", standardCounters);
    expMap.put("summary", Collections.emptySet() /* TODO: add a test case for summary */);

    metrics.forEach(
        (k, v) -> {
          Set<String> exp = expMap.get(k);
          Assertions.assertNotNull(exp, "Unknown metrics type " + k);

          if (!exp.isEmpty() && !v.containsAll(exp)) {
            // check both ways to see which metrics are missing
            Set<String> missingInExpected =
                v.stream().filter(e -> !exp.contains(e)).collect(Collectors.toSet());
            Set<String> missingInActual =
                exp.stream().filter(e -> !v.contains(e)).collect(Collectors.toSet());

            Assertions.fail(
                "\nMissing in expected:\n"
                    + String.join("\n", missingInExpected)
                    + "\nMissing in actual:\n"
                    + String.join("\n", missingInActual));
          }
        });
  }

  @Test
  public void testPrometheusHistogramFlavor() throws Exception {
    globalRegistry.remove(Metrics.RegistryHolder.getPrometheusMeterRegistry());
    Metrics.RegistryHolder.initRegistry();

    MeterProvider<DistributionSummary> ds = newDistributionSummaryProvider("ds1");
    ds.withTags("foo", "baz", "bar", "qux").record(10);

    // scrape the metrics
    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    // make sure the new histogram _is_ affected by the histogram flavor
    assertThat(sw.toString().lines())
        .anyMatch(line -> line.contains("ds1_bucket{bar=\"qux\",foo=\"baz\",le=\"10.0\",} 1.0"));
  }

  @Test
  public void testUnregister() throws Exception {
    Gauge gauge =
        MeterProviders.newGauge(
            "my_gauge", "My gauge", Tags.of(Tag.of("my_tag", "my_value")), () -> 1.0);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines())
        .anyMatch(line -> line.contains("my_gauge{my_tag=\"my_value\",} 1.0"));

    assertThat(Metrics.unregister(gauge)).isNotNull();

    sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines()).noneMatch(line -> line.contains("my_gauge"));
  }

  @Test
  public void testUnregisterByMeterName() throws Exception {
    Gauge gauge =
        MeterProviders.newGauge(
            "my_gauge_name", "My gauge", Tags.of(Tag.of("my_tag", "my_value")), () -> 1.0);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines())
        .anyMatch(line -> line.contains("my_gauge_name{my_tag=\"my_value\",} 1.0"));

    assertThat(Metrics.unregister("my_gauge_name")).isNotNull();

    sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines()).noneMatch(line -> line.contains("my_gauge_name"));
  }

  @Test
  public void testUnregisterNonExistingMeter() throws Exception {
    assertThat(Metrics.unregister("non_existing_meter")).isNull();
  }
}
