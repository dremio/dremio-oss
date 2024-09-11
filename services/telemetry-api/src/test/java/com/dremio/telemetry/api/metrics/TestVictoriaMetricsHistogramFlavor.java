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
import io.micrometer.core.instrument.Meter.MeterProvider;
import java.io.StringWriter;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class TestVictoriaMetricsHistogramFlavor extends TestMetricsBase {
  @Test
  public void test() throws Exception {
    globalRegistry.remove(Metrics.RegistryHolder.getPrometheusMeterRegistry());
    Metrics.RegistryHolder.initRegistry(Optional.of("VictoriaMetrics"));

    MeterProvider<DistributionSummary> ds = newDistributionSummaryProvider("ds1");
    ds.withTags("foo", "baz", "bar", "qux").record(10);

    // scrape the metrics
    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    // make sure the new histogram _is_ affected by the histogram flavor
    assertThat(sw.toString().lines())
        .anyMatch(
            line ->
                line.contains(
                    "ds1_bucket{bar=\"qux\",foo=\"baz\",vmrange=\"9.5e0...1.0e1\",} 1.0"));
  }
}
