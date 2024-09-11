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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Tags;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestSimpleUpdatableTimer extends TestMetricsBase {
  @Test
  public void testUpdate() throws Exception {
    SimpleUpdatableTimer timer = SimpleUpdatableTimer.of("my.simple.updatable.timer");

    timer.update(2, TimeUnit.SECONDS);
    timer.update(1, TimeUnit.SECONDS);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines())
        .anyMatch(line -> line.contains("my_simple_updatable_timer_seconds_sum 3.0"));

    assertThat(sw.toString().lines())
        .anyMatch(line -> line.contains("my_simple_updatable_timer_seconds_count 2.0"));
  }

  @Test
  public void testUpdateWithTags() throws Exception {
    SimpleUpdatableTimer timer = SimpleUpdatableTimer.of("my.simple.updatable.timer");

    Tags tags = Tags.of("tag1", "value1");

    timer.update(2, TimeUnit.SECONDS, tags);
    timer.update(1, TimeUnit.SECONDS, tags);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines())
        .anyMatch(
            line -> line.contains("my_simple_updatable_timer_seconds_sum{tag1=\"value1\",} 3.0"));

    assertThat(sw.toString().lines())
        .anyMatch(
            line -> line.contains("my_simple_updatable_timer_seconds_count{tag1=\"value1\",} 2.0"));
  }
}
