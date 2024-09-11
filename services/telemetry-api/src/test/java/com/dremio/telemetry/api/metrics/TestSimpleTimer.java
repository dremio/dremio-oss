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

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestSimpleTimer extends TestMetricsBase {
  @Test
  public void testTagOverrides() throws Exception {
    SimpleTimer timer =
        SimpleTimer.of(
            "my.simple.timer",
            Tags.of(
                Tag.of("project_id", ""),
                CommonTags.TAG_OUTCOME_SUCCESS,
                CommonTags.TAG_EXCEPTION_NONE));

    try (Timer.ResourceSample sample = timer.start()) {
      TimeUnit.MILLISECONDS.sleep(100);
      sample.tags(Tags.of(Tag.of("project_id", "test"), CommonTags.TAG_OUTCOME_ERROR));
    }

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    assertThat(sw.toString().lines())
        .anyMatch(
            line ->
                line.contains(
                    "my_simple_timer_seconds_sum{exception=\"none\",outcome=\"error\",project_id=\"test\",}"));

    assertThat(sw.toString().lines())
        .anyMatch(
            line ->
                line.contains(
                    "my_simple_timer_seconds_count{exception=\"none\",outcome=\"error\",project_id=\"test\",} 1.0"));
  }
}
