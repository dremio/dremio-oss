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

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class TestUpdatableTimer extends TestMetricsBase {
  @Test
  public void test() throws Exception {
    UpdatableTimer timer = UpdatableTimer.of("my.updatable.timer");

    timer.updateOnError(2, TimeUnit.SECONDS);
    timer.updateOnSuccess(1, TimeUnit.SECONDS);
    timer.updateOnError(3, TimeUnit.SECONDS, IOException.class);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicBoolean foundError = new AtomicBoolean(false);
    AtomicBoolean foundSuccess = new AtomicBoolean(false);
    AtomicBoolean foundErrorWithException = new AtomicBoolean(false);

    processLines(
        sw.toString(),
        line -> {
          if (line.startsWith(
              "my_updatable_timer_seconds_sum{exception=\"none\",outcome=\"error\",}")) {
            assertEquals("2.0", line.split(" ")[1]);
            foundError.set(true);
          } else if (line.startsWith(
              "my_updatable_timer_seconds_sum{exception=\"none\",outcome=\"success\",}")) {
            assertEquals("1.0", line.split(" ")[1]);
            foundSuccess.set(true);
          } else if (line.startsWith(
              "my_updatable_timer_seconds_sum{exception=\"IOException\",outcome=\"error\",}")) {
            assertEquals("3.0", line.split(" ")[1]);
            foundErrorWithException.set(true);
          }
        });

    assertTrue(foundError.get());
    assertTrue(foundSuccess.get());
    assertTrue(foundErrorWithException.get());
  }
}
