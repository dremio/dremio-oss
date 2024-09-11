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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import io.micrometer.core.instrument.Timer;
import org.junit.After;
import org.junit.Test;

public class TestIntegrationMetricsInstrumenter {
  private static final String SERVICE_NAME = "some_service";
  private static final String OPERATION_NAME = "some_operation";

  private static final String TIME_METRIC_NAME =
      String.join(".", SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.TIME_METRIC_SUFFIX);

  private static final String COUNT_METRIC_NAME =
      String.join(".", SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.COUNT_METRIC_SUFFIX);

  private static final String ERROR_METRIC_NAME =
      String.join(".", SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.ERROR_METRIC_SUFFIX);

  private static final int EXPECTED_TOTAL_COUNT = 10;
  private static final int EXPECTED_TOTAL_ERRORS = 5;

  private final MetricsAsserter metricsAsserter = new MetricsAsserter();

  private final DefaultMetricsProvider provider = new DefaultMetricsProvider();
  private final MetricsInstrumenter metrics = new MetricsInstrumenter(SERVICE_NAME, provider);

  @After
  public void resetMetrics() {
    Metrics.resetMetrics();
  }

  @Test
  public void successfulOperationsAreLogged() {
    for (int i = 0; i < EXPECTED_TOTAL_COUNT; ++i) {
      metrics.log(OPERATION_NAME, () -> {});
    }

    metricsAsserter.assertCounterCount(COUNT_METRIC_NAME, EXPECTED_TOTAL_COUNT);
    metricsAsserter.assertCounterCount(ERROR_METRIC_NAME, 0);
    metricsAsserter.assertTimerCount(TIME_METRIC_NAME, EXPECTED_TOTAL_COUNT);
  }

  @Test
  public void failingOperationsAreLogged() {
    for (int i = 0; i < EXPECTED_TOTAL_COUNT; ++i) {
      if (i < 5) {
        // 5 successful operations
        metrics.log(OPERATION_NAME, () -> {});
      } else {
        // 5 failing operations
        assertThatThrownBy(
                () ->
                    metrics.log(
                        OPERATION_NAME,
                        () -> {
                          throw new RuntimeException();
                        }))
            .isInstanceOf(RuntimeException.class);
      }
    }

    metricsAsserter.assertCounterCount(COUNT_METRIC_NAME, EXPECTED_TOTAL_COUNT);
    metricsAsserter.assertCounterCount(ERROR_METRIC_NAME, EXPECTED_TOTAL_ERRORS);
    metricsAsserter.assertTimerCount(TIME_METRIC_NAME, EXPECTED_TOTAL_COUNT);
  }

  private static class MetricsAsserter {
    public MetricsAsserter() {
      Metrics.RegistryHolder.initRegistry();
    }

    public void assertCounterCount(String name, int expectedCount) {
      long actualCount = SimpleCounter.of(name).count();
      assertEquals(expectedCount, actualCount);
    }

    public void assertTimerCount(String name, int expectedCount) {
      long actualCount =
          io.micrometer.core.instrument.Metrics.globalRegistry.get(name).timers().stream()
              .mapToLong(Timer::count)
              .sum();
      assertEquals(expectedCount, actualCount);
    }
  }
}
