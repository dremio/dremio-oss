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
package com.dremio.telemetry.impl.config.metrics;

import static org.junit.Assert.assertEquals;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.junit.Test;

/** Tests JmxConfigurator */
public class TestJmxConfigurator {
  private static final String NAME = "jmx-test-counter";
  private static final String OBJECT_NAME = "metrics:name=" + NAME + ",type=counters";

  @Test
  public void testConfigureAndStart()
      throws IOException, InterruptedException, MalformedObjectNameException {
    // Arrange
    final MetricRegistry metrics = new MetricRegistry();
    metrics.counter(
        NAME,
        () -> {
          Counter counter = new Counter();
          counter.inc(1234);
          return counter;
        });

    final JmxConfigurator configurator =
        new JmxConfigurator(TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    // Act
    configurator.configureAndStart(
        "test",
        metrics,
        new MetricFilter() {
          @Override
          public boolean matches(String s, Metric metric) {
            return true;
          }
        });

    // Assert
    final Set<ObjectInstance> beans =
        ManagementFactory.getPlatformMBeanServer().queryMBeans(new ObjectName(OBJECT_NAME), null);

    assertEquals(1, beans.size());
    assertEquals(OBJECT_NAME, beans.iterator().next().getObjectName().getCanonicalName());
  }
}
