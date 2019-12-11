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
package com.dremio.telemetry.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;

import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.test.DremioTest;

/**
 * Ensure metric config is loaded and refreshed as expected.
 */
public class TestMetricConfig extends DremioTest {

  @Test
  public void tryBasicReport() throws InterruptedException {
    TestReportConfigurator trc1 = new TestReportConfigurator();
    MetricsConfigurator parent1a = new MetricsConfigurator("a", null, trc1, Arrays.asList("a\\.*"), Arrays.asList("a\\.b.*"));
    MetricsConfigurator parent1b = new MetricsConfigurator("b", null, trc1, Arrays.asList(), Arrays.asList());
    MetricsConfigurator parent2 = new MetricsConfigurator("b", null, new TestReportConfigurator(), Arrays.asList("a\\.*"), Arrays.asList("a\\.b.*"));

    Metrics.onChange(Arrays.asList(parent1a));

    {
      Collection<MetricsConfigurator> parent = Metrics.getConfigurators();
      assertEquals(1, parent.size());
      // ensure that reporter was called as expected.
      TestReportConfigurator trc = (TestReportConfigurator) parent.iterator().next().getConfigurator();
      Thread.sleep(120);
      assertTrue(trc.getCount() > 5);
    }

    // make sure that parent 2 is added without changing parent1a.
    Metrics.onChange(Arrays.asList(parent1a, parent2));
    assertThat("swapped item is correct", Arrays.asList(parent1a, parent2), IsIterableContainingInAnyOrder.containsInAnyOrder(Metrics.getConfigurators().toArray()));

    // make sure that parent1a is swapped with parent 1b.
    Metrics.onChange(Arrays.asList(parent1b, parent2));
    assertThat("swapped item is correct", Arrays.asList(parent1b, parent2), IsIterableContainingInAnyOrder.containsInAnyOrder(Metrics.getConfigurators().toArray()));
  }
}
