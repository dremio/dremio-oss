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

import static com.dremio.telemetry.api.metrics.Metrics.RegistryHolder.LEGACY_TAGS;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  public void testCounter() throws Exception {
    Counter c1 = Metrics.newCounter("c1", Metrics.ResetType.NEVER);
    c1.increment(1);
    c1.increment(1);
    c1.decrement(1);

    Counter c2 = Metrics.newCounter("c2", Metrics.ResetType.PERIODIC_1S);
    c2.increment(1);
    TimeUnit.SECONDS.sleep(2);
    c2.increment(1);
    TimeUnit.SECONDS.sleep(2);
    c2.increment(1);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nC1MetricsFound = new AtomicInteger(0);
    AtomicInteger nC2MetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith("c1{" + tagsToString(LEGACY_TAGS) + ",}")) {
            Assertions.assertTrue(s.endsWith(" 1.0"), "metric value should be 1.0: " + s);
            if (nC1MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 'c1'");
            }
          }
          if (s.startsWith("c2{" + tagsToString(LEGACY_TAGS) + ",}")) {
            Assertions.assertTrue(s.endsWith(" 1.0"), "metric value should be 1.0: " + s);
            if (nC2MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 'c2'");
            }
          }
        });

    Assertions.assertEquals(1, nC1MetricsFound.get(), "Could not find metric with name 'c1'");
    Assertions.assertEquals(1, nC2MetricsFound.get(), "Could not find metric with name 'c2'");
  }

  @Test
  public void testTimer() throws Exception {
    Timer t1 = Metrics.newTimer("t1", Metrics.ResetType.NEVER);
    t1.update(17, TimeUnit.SECONDS);
    t1.update(23, TimeUnit.SECONDS);
    t1.update(11, TimeUnit.SECONDS);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));
    validateQuantiles("t1", sw.toString());
  }

  @Test
  public void testHistogram() throws Exception {
    Histogram h = Metrics.newHistogram("h1", Metrics.ResetType.NEVER);
    h.update(10);
    h.update(39);
    h.update(90);
    h.update(70);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));
    validateQuantiles("h1", sw.toString());
  }

  @Test
  public void testGauge() throws Exception {
    Metrics.newGauge("g1", () -> 42);
    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nG1MetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith("g1{" + tagsToString(LEGACY_TAGS) + ",}")) {
            Assertions.assertTrue(s.endsWith(" 42.0"), "metric value should be 42.0: " + s);
            if (nG1MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 'g1'");
            }
          }
        });

    Assertions.assertEquals(1, nG1MetricsFound.get(), "Could not find metric with name 'g1'");
  }

  @Test
  public void testTopReporter() throws Exception {
    Metrics.newTopReporter("top1", 1, Duration.ZERO, Metrics.ResetType.NEVER);
    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nTop1MetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith("top1_0_latency{" + tagsToString(LEGACY_TAGS) + ",}")) {
            Assertions.assertTrue(s.endsWith(" 0.0"), "metric value should be 0.0: " + s);
            if (nTop1MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 'top1'");
            }
          }
        });

    Assertions.assertEquals(1, nTop1MetricsFound.get(), "Could not find metric with name 'top1'");
  }

  @Test
  public void testMicrometerAndLegacyCountersCoexist() throws Exception {
    Counter legacyC1 = Metrics.newCounter("c1", Metrics.ResetType.NEVER);
    legacyC1.increment(1);

    Tags c1Tags = Tags.of(Tag.of("meter_type", "new"));
    io.micrometer.core.instrument.Counter c1 =
        io.micrometer.core.instrument.Metrics.globalRegistry.counter("c1", c1Tags);
    c1.increment(1);
    c1.increment(1);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nLegacyC1MetricsFound = new AtomicInteger(0);
    AtomicInteger nC1MetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith("c1{" + tagsToString(LEGACY_TAGS) + ",}")) {
            Assertions.assertTrue(s.endsWith(" 1.0"), "legacy metric value should be 1.0: " + s);
            if (nLegacyC1MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one legacy metric with name 'c1'");
            }
          } else if (s.startsWith("c1")) {
            String start = "^c1(.+)" + "\\{" + tagsToString(c1Tags) + "(.*)" + "\\}(.*)$";
            Pattern p = Pattern.compile(start);
            Matcher m = p.matcher(s);
            Assertions.assertTrue(m.find());
            Assertions.assertTrue(s.endsWith(" 2.0"), "metric value should be 2.0: " + s);
            if (nC1MetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one new Micrometer metric with name 'c1'");
            }
          }
        });

    Assertions.assertEquals(
        1, nLegacyC1MetricsFound.get(), "Could not find legacy metric with name 'c1'");
    Assertions.assertEquals(
        1, nC1MetricsFound.get(), "Could not find new Micrometer metric with name 'c1'");
  }
}
