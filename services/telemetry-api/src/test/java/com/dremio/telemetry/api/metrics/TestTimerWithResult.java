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

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTimerWithResult extends TestMetricsBase {
  private static final String FLOAT_REGEX = "\\d+\\.\\d+";

  @Test
  public void testTimerWithResult() throws Exception {
    Tags tags = Tags.of(Tag.of("timerWithResult", "ohMy"));
    TimerWithResult t1 =
        new TimerWithResult(
            io.micrometer.core.instrument.Metrics.globalRegistry,
            "t1",
            "The most awesome timer",
            tags);

    t1.record(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            // ignore
          }
        });

    t1.record(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            // ignore
          }
          throw new RuntimeException("failure");
        });

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nT1SuccessMetricsFound = new AtomicInteger(0);
    AtomicInteger nT1FailureMetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith(
              "t1_seconds_sum{" + tagsToString(tags.and(CommonTags.TAG_RESULT_SUCCESS)) + ",}")) {
            String[] parts = s.split(" ");
            Assertions.assertTrue(
                parts[1].trim().matches(FLOAT_REGEX), "Metric value should be a float: " + s);

            // Actual float value is usually slightly larger than 2 (e.g., 2.010308958), so truncate
            // to int
            Assertions.assertEquals(
                2, (int) Float.parseFloat(parts[1].trim()), "metric value should be 2.0: " + s);

            if (nT1SuccessMetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 't1' and result 'success'");
            }
          }

          if (s.startsWith(
              "t1_seconds_sum{" + tagsToString(tags.and(CommonTags.TAG_RESULT_FAILURE)) + ",}")) {
            String[] parts = s.split(" ");
            Assertions.assertTrue(
                parts[1].trim().matches(FLOAT_REGEX), "Metric value should be a float: " + s);

            // Actual float value is usually slightly larger than 1 (e.g., 1.013308958), so truncate
            // to int
            Assertions.assertEquals(
                1, (int) Float.parseFloat(parts[1].trim()), "metric value should be 1.0: " + s);

            if (nT1FailureMetricsFound.incrementAndGet() > 1) {
              Assertions.fail("Found more than one metric with name 't1' and result 'failure'");
            }
          }
        });

    Assertions.assertEquals(
        1,
        nT1SuccessMetricsFound.get(),
        "Could not find metric with name 't1' and result 'success'");
    Assertions.assertEquals(
        1,
        nT1FailureMetricsFound.get(),
        "Could not find metric with name 't1' and result 'failure'");
  }

  @Test
  public void testWithNewTags() throws Exception {
    Tags tags = Tags.of(Tag.of("timerWithResult", "ohMy"));
    TimerWithResult t1 =
        new TimerWithResult(
            io.micrometer.core.instrument.Metrics.globalRegistry,
            "t1",
            "The most awesome timer",
            tags);

    Tags newTags = Tags.of(Tag.of("newTag", "newValue"));
    TimerWithResult t1WithNewTags = t1.withNewTags(newTags);

    t1WithNewTags.record(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            // ignore
          }
        });

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nT1WithNewTagsMetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith(
              "t1_seconds_sum{"
                  + tagsToString(newTags.and(CommonTags.TAG_RESULT_SUCCESS))
                  + ",}")) {
            nT1WithNewTagsMetricsFound.incrementAndGet();
          }
          if (s.startsWith(
                  "t1_seconds_sum{" + tagsToString(tags.and(CommonTags.TAG_RESULT_SUCCESS)) + ",}")
              || s.startsWith(
                  "t1_seconds_sum{"
                      + tagsToString(tags.and(CommonTags.TAG_RESULT_FAILURE))
                      + ",}")) {
            Assertions.fail("Found metric with name 't1' and old tags");
          }
        });

    Assertions.assertEquals(
        1, nT1WithNewTagsMetricsFound.get(), "Could not find metric with name 't1' and new tags");
  }

  @Test
  public void testWithAddedTags() throws Exception {
    Tags tags = Tags.of(Tag.of("timerWithResult", "ohMy"));
    TimerWithResult t1 =
        new TimerWithResult(
            io.micrometer.core.instrument.Metrics.globalRegistry,
            "t1",
            "The most awesome timer",
            tags);

    Tags addedTags = Tags.of(Tag.of("newTag", "newValue"));
    TimerWithResult t1WithAddedTags = t1.withAddedTags(addedTags);

    t1WithAddedTags.record(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            // ignore
          }
        });

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nT1WithAddedTagsMetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith(
              "t1_seconds_sum{"
                  + tagsToString(tags.and(addedTags).and(CommonTags.TAG_RESULT_SUCCESS))
                  + ",}")) {
            nT1WithAddedTagsMetricsFound.incrementAndGet();
          }
          if (s.startsWith(
                  "t1_seconds_sum{" + tagsToString(tags.and(CommonTags.TAG_RESULT_SUCCESS)) + ",}")
              || s.startsWith(
                  "t1_seconds_sum{"
                      + tagsToString(tags.and(CommonTags.TAG_RESULT_FAILURE))
                      + ",}")) {
            Assertions.fail("Found metric with name 't1' and old tags");
          }
        });

    Assertions.assertEquals(
        1,
        nT1WithAddedTagsMetricsFound.get(),
        "Could not find metric with name 't1' and added tags");
  }

  @Test
  public void testGetTimer() throws Exception {
    Tags tags = Tags.of(Tag.of("timerWithResult", "ohMy"));
    TimerWithResult t1 =
        new TimerWithResult(
            io.micrometer.core.instrument.Metrics.globalRegistry,
            "t1",
            "The most awesome timer",
            tags);

    Timer t1Timer = t1.getTimer();
    t1Timer.record(1, TimeUnit.SECONDS);

    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));

    AtomicInteger nT1WithTagsMetricsFound = new AtomicInteger(0);

    processLines(
        sw.toString(),
        s -> {
          if (s.startsWith("t1_seconds_sum{" + tagsToString(tags) + ",}")) {
            nT1WithTagsMetricsFound.incrementAndGet();
          }
          if (s.startsWith(
                  "t1_seconds_sum{" + tagsToString(tags.and(CommonTags.TAG_RESULT_SUCCESS)) + ",}")
              || s.startsWith(
                  "t1_seconds_sum{"
                      + tagsToString(tags.and(CommonTags.TAG_RESULT_FAILURE))
                      + ",}")) {
            Assertions.fail("Found metric with name 't1' and extra unexpected tags");
          }
        });

    Assertions.assertEquals(
        1, nT1WithTagsMetricsFound.get(), "Could not find metric with name 't1' its tags");
  }
}
