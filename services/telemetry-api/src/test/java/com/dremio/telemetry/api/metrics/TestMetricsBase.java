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

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

public class TestMetricsBase {
  protected HttpServletRequest newRequest(String method) {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    if (method == null) {
      method = "GET";
    }
    Mockito.when(req.getMethod()).thenReturn(method);
    return req;
  }

  @BeforeEach
  public void reset() {
    Metrics.resetMetrics();
    Metrics.RegistryHolder.initRegistry();
  }

  protected HttpServletResponse newResponse(StringWriter sw) throws IOException {
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    PrintWriter pw;
    if (sw == null) {
      pw = new PrintWriter(System.out);
    } else {
      pw = new PrintWriter(sw);
    }
    Mockito.when(res.getWriter()).thenReturn(pw);
    return res;
  }

  @VisibleForTesting
  protected static Set<String> standardGauges =
      Stream.of(
              "memory_non_heap_usage",
              "memory_pools_CodeHeap__profiled_nmethods__committed",
              "buffer_pool_direct_used",
              "memory_heap_init",
              "memory_non_heap_committed",
              "memory_pools_CodeHeap__non_nmethods__usage",
              "memory_pools_G1_Old_Gen_used_after_gc",
              "threads_count",
              "memory_pools_CodeHeap__non_profiled_nmethods__usage",
              "memory_total_committed",
              "threads_timed_waiting_count",
              "memory_pools_Compressed_Class_Space_used",
              "memory_pools_Metaspace_used",
              "memory_heap_used",
              "memory_pools_CodeHeap__profiled_nmethods__max",
              "memory_pools_Metaspace_max",
              "threads_terminated_count",
              "buffer_pool_mapped_count",
              "memory_pools_G1_Eden_Space_committed",
              "memory_pools_G1_Survivor_Space_committed",
              "memory_pools_Compressed_Class_Space_committed",
              "memory_non_heap_used",
              "memory_pools_Compressed_Class_Space_init",
              "threads_daemon_count",
              "gc_G1_Old_Generation_time",
              "memory_pools_G1_Eden_Space_used",
              "memory_pools_G1_Eden_Space_used_after_gc",
              "threads_waiting_count",
              "memory_pools_CodeHeap__non_profiled_nmethods__used",
              "memory_heap_committed",
              "memory_non_heap_max",
              "memory_pools_Metaspace_committed",
              "memory_pools_CodeHeap__non_profiled_nmethods__max",
              "memory_pools_G1_Old_Gen_used",
              "memory_pools_CodeHeap__profiled_nmethods__init",
              "threads_runnable_count",
              "memory_total_init",
              "gc_G1_Young_Generation_time",
              "memory_pools_G1_Old_Gen_init",
              "memory_pools_Metaspace_init",
              "memory_pools_CodeHeap__non_nmethods__used",
              "memory_pools_CodeHeap__non_nmethods__max",
              "memory_heap_usage",
              "memory_pools_G1_Survivor_Space_used",
              "memory_pools_CodeHeap__non_profiled_nmethods__init",
              "memory_pools_G1_Survivor_Space_usage",
              "threads_total_started_count",
              "memory_total_used",
              "buffer_pool_direct_capacity",
              "memory_pools_G1_Eden_Space_usage",
              "memory_pools_G1_Old_Gen_committed",
              "memory_heap_max",
              "memory_pools_CodeHeap__non_profiled_nmethods__committed",
              "memory_pools_G1_Eden_Space_max",
              "memory_total_max",
              "buffer_pool_mapped_capacity",
              "memory_pools_G1_Eden_Space_init",
              "memory_pools_G1_Survivor_Space_max",
              "memory_pools_Compressed_Class_Space_max",
              "memory_pools_G1_Old_Gen_usage",
              "buffer_pool_mapped_used",
              "threads_new_count",
              "memory_pools_CodeHeap__profiled_nmethods__usage",
              "threads_blocked_count",
              "memory_pools_G1_Old_Gen_max",
              "gc_G1_Old_Generation_count",
              "memory_pools_CodeHeap__non_nmethods__init",
              "memory_pools_CodeHeap__non_nmethods__committed",
              "memory_pools_CodeHeap__profiled_nmethods__used",
              "memory_pools_Metaspace_usage",
              "memory_pools_G1_Survivor_Space_used_after_gc",
              "memory_non_heap_init",
              "memory_pools_Compressed_Class_Space_usage",
              "threads_peak_count",
              "buffer_pool_direct_count",
              "memory_pools_G1_Survivor_Space_init",
              "threads_deadlock_count",
              "gc_G1_Young_Generation_count",
              "process_start_time_seconds",
              "process_open_fds",
              "process_max_fds")
          .collect(Collectors.toSet());

  @VisibleForTesting
  protected static Set<String> standardCounters =
      Stream.of("process_cpu_seconds_total").collect(Collectors.toSet());

  protected void processLines(Scanner scanner, Consumer<String> fn) {
    // convience function to scan lines, rather
    while (scanner.hasNextLine()) {
      fn.accept(scanner.nextLine());
    }
  }

  protected void processLines(String lines, Consumer<String> fn) {
    processLines(new Scanner(lines), fn);
  }

  protected Map<String, Set<String>> parseMetrics(String scrape) {
    Map<String, Set<String>> m = new HashMap<>();
    processLines(
        scrape,
        line -> {
          if (line.startsWith("# TYPE")) {
            String[] fields = line.split("\\s+");
            // foregoing length check, because this is defined format
            m.computeIfAbsent(fields[3], k -> new HashSet<>()).add(fields[2]);
          }
        });
    return m;
  }

  protected Map<String, Set<String>> getMetrics() throws Exception {
    StringWriter sw = new StringWriter();
    Metrics.MetricServletFactory.createMetricsServlet().service(newRequest(null), newResponse(sw));
    return parseMetrics(sw.toString());
  }

  protected String tagsToString(Tags tags) {
    return tags.stream()
        .map(t -> t.getKey() + "=" + '"' + t.getValue() + '"')
        .collect(Collectors.joining(","));
  }

  protected void validateQuantiles(String metricName, String scrape) {
    Set<String> quantiles =
        Stream.of("0.5", "0.75", "0.95", "0.98", "0.99", "0.999").collect(Collectors.toSet());

    AtomicInteger nMetricsFound = new AtomicInteger(0);

    processLines(
        scrape,
        s -> {
          if (s.startsWith(metricName + "{")) {
            quantiles.stream()
                .filter(i -> s.contains("quantile=\"" + i))
                .findFirst()
                .ifPresent(quantiles::remove);
            String expectedTags = tagsToString(LEGACY_TAGS);
            if (!s.contains(expectedTags)) {
              Assertions.fail("Could not find tags '" + expectedTags + "' in '" + s + "'");
            }
            if (nMetricsFound.incrementAndGet() > 6) {
              Assertions.fail("Found more than one metric with name '" + metricName + "'");
            }
          }
        });

    if (!quantiles.isEmpty()) {
      Assertions.fail("Some quantiles not found:" + String.join(",", quantiles));
    }

    // we should have found 6 entries for the metric -- 1 for each quantile
    Assertions.assertEquals(
        6, nMetricsFound.get(), "Could not find metric with name '" + metricName + "'");
  }
}
