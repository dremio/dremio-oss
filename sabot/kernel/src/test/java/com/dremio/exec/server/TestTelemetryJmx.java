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
package com.dremio.exec.server;

import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.google.common.collect.ImmutableList;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that core metrics are available through JMX based on the dremio-telemetry.yalm in the
 * resources directory.
 */
public class TestTelemetryJmx extends BaseTestQuery {

  @Before
  public void runPrimeQuery() throws Exception {
    testBuilder().sqlQuery("SELECT 1").unOrdered().baselineColumns("EXPR$0").baselineValues(1).go();
  }

  private void runTest(ObjectName objectName, List<String> expectedNames) {
    Set<ObjectInstance> objectInstances =
        ManagementFactory.getPlatformMBeanServer().queryMBeans(objectName, null);

    List<String> actualNames =
        objectInstances.stream()
            .map(obj -> obj.getObjectName().getCanonicalName())
            .sorted()
            .collect(Collectors.toList());

    // Asserting that expected metrics are appearing. Count is not strictly adhered to as
    // Jenkins runs have more metrics than workstation runs. If the metrics listed here are
    // present then the Telemetry API/Impl modules are considered to be working with JMX.
    String missing =
        expectedNames.stream()
            .map(Pattern::compile)
            .filter(
                pattern -> actualNames.stream().noneMatch(name -> pattern.matcher(name).matches()))
            .map(pattern -> String.format("'%s'", pattern.pattern()))
            .sorted()
            .collect(Collectors.joining(",\n    "));
    assertTrue(
        "Metric missing pattern(s): \n"
            + missing
            + " from complete list: \n"
            + String.join(",\n    ", actualNames),
        missing.isEmpty());
  }

  @Test
  public void testExistenceOfAllMetricsBeans() throws MalformedObjectNameException {
    runTest(new ObjectName("metrics:name=*,type=*"), objectNames);
  }

  private static final List<String> objectNames =
      ImmutableList.of(
          "metrics:name=buffer-pool\\.direct\\.capacity,type=gauges",
          "metrics:name=buffer-pool\\.direct\\.count,type=gauges",
          "metrics:name=buffer-pool\\.direct\\.used,type=gauges",
          "metrics:name=buffer-pool\\.mapped\\.capacity,type=gauges",
          "metrics:name=buffer-pool\\.mapped\\.count,type=gauges",
          "metrics:name=buffer-pool\\.mapped\\.used,type=gauges",
          "metrics:name=dremio\\.memory\\.apache_arrow_direct_current,type=gauges",
          "metrics:name=dremio\\.memory\\.apache_arrow_direct_memory_peak,type=gauges",
          "metrics:name=dremio\\.memory\\.apache_arrow_direct_memory_max,type=gauges",
          "metrics:name=dremio\\.memory\\.buffers_max,type=gauges",
          "metrics:name=dremio\\.memory\\.buffers_remaining_count,type=gauges",
          "metrics:name=dremio\\.memory\\.netty_direct_memory_current,type=gauges",
          "metrics:name=dremio\\.memory\\.netty_direct_memory_max,type=gauges",
          "metrics:name=dremio\\.memory\\.jvm_direct_current,type=gauges",
          "metrics:name=dremio\\.memory\\.jvm_direct_memory_max,type=gauges",
          "metrics:name=fabric.send_durations_ms,type=histograms",
          "metrics:name=exec\\.fragments_active,type=gauges",
          "metrics:name=gc\\..*\\.count,type=gauges",
          "metrics:name=gc\\..*\\.time,type=gauges",
          "metrics:name=jobs\\.active,type=gauges",
          "metrics:name=jobs\\.command_pool\\.active_threads,type=gauges",
          "metrics:name=jobs\\.command_pool\\.queue_size,type=gauges",
          "metrics:name=kvstore\\.lucene\\.dac-namespace\\.deleted_records,type=gauges",
          "metrics:name=kvstore\\.lucene\\.dac-namespace\\.live_records,type=gauges",
          "metrics:name=kvstore\\.lucene\\.metadata-dataset-splits\\.deleted_records,type=gauges",
          "metrics:name=kvstore\\.lucene\\.metadata-dataset-splits\\.live_records,type=gauges",
          "metrics:name=kvstore\\.lucene\\.usergroup\\.deleted_records,type=gauges",
          "metrics:name=kvstore\\.lucene\\.usergroup\\.live_records,type=gauges",
          "metrics:name=maestro\\.active,type=gauges",
          "metrics:name=memory\\.heap\\.committed,type=gauges",
          "metrics:name=memory\\.heap\\.init,type=gauges",
          "metrics:name=memory\\.heap\\.max,type=gauges",
          "metrics:name=memory\\.heap\\.usage,type=gauges",
          "metrics:name=memory\\.heap\\.used,type=gauges",
          "metrics:name=memory\\.non-heap\\.committed,type=gauges",
          "metrics:name=memory\\.non-heap\\.init,type=gauges",
          "metrics:name=memory\\.non-heap\\.max,type=gauges",
          "metrics:name=memory\\.non-heap\\.usage,type=gauges",
          "metrics:name=memory\\.non-heap\\.used,type=gauges",
          "metrics:name=memory\\.pools\\.Code.*\\.committed,type=gauges",
          "metrics:name=memory\\.pools\\.Code.*\\.init,type=gauges",
          "metrics:name=memory\\.pools\\.Code.*\\.max,type=gauges",
          "metrics:name=memory\\.pools\\.Code.*\\.usage,type=gauges",
          "metrics:name=memory\\.pools\\.Code.*\\.used,type=gauges",
          "metrics:name=memory\\.pools\\.Metaspace\\.committed,type=gauges",
          "metrics:name=memory\\.pools\\.Metaspace\\.init,type=gauges",
          "metrics:name=memory\\.pools\\.Metaspace\\.max,type=gauges",
          "metrics:name=memory\\.pools\\.Metaspace\\.usage,type=gauges",
          "metrics:name=memory\\.pools\\.Metaspace\\.used,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.committed,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.init,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.max,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.usage,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.used,type=gauges",
          "metrics:name=memory\\.pools\\..*-Space\\.used-after-gc,type=gauges",
          "metrics:name=memory\\.total\\.committed,type=gauges",
          "metrics:name=memory\\.total\\.init,type=gauges",
          "metrics:name=memory\\.total\\.max,type=gauges",
          "metrics:name=memory\\.total\\.used,type=gauges",
          "metrics:name=rpc\\.bit\\.data_current,type=gauges",
          "metrics:name=rpc\\.bit\\.data_peak,type=gauges",
          "metrics:name=rpc\\.peers,type=gauges",
          "metrics:name=rpc\\.user\\.current,type=gauges",
          "metrics:name=rpc\\.user\\.peak,type=gauges",
          "metrics:name=rpc\\.bit\\.control_current,type=gauges",
          "metrics:name=rpc\\.bit\\.control_peak,type=gauges",
          "metrics:name=threads\\.blocked\\.count,type=gauges",
          "metrics:name=threads\\.count,type=gauges",
          "metrics:name=threads\\.daemon\\.count,type=gauges",
          "metrics:name=threads\\.deadlock\\.count,type=gauges",
          "metrics:name=threads\\.deadlocks,type=gauges",
          "metrics:name=threads\\.new\\.count,type=gauges",
          "metrics:name=threads\\.runnable\\.count,type=gauges",
          "metrics:name=threads\\.terminated\\.count,type=gauges",
          "metrics:name=threads\\.timed_waiting\\.count,type=gauges",
          "metrics:name=threads\\.waiting\\.count,type=gauges",
          "metrics:name=user.send_durations_ms,type=histograms");
}
