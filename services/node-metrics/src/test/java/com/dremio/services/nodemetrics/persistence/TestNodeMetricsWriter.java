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
package com.dremio.services.nodemetrics.persistence;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.nodemetrics.ImmutableNodeMetrics;
import com.dremio.services.nodemetrics.NodeMetrics;
import com.dremio.services.nodemetrics.NodeMetricsService;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestNodeMetricsWriter {
  private static final Supplier<Boolean> enabled = () -> true;
  private static final Supplier<Boolean> disabled = () -> false;
  private static final long oneMinute = 1;
  private static final NodeMetrics executor =
      new ImmutableNodeMetrics.Builder()
          .setName("executor_1")
          .setHost("executor1.local")
          .setIp("192.168.1.2")
          .setPort(1234)
          .setCpu(0.5)
          .setMemory(0.25)
          .setStatus("green")
          .setIsMaster(false)
          .setIsCoordinator(false)
          .setIsExecutor(true)
          .setIsCompatible(true)
          .setNodeTag("tag")
          .setVersion("version")
          .setStart(System.currentTimeMillis())
          .setDetails("")
          .build();

  private static final NodeMetrics coordinator =
      new ImmutableNodeMetrics.Builder()
          .setName("coordinator_1")
          .setHost("localhost")
          .setIp("192.168.1.1")
          .setPort(1234)
          .setCpu(0.2)
          .setMemory(0.1)
          .setStatus("green")
          .setIsMaster(true)
          .setIsCoordinator(true)
          .setIsExecutor(false)
          .setIsCompatible(true)
          .setNodeTag("tag")
          .setVersion("version")
          .setStart(System.currentTimeMillis())
          .setDetails("")
          .build();

  @Test
  public void testStart() {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    boolean executorsOnly = false;
    NodeMetricsCsvFormatter csvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsWriter writer =
        new NodeMetricsWriter(
            enabled,
            getSchedule,
            oneMinute,
            executorsOnly,
            csvFormatter,
            nodeMetricsStorage,
            nodeMetricsService,
            schedulerService,
            mutex);
    Instant before = TimeUtils.getNowSeconds();
    writer.start();

    ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
    verify(getSchedule, times(1)).apply(captor.capture());
    Assertions.assertTrue(
        captor.getValue().isAfter(before.plus(Duration.ofMinutes(oneMinute)))
            && captor.getValue().isBefore(Instant.now().plus(Duration.ofMinutes(oneMinute))));
  }

  @Test
  public void testRun_Disabled() {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    boolean executorsOnly = false;
    NodeMetricsCsvFormatter csvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsWriter writer =
        new NodeMetricsWriter(
            disabled,
            getSchedule,
            oneMinute,
            executorsOnly,
            csvFormatter,
            nodeMetricsStorage,
            nodeMetricsService,
            schedulerService,
            mutex);
    writer.run();

    verify(nodeMetricsService, never()).getClusterMetrics(anyBoolean());
    verify(schedulerService, never()).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testRun_NoMetrics() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    boolean executorsOnly = false;
    NodeMetricsCsvFormatter csvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsService.getClusterMetrics(true)).thenReturn(Collections.emptyList());
    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsWriter writer =
        new NodeMetricsWriter(
            enabled,
            getSchedule,
            oneMinute,
            executorsOnly,
            csvFormatter,
            nodeMetricsStorage,
            nodeMetricsService,
            schedulerService,
            mutex);
    writer.run();

    verify(nodeMetricsService, times(1)).getClusterMetrics(true);
    verify(csvFormatter, never()).toCsv(anyList());
    verify(nodeMetricsStorage, never()).write(any(InputStream.class), any(NodeMetricsFile.class));
    Assertions.assertTrue(
        instantArgumentCaptor.getValue().isAfter(before.plus(Duration.ofMinutes(oneMinute)))
            && instantArgumentCaptor
                .getValue()
                .isBefore(Instant.now().plus(Duration.ofMinutes(oneMinute))));
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testRun_Metrics() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    boolean executorsOnly = false;
    NodeMetricsCsvFormatter csvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    List<NodeMetrics> nodeMetrics = List.of(coordinator, executor);
    when(nodeMetricsService.getClusterMetrics(true)).thenReturn(nodeMetrics);
    String csv = "a,b,c";
    when(csvFormatter.toCsv(nodeMetrics)).thenReturn(csv);
    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsWriter writer =
        new NodeMetricsWriter(
            enabled,
            getSchedule,
            oneMinute,
            executorsOnly,
            csvFormatter,
            nodeMetricsStorage,
            nodeMetricsService,
            schedulerService,
            mutex);
    writer.run();

    verify(nodeMetricsService, times(1)).getClusterMetrics(true);
    verify(csvFormatter, times(1)).toCsv(nodeMetrics);
    verify(nodeMetricsStorage, times(1)).write(any(InputStream.class), any(NodeMetricsFile.class));
    Assertions.assertTrue(
        instantArgumentCaptor.getValue().isAfter(before.plus(Duration.ofMinutes(oneMinute)))
            && instantArgumentCaptor
                .getValue()
                .isBefore(Instant.now().plus(Duration.ofMinutes(oneMinute))));
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }
}
