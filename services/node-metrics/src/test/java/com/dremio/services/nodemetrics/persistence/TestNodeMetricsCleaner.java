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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestNodeMetricsCleaner {
  private static final Supplier<Boolean> enabled = () -> true;
  private static final Supplier<Boolean> disabled = () -> false;
  private static final long oneDay = 1;
  private static final NodeMetricsFile uncompacted = NodeMetricsPointFile.newInstance();
  private static final NodeMetricsFile compacted =
      NodeMetricsCompactedFile.newInstance(CompactionType.Single);
  private static final NodeMetricsFile recompacted =
      NodeMetricsCompactedFile.newInstance(CompactionType.Double);

  @Test
  public void testStart_ScheduleNow() {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsCleaner cleaner =
        new NodeMetricsCleaner(
            enabled, getSchedule, oneDay, nodeMetricsStorage, schedulerService, mutex);
    Instant before = TimeUtils.getNowSeconds();
    cleaner.start();

    ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
    verify(getSchedule, times(1)).apply(captor.capture());
    Assertions.assertTrue(
        TimeUtils.firstIsBeforeOrEqualsSecond(before, captor.getValue())
            && TimeUtils.firstIsAfterOrEqualsSecond(Instant.now(), captor.getValue()));
  }

  @Test
  public void testRun_Disabled() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsCleaner cleaner =
        new NodeMetricsCleaner(
            disabled, getSchedule, oneDay, nodeMetricsStorage, schedulerService, mutex);
    cleaner.run();

    verify(nodeMetricsStorage, never()).list();
    verify(schedulerService, never()).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testRun_EmptyStorage() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list())
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return Collections.emptyIterator();
              }

              @Override
              public void close() {}
            });
    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsCleaner cleaner =
        new NodeMetricsCleaner(
            enabled, getSchedule, oneDay, nodeMetricsStorage, schedulerService, mutex);
    cleaner.run();

    verify(nodeMetricsStorage, times(1)).list();
    verify(nodeMetricsStorage, never()).delete(any(NodeMetricsFile.class));
    Assertions.assertTrue(
        instantArgumentCaptor.getValue().isAfter(before.plus(Duration.ofDays(1)))
            && instantArgumentCaptor.getValue().isBefore(Instant.now().plus(Duration.ofDays(1))));
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testRun_AllRetained() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list())
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return List.of(uncompacted, compacted, recompacted).iterator();
              }

              @Override
              public void close() {}
            });
    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsCleaner cleaner =
        new NodeMetricsCleaner(
            enabled, getSchedule, oneDay, nodeMetricsStorage, schedulerService, mutex);
    cleaner.run();

    verify(nodeMetricsStorage, times(1)).list();
    verify(nodeMetricsStorage, never()).delete(any(NodeMetricsFile.class));
    Assertions.assertTrue(
        instantArgumentCaptor.getValue().isAfter(before.plus(Duration.ofDays(1)))
            && instantArgumentCaptor.getValue().isBefore(Instant.now().plus(Duration.ofDays(1))));
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testRun_AllDeleted() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list())
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return ImmutableList.of(uncompacted, compacted, recompacted).stream().iterator();
              }

              @Override
              public void close() {}
            });
    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    Instant before = TimeUtils.getNowSeconds();
    long maxAgeDays = 0;
    NodeMetricsCleaner cleaner =
        new NodeMetricsCleaner(
            enabled, getSchedule, maxAgeDays, nodeMetricsStorage, schedulerService, mutex);
    cleaner.run();

    verify(nodeMetricsStorage, times(1)).list();
    verify(nodeMetricsStorage, times(1)).delete(uncompacted);
    verify(nodeMetricsStorage, times(1)).delete(compacted);
    verify(nodeMetricsStorage, times(1)).delete(recompacted);
    Assertions.assertTrue(
        instantArgumentCaptor.getValue().isAfter(before.plus(Duration.ofDays(1)))
            && instantArgumentCaptor.getValue().isBefore(Instant.now().plus(Duration.ofDays(1))));
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }
}
