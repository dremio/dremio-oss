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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class TestNodeMetricsCompacter {
  private static final Supplier<Boolean> enabled = () -> true;
  private static final Supplier<Boolean> disabled = () -> false;
  private static final long zeroMinutes = 0;
  private static final long tenMinutes = 10;
  // For these, we need to manually create the filename to ensure they are unique across the
  // multiple node metrics files of that compaction type, since the name is based on
  // second-precision and would otherwise very likely be duplicated across the files
  private static final NodeMetricsFile uncompacted =
      NodeMetricsPointFile.from(TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(1)));
  private static final NodeMetricsFile uncompacted2 =
      NodeMetricsPointFile.from(TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(2)));
  private static final NodeMetricsFile compacted =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(1), CompactionType.Single));
  private static final NodeMetricsFile compacted2 =
      NodeMetricsCompactedFile.from(
          TimeUtils.getNodeMetricsFilename(Duration.ofSeconds(2), CompactionType.Single));

  @Test
  public void testStart_ScheduleNow() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    CompactionType compactionType = CompactionType.Single;
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list(CompactionType.Uncompacted))
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return List.of(uncompacted, uncompacted2).iterator();
              }

              @Override
              public void close() {}
            });

    NodeMetricsCompacter compacter =
        new NodeMetricsCompacter(
            enabled,
            getSchedule,
            zeroMinutes, // Test file eligible for compaction
            compactionType,
            nodeMetricsStorage,
            schedulerService,
            mutex);
    Instant before = TimeUtils.getNowSeconds();
    compacter.start();

    ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
    verify(getSchedule, times(1)).apply(captor.capture());
    Assertions.assertTrue(
        TimeUtils.firstIsBeforeOrEqualsSecond(before, captor.getValue())
            && TimeUtils.firstIsAfterOrEqualsSecond(Instant.now(), captor.getValue()));
  }

  @Test
  public void testStart_ScheduleForLater_TooNew() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    CompactionType compactionType = CompactionType.Single;
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list(CompactionType.Uncompacted))
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return List.of(uncompacted).iterator();
              }

              @Override
              public void close() {}
            });

    NodeMetricsCompacter compacter =
        new NodeMetricsCompacter(
            enabled,
            getSchedule,
            tenMinutes, // Test file not eligible for compaction
            compactionType,
            nodeMetricsStorage,
            schedulerService,
            mutex);
    Instant before = TimeUtils.getNowSeconds();
    compacter.start();

    ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
    verify(getSchedule, times(1)).apply(captor.capture());
    Assertions.assertTrue(
        captor.getValue().isAfter(before.plus(Duration.ofMinutes(tenMinutes)))
            && captor.getValue().isBefore(Instant.now().plus(Duration.ofMinutes(tenMinutes))));
  }

  @Test
  public void testStart_ScheduleForLater_NotEnough() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    CompactionType compactionType = CompactionType.Single;
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsFile olderFileThanScheduleInterval = mock(NodeMetricsFile.class);
    when(olderFileThanScheduleInterval.getWriteTimestamp())
        .thenReturn(Instant.now().minus(Duration.ofDays(1)).atZone(ZoneId.of("UTC")));

    when(nodeMetricsStorage.list(CompactionType.Uncompacted))
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return List.of(olderFileThanScheduleInterval).iterator();
              }

              @Override
              public void close() {}
            }); // Need at least two to compact, won't run on startup

    NodeMetricsCompacter compacter =
        new NodeMetricsCompacter(
            enabled,
            getSchedule,
            tenMinutes,
            compactionType,
            nodeMetricsStorage,
            schedulerService,
            mutex);
    Instant before = TimeUtils.getNowSeconds();
    compacter.start();

    ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
    verify(getSchedule, times(1)).apply(captor.capture());
    Assertions.assertTrue(
        captor.getValue().isAfter(before.plus(Duration.ofMinutes(tenMinutes)))
            && captor.getValue().isBefore(Instant.now().plus(Duration.ofMinutes(tenMinutes))));
  }

  @Test
  public void testRun_Disabled() throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    CompactionType compactionType = CompactionType.Single;
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    NodeMetricsCompacter compacter =
        new NodeMetricsCompacter(
            disabled,
            getSchedule,
            tenMinutes,
            compactionType,
            nodeMetricsStorage,
            schedulerService,
            mutex);
    compacter.run();

    verify(nodeMetricsStorage, never()).list(CompactionType.Uncompacted);
    verify(schedulerService, never()).schedule(any(Schedule.class), any(Runnable.class));
  }

  private static Stream<Arguments> compactAllArgsProvider() {
    return Stream.of(
        Arguments.of(
            CompactionType.Single, CompactionType.Uncompacted, List.of(uncompacted, uncompacted2)),
        Arguments.of(CompactionType.Double, CompactionType.Single, List.of(compacted, compacted2)));
  }

  @ParameterizedTest()
  @MethodSource("compactAllArgsProvider")
  void testRun_CompactAll(
      CompactionType compactionType,
      CompactionType expectedListFilesType,
      List<NodeMetricsFile> eligibleFiles)
      throws IOException {
    Function<Instant, Schedule> getSchedule = (Function<Instant, Schedule>) mock(Function.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    Object mutex = new Object();

    when(nodeMetricsStorage.list(expectedListFilesType))
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return eligibleFiles.iterator();
              }

              @Override
              public void close() {}
            });

    List<InputStream> fileStreams =
        eligibleFiles.stream()
            .map((NodeMetricsFile file) -> InputStream.nullInputStream())
            .collect(Collectors.toList());
    for (int i = 0; i < eligibleFiles.size(); i++) {
      NodeMetricsFile file = eligibleFiles.get(i);
      when(nodeMetricsStorage.open(file)).thenReturn(fileStreams.get(i));
    }

    ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
    Schedule schedule = mock(Schedule.class);
    when(getSchedule.apply(instantArgumentCaptor.capture())).thenReturn(schedule);

    NodeMetricsCompacter compacter =
        new NodeMetricsCompacter(
            enabled,
            getSchedule,
            zeroMinutes,
            compactionType,
            nodeMetricsStorage,
            schedulerService,
            mutex);
    compacter.run();

    verify(nodeMetricsStorage, times(1)).list(expectedListFilesType);
    for (NodeMetricsFile file : eligibleFiles) {
      verify(nodeMetricsStorage, times(1)).open(file);
      verify(nodeMetricsStorage, times(1)).delete(file);
    }

    ArgumentCaptor<NodeMetricsFile> compactedFileCaptor =
        ArgumentCaptor.forClass(NodeMetricsFile.class);
    verify(nodeMetricsStorage, times(1))
        .write(any(ByteArrayOutputStream.class), compactedFileCaptor.capture());
    Assertions.assertEquals(compactionType, compactedFileCaptor.getValue().getCompactionType());
    verify(schedulerService, times(1)).schedule(any(Schedule.class), any(Runnable.class));
  }
}
