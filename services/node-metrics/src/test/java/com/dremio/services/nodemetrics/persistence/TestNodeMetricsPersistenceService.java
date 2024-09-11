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

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.nodemetrics.NodeMetricsService;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Test integration of node metrics persistence runnables with the persistence service */
public class TestNodeMetricsPersistenceService {
  @Test
  public void testStart_OptionDisabled() {
    boolean executorsOnly = false;
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    OptionManager optionManager = mock(OptionManager.class);
    NodeMetricsCsvFormatter nodeMetricsCsvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);

    when(optionManager.getOption(ExecConstants.NODE_HISTORY_ENABLED)).thenReturn(false);

    NodeMetricsPersistenceService nodeMetricsPersistenceService =
        new NodeMetricsPersistenceService(
            executorsOnly,
            () -> nodeMetricsService,
            () -> schedulerService,
            () -> optionManager,
            nodeMetricsCsvFormatter,
            nodeMetricsStorage);
    nodeMetricsPersistenceService.start();

    verify(schedulerService, never()).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testStart() throws IOException {
    boolean executorsOnly = false;
    NodeMetricsService nodeMetricsService = mock(NodeMetricsService.class);
    SchedulerService schedulerService = mock(SchedulerService.class);
    OptionManager optionManager = mock(OptionManager.class);
    NodeMetricsCsvFormatter nodeMetricsCsvFormatter = mock(NodeMetricsCsvFormatter.class);
    NodeMetricsStorage nodeMetricsStorage = mock(NodeMetricsStorage.class);

    when(optionManager.getOption(ExecConstants.NODE_HISTORY_ENABLED)).thenReturn(true);
    when(nodeMetricsStorage.list(any(CompactionType.class)))
        .thenReturn(
            new DirectoryStream<>() {
              @Override
              public @NotNull Iterator<NodeMetricsFile> iterator() {
                return Collections.emptyIterator();
              }

              @Override
              public void close() {}
            });
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    when(schedulerService.schedule(any(Schedule.class), runnableArgumentCaptor.capture()))
        .thenReturn(null);

    NodeMetricsPersistenceService nodeMetricsPersistenceService =
        new NodeMetricsPersistenceService(
            executorsOnly,
            () -> nodeMetricsService,
            () -> schedulerService,
            () -> optionManager,
            nodeMetricsCsvFormatter,
            nodeMetricsStorage);
    nodeMetricsPersistenceService.start();

    verify(schedulerService, times(4)).schedule(any(Schedule.class), any(Runnable.class));

    List<Class<? extends Runnable>> expectedRunnableClasses =
        List.of(
            NodeMetricsWriter.class,
            NodeMetricsCompacter.class,
            NodeMetricsCompacter.class,
            NodeMetricsCleaner.class);
    List<Runnable> capturedRunnables = runnableArgumentCaptor.getAllValues();
    Assertions.assertEquals(4, capturedRunnables.size());
    Assertions.assertEquals(
        expectedRunnableClasses,
        capturedRunnables.stream().map(Runnable::getClass).collect(Collectors.toList()));
  }
}
