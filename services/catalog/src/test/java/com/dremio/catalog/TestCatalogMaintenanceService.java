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
package com.dremio.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestCatalogMaintenanceService {
  private static final String RUNNABLE_NAME = "test";
  private final ExecutorService executorService = Executors.newWorkStealingPool(1);

  @Mock private SchedulerService schedulerService;

  @Mock private Runnable runnable;

  private CatalogMaintenanceService service;

  @BeforeEach
  public void setUp() {
    ImmutableList<CatalogMaintenanceRunnable> runnables =
        ImmutableList.of(
            CatalogMaintenanceRunnable.builder()
                .setName(RUNNABLE_NAME)
                .setRunnable(runnable)
                .setSchedule(Schedule.Builder.everyMillis(1).build())
                .build());
    service =
        new CatalogMaintenanceService(() -> schedulerService, () -> executorService, runnables);
  }

  @Test
  public void testRunnableInvoked() throws Exception {
    // Mock schedule service.
    doAnswer(
            invocationOnMock -> {
              ((Runnable) invocationOnMock.getArgument(1)).run();
              return null;
            })
        .when(schedulerService)
        .schedule(any(), any());

    service.start();

    // Wait for any runnable to finish.
    executorService.shutdown();
    executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

    // Check that the runnable was invoked.
    verify(runnable, times(1)).run();
  }
}
