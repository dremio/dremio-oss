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
package com.dremio.service.nessie;


import static com.dremio.service.nessie.NessieRepoMaintenanceTask.MAINTENANCE_PERIOD_MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;

import com.dremio.options.OptionManager;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;

@ExtendWith(MockitoExtension.class)
class TestNessieRepoMaintenanceTask {

  @Mock
  private DatabaseAdapter adapter;
  @Mock
  private OptionManager options;
  @Mock
  private SchedulerService scheduler;

  private NessieRepoMaintenanceTask task;

  @BeforeEach
  void createKVStore() {
    task = new NessieRepoMaintenanceTask(adapter, options);
  }

  @Test
  void testSchedule() {
    Mockito.when(options.getOption(MAINTENANCE_PERIOD_MINUTES)).thenReturn(42L);
    task.schedule(scheduler);
    ArgumentCaptor<Schedule> schedule = ArgumentCaptor.forClass(Schedule.class);
    Mockito.verify(scheduler, Mockito.times(1)).schedule(schedule.capture(), any());
    assertThat(schedule.getValue().getPeriod()).isEqualTo(Duration.ofMinutes(42));
  }

  @Test
  void testMaintenance() {
    task.schedule(scheduler);

    ArgumentCaptor<Runnable> runnable = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(scheduler, Mockito.times(1)).schedule(any(), runnable.capture());
    assertThat(runnable.getValue()).isNotNull();

    runnable.getValue().run();

    ArgumentCaptor<RepoMaintenanceParams> params = ArgumentCaptor.forClass(RepoMaintenanceParams.class);
    Mockito.verify(adapter, Mockito.times(1)).repoMaintenance(params.capture());
    assertThat(params.getValue())
      .isEqualTo(EmbeddedRepoMaintenanceParams.builder()
        .setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder().setDryRun(false).build())
        .build());
  }
}
