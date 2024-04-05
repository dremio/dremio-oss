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
package com.dremio.service.spill;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.config.DremioConfig;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.ImmutableList;
import java.io.File;
import javax.inject.Provider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit test(s) of the SpillService */
public class TestSpillService {
  @Rule public TemporaryFolder spillParentDir = new TemporaryFolder();

  /**
   * "scheduler service" that allows the test to manually execute the (single) task that is
   * registered with this service
   */
  public class NoopScheduler implements SchedulerService {
    private Runnable taskToRun;

    @Override
    public Cancellable schedule(Schedule schedule, Runnable task) {
      taskToRun = task;
      return new Cancellable() {
        @Override
        public void cancel(boolean mayInterruptIfRunning) {}

        @Override
        public boolean isCancelled() {
          return true;
        }

        @Override
        public boolean isDone() {
          return false;
        }
      };
    }

    @Override
    public void start() {}

    @Override
    public void close() {}
  }

  class TestSpillServiceOptions extends DefaultSpillServiceOptions {
    // sweep every time the task runs
    @Override
    public long spillSweepInterval() {
      return 0;
    }

    // sweep every single directory every time the task runs
    @Override
    public long spillSweepThreshold() {
      return 0;
    }
  }

  /** Unit test of the spill service's health check */
  @Test
  public void testSpillHealthCheck() throws Exception {
    final DremioConfig config = mock(DremioConfig.class);
    final File spillDir = spillParentDir.newFolder();
    when(config.getStringList(DremioConfig.SPILLING_PATH_STRING))
        .thenReturn(ImmutableList.of(spillDir.getPath()));
    final NoopScheduler schedulerService = new NoopScheduler();
    final SpillService spillService =
        new SpillServiceImpl(
            config,
            new TestSpillServiceOptions(),
            new Provider<SchedulerService>() {
              @Override
              public SchedulerService get() {
                return schedulerService;
              }
            });

    assertEquals(null, schedulerService.taskToRun);
    spillService.start();

    assertEquals(0, spillDir.listFiles().length);
    File spillSubDir1 = new File(spillDir, "test1");
    spillSubDir1.mkdir();
    File spillSubDir2 = new File(spillDir, "test2");
    spillSubDir2.mkdir();
    File spillFile11 = new File(spillSubDir1, "file11");
    spillFile11.createNewFile();
    assertEquals(2, spillDir.listFiles().length);

    // Spill sweep should get rid of the sub-directory, since the test option defines sweep
    // threshold as 0
    schedulerService.taskToRun.run();
    assertEquals(0, spillDir.listFiles().length);

    assertNotEquals(null, schedulerService.taskToRun);

    spillService.close();
  }
}
