/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.task.single;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dremio.config.DremioConfig;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.TaskPool;
import com.dremio.sabot.task.TaskPoolFactory;

/**
 * A task pool that dedicates one thread to each task and relies on OS context switching.
 */
public class DedicatedTaskPool implements TaskPool {

  /**
   * Factory for {@code DedicatedTaskPool}
   */
  public static final class Factory implements TaskPoolFactory {
    @Override
    public TaskPool newInstance(OptionManager options, DremioConfig config) {
      return new DedicatedTaskPool();
    }
  }

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Override
  public void execute(AsyncTaskWrapper task) {
    final DedicatedFragmentRunnable runnable = new DedicatedFragmentRunnable(task);
    executorService.submit(runnable);

    task.setTaskHandle(runnable.toTaskHandle());
  }

  @Override
  public void close() throws Exception {
    executorService.shutdownNow();
  }


}
