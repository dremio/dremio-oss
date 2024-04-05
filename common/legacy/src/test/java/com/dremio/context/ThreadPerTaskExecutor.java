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
package com.dremio.context;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/** Test helper for running tasks in separate threads. */
public class ThreadPerTaskExecutor implements Executor {
  private static final ThreadPerTaskExecutor INSTANCE = new ThreadPerTaskExecutor();

  @Override
  public void execute(Runnable command) {
    new Thread(command).start();
  }

  /** Runs the command in a separate thread */
  public static Future<Void> run(Runnable runnable) {
    final FutureTask<Void> task = new FutureTask<>(runnable, null);
    INSTANCE.execute(task);
    return task;
  }
}
