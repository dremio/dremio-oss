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
package com.dremio.sabot.task;

import java.util.concurrent.atomic.AtomicBoolean;

import com.dremio.sabot.task.TaskManager.TaskHandle;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.google.common.base.Preconditions;

/**
 * A callback wrapper around {@code TaskHandle} to be called after task is unblocked
 */
public class BlockRun implements AvailabilityCallback {

  private final TaskHandle<AsyncTaskWrapper> handle;
  private final AtomicBoolean executed = new AtomicBoolean(false);

  public BlockRun(TaskHandle<AsyncTaskWrapper> handle) {
    super();
    Preconditions.checkNotNull(handle);
    this.handle = handle;
  }

  @Override
  public void nowAvailable() {
    Preconditions.checkArgument(executed.compareAndSet(false, true), "Now available executed multiple times.");
    handle.reEnqueue();
  }

}