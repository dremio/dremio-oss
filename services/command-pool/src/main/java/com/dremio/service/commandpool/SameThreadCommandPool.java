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
package com.dremio.service.commandpool;

import java.util.concurrent.CompletableFuture;

/**
 * Same thread implementation of {@link CommandPool}.<br>
 * Each task is run immediately in the submitting thread.<br>
 * Priority is irrelevant
 */
class SameThreadCommandPool implements CommandPool {

  @Override
  public <V> CompletableFuture<V> submit(Priority priority, String descriptor, Command<V> command, boolean runInSameThread) {
    CompletableFuture<V> future = new CompletableFuture<>();
    try {
      future.complete(command.get(0));
    } catch (Exception ex) {
      future.completeExceptionally(ex);
    }
    return future;
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }
}
