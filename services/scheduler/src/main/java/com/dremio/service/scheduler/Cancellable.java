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
package com.dremio.service.scheduler;

/**
 * A {@code Cancellable} is an object whose execution is asynchronous
 * and/or recurrent, and which can be cancelled.
 *
 * The cancel method is called to stop pending/future execution. It
 * does not guarantee for current execution to be halted.
 */
public interface Cancellable {
  /**
   * Cancel the operation.
   *
   * @param mayInterruptIfRunning if true, might interrupt the thread if the operation is
   * currently running (to use with caution).
   */
  void cancel(boolean mayInterruptIfRunning);

  boolean isCancelled();
}
