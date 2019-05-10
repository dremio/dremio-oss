/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.service.Service;

/**
 * Command Thread pool interface. Allows limiting how many queries/job requests are handled in parallel at any given time.<br>
 * Submitted tasks implement the {@link Command} interface which is also {@link Comparable} and will be used to define the
 * priority of the tasks.<br>
 * Tasks are scheduled for execution according to the following rules:<br>
 * <ul>
 *   <li>tasks are ordered naturally using their priority + submission time</li>
 *   <li>first task will be picked up for execution as soon as a thread is idle</li>
 *   <li>busy threads will first finish what they have before picking another task to run</li>
 * </ul>
 */
public interface CommandPool extends Service {

  int WARN_DELAY_MS = 100; /** how long job/work submission can wait before we log a warning */
  /**
   * Task priority.
   */
  enum Priority {
    LOW,
    MEDIUM,
    HIGH
  }

  /**
   * Tasks submitted to the command pool need to implement this interface.
   */
  interface Command<T> {
    /**
     * Implements the command logic
     *
     * @param waitInMillis how much the command waited in the thread pool before this method was called
     */
    T get(long waitInMillis) throws Exception;
  }

  /**
   * Submit a {@link Command} to the thread pool
   *
   * @param priority command priority
   * @param descriptor command descriptor, mainly used for logging
   * @param command {@link Command} submitted to the thread pool
   */
  <V> CompletableFuture<V> submit(Priority priority, String descriptor, Command<V> command);
}
