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

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;

/**
 * Wraps a {@link CommandPool.Command} to make it both {@link Runnable} and {@link Comparable}
 */
public class CommandWrapper<T> implements Runnable, Comparable<CommandWrapper<T>> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommandWrapper.class);

  private final String descriptor;
  private final CommandPool.Priority priority;
  private final long submittedTime;
  private final CommandPool.Command<T> command;
  private final CompletableFuture<T> future = new CompletableFuture<>();

  CommandWrapper(CommandPool.Priority priority, String descriptor, long submittedTime, CommandPool.Command<T> command) {
    this.descriptor = descriptor;
    this.priority = priority;
    this.submittedTime = submittedTime;
    this.command = command;
  }

  String getDescriptor() {
    return descriptor;
  }

  CompletableFuture<T> getFuture() {
    return future;
  }

  @Override
  public void run() {
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(descriptor);

    try {
      final long waitInMillis = System.currentTimeMillis() - submittedTime;
      logger.debug("command {} started after waiting {} ms", descriptor, waitInMillis);


      final T result = command.get(waitInMillis);
      logger.debug("command {} completed successfully", descriptor);
      future.complete(result);
    } catch (Throwable t) {
      Throwable rootException = ErrorHelper.findWrappedCause(t, UserException.class);
      if (rootException != null && rootException.getMessage().equals(UserException.QUERY_REJECTED_MSG)) {
        // Ignore the exception and just show that the job was rejected if the query was rejected by alive queries limit
        logger.error("command {} was rejected because it exceeded the maximum allowed number of live queries in a single coordinator", descriptor);
      } else {
        logger.error("command {} failed", descriptor, t);
      }
      future.completeExceptionally(t);
    } finally {
      // restore the thread's original name
      currentThread.setName(originalName);
    }
  }

  @Override
  public int compareTo(CommandWrapper<T> o) {
    // commands are ordered by priority, submittedTime is used to break ties
    if (o.priority == priority) {
      return Long.compare(submittedTime, o.submittedTime);
    }
    return Comparator.<CommandPool.Priority>reverseOrder().compare(priority, o.priority);
  }
}
