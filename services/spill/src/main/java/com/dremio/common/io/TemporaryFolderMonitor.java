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
package com.dremio.common.io;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors periodically and cleans up after dead executors.
 *
 * <p>This is enabled only if periodic monitoring is turned on (DCS excluded).
 */
final class TemporaryFolderMonitor {
  private static final Logger logger = LoggerFactory.getLogger(TemporaryFolderMonitor.class);

  private final Supplier<ExecutorId> thisExecutor;
  private final Supplier<Set<ExecutorId>> availableExecutors;
  private final Set<Path> managedRootPaths;
  private final String purpose;
  private final Map<ExecutorId, HealthTracker> healthMap;
  private final Deque<HealthTracker> expiryChecker;
  private final FileSystemHelper fsWrapper;
  private final int stalenessLimitSeconds;
  private final int minUnhealthyCyclesBeforeDelete;

  private int currentCycle = 0;

  TemporaryFolderMonitor(
      Supplier<ExecutorId> thisExecutor,
      Supplier<Set<ExecutorId>> availableExecutors,
      String purpose,
      int stalenessLimitSeconds,
      int unhealthyCyclesBeforeDelete,
      FileSystemHelper fsWrapper) {
    this.thisExecutor = thisExecutor;
    this.availableExecutors = availableExecutors;
    this.purpose = purpose;
    this.managedRootPaths = ConcurrentHashMap.newKeySet();

    // used only by a single scheduled thread
    this.healthMap = new HashMap<>();
    this.expiryChecker = new ArrayDeque<>();
    this.fsWrapper = fsWrapper;
    this.stalenessLimitSeconds = stalenessLimitSeconds;
    this.minUnhealthyCyclesBeforeDelete = unhealthyCyclesBeforeDelete;
  }

  void startMonitoring(Path rootPath) {
    managedRootPaths.add(rootPath);
  }

  void doCleanupOther() {
    if (managedRootPaths.isEmpty()) {
      return;
    }
    final ExecutorId myId = thisExecutor.get();
    currentCycle++;

    final Set<ExecutorId> otherExecutors = getExecutorsHavingFolders(myId);
    final Set<ExecutorId> aliveExecutors = availableExecutors.get();
    otherExecutors.removeAll(aliveExecutors);
    for (final ExecutorId executor : otherExecutors) {
      logger.debug("this executor = {}, other executor = {}", myId, executor);
      healthMap.compute(
          executor,
          (k, v) -> {
            if (v == null) {
              final HealthTracker tracker =
                  new HealthTracker(
                      executor, currentCycle, currentCycle + minUnhealthyCyclesBeforeDelete);
              expiryChecker.addLast(tracker);
              return tracker;
            } else {
              v.incrementCycle();
              return v;
            }
          });
    }

    // process the elements whose expiry cycle has crossed the current cycle
    while (!expiryChecker.isEmpty() && expiryChecker.peek().endCycle <= currentCycle) {
      final HealthTracker next = expiryChecker.poll();
      healthMap.remove(next.getExecutorId());
      if (next.isEligibleForDelete()) {
        // we can safely check this executor directory for deletion now
        for (final Path rootPath : managedRootPaths) {
          fsWrapper.safeCleanDirectory(
              rootPath, next.getExecutorId().toPrefix(purpose), stalenessLimitSeconds);
        }
      }
    }
  }

  private Set<ExecutorId> getExecutorsHavingFolders(ExecutorId myId) {
    final Set<ExecutorId> discoveredExecutors = new HashSet<>();
    for (final Path rootPath : managedRootPaths) {
      fsWrapper.visitNonEmptyDirectory(
          rootPath,
          fileStatus -> {
            final ExecutorId id = extractExecutorId(fileStatus);
            if (id != null && !id.equals(myId)) {
              discoveredExecutors.add(id);
            }
          });
    }
    return discoveredExecutors;
  }

  private ExecutorId extractExecutorId(FileStatus fileStatus) {
    if (!fileStatus.isDirectory()) {
      return null;
    }
    final String[] executorId = fileStatus.getPath().getName().split(ExecutorId.PREFIX_DELIMITER);
    if (executorId.length != 3
        || !executorId[0].equals(purpose)
        || executorId[1].isEmpty()
        || executorId[2].isEmpty()) {
      return null;
    }
    try {
      return new ExecutorId(executorId[1], Integer.parseInt(executorId[2]));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static final class HealthTracker {
    private final int endCycle;
    private final ExecutorId executorId;
    private int currentCycle;

    private HealthTracker(ExecutorId executorId, int startCycle, int endCycle) {
      this.currentCycle = startCycle;
      this.endCycle = endCycle;
      this.executorId = executorId;
    }

    boolean isEligibleForDelete() {
      return this.currentCycle >= this.endCycle;
    }

    void incrementCycle() {
      currentCycle++;
    }

    ExecutorId getExecutorId() {
      return executorId;
    }
  }
}
