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
package com.dremio.dac.cmd.upgrade;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * To sort UpgrateTasks based on dependencies - topological sort
 */
public final class UpgradeTaskDependencyResolver {

  private Map<String, UpgradeTask> uuidToTask = Maps.newHashMap();

  UpgradeTaskDependencyResolver(List<? extends UpgradeTask> fullList) {
    for (UpgradeTask task : fullList) {
      Preconditions.checkState(uuidToTask.putIfAbsent(task.getTaskUUID(), task) == null);
    }
  }

  /**
   * To sort data based on dependencies
   * @return topologically sorted tasks
   */
  public List<UpgradeTask> topologicalTasksSort() {
    List<String> resolved = Lists.newArrayList();

    for(UpgradeTask task : uuidToTask.values()) {
      visit(task, resolved, Lists.newArrayList());
    }

    List<UpgradeTask> resolvedTasks = Lists.newArrayList();
    for (String dep : resolved) {
      resolvedTasks.add(uuidToTask.get(dep));
    }
    return resolvedTasks;
  }

  private void visit(UpgradeTask task, List<String> resolved, List<String> unresolved) {
    if (resolved.contains(task.getTaskUUID())) {
      return;
    }
    if (unresolved.contains(task.getTaskUUID())) {
      throw new IllegalStateException(
        String.format("Dependencies loop detected: %s", task.getTaskName()));
    }
    unresolved.add(task.getTaskUUID());
    for (String dep : task.getDependencies()) {
      UpgradeTask depTask = uuidToTask.get(dep);
      if (depTask == null) {
        // dependency not defined in this classpath. skipping.
        continue;
      }
      visit(uuidToTask.get(dep), resolved, unresolved);
    }
    resolved.add(task.getTaskUUID());
    unresolved.remove(task.getTaskUUID());
  }
}
