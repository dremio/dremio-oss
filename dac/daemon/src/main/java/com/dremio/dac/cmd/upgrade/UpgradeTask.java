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
package com.dremio.dac.cmd.upgrade;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base implementation for all upgrade tasks
 *
 * For upgrade tasks to be picked up, make sure that the package is searched for
 * by {@code com.dremio.common.scanner.ClassPathScanner}
 *
 * Also, use {@code TestUpgrade} to confirm that the task is detected and order
 * is valid.
 */
public abstract class UpgradeTask {

  private final String taskName;
  private final String description;
  private final List<String> dependencies = Lists.newArrayList(); // list of UUID of the tasks this one depends on

  protected UpgradeTask(String description, List<String> dependencies) {
    this.description = Preconditions.checkNotNull(description);
    this.taskName = getClass().getSimpleName();
    this.dependencies.addAll(dependencies);
  }

  public String getDescription() {
    return description;
  }

  /**
   * Dependencies are expressed as UUIDs and not tasks
   * as some tasks may not be visible because of module
   * dependencies
   * @return list of UUIDs this task depends on
   */
  public List<String> getDependencies() {
    return dependencies;
  }

  String getTaskName() {
    return taskName;
  }

  /**
   * Gets the upgrade task UUID.
   *
   * @return the UUID from the current task
   */
  public abstract String getTaskUUID();

  /**
   * Executes an upgrade task.
   *
   * @param context an instance that contains the stores required by the current upgrade task
   * @throws Exception If any exception or errors occurs.
   */
  public abstract void upgrade(UpgradeContext context) throws Exception;

  /**
   * Gets a string representation of the current task description.
   *
   * @return the task description represented by a string format
   */
  @Override
  public String toString() {
    return String.format("'%s'", description);
  }
}
