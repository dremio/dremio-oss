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
package com.dremio.service.scheduler;

/**
 * Specifies the configuration of a group to which a given task and its schedule are associated with.
 * The group determines common resources such as thread pool used to execute a given task. This allows different
 * types of task to be executed under different thread pool(s), allowing for better control of JVM resource usage
 * by these tasks.
 * <p>
 * If a group is not specified for a task, the default group is used for the task. All groups other than the
 * 'default' group is modifiable, which means an external system outside the scheduler can vary properties such as the
 * capacity. The 'default' group must be provided while constructing the scheduler service, along with a
 * 'root path' that is common across service instances of a given service, but must differ across different services.
 * </p>
 */
public interface ScheduleTaskGroup {
  String getGroupName();

  int getCapacity();

  static ScheduleTaskGroup create(String name, int capacity) {
    return new ScheduleTaskGroup() {
      @Override
      public String getGroupName() {
        return name;
      }

      @Override
      public int getCapacity() {
        return capacity;
      }
    };
  }
}
