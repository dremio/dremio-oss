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
 * An extension of the scheduler service that allows addition and deletion of task groups and optionally
 * allows these task groups to be associated with a task and its schedule.
 */
public interface ModifiableSchedulerService extends SchedulerService {
  /**
   * Adds a task group which can then be used to tie a created task and its schedule.
   *
   * @param taskGroup details of the task group
   */
  void addTaskGroup(ScheduleTaskGroup taskGroup);

  /**
   * Modify a task group, given its name.
   * <p>
   * <strong>NOTE:</strong> As of now throws a runtime exception if a group is not found.
   * </p>
   *
   * @param groupName name of the group
   * @param taskGroup The modified group
   */
  void modifyTaskGroup(String groupName, ScheduleTaskGroup taskGroup);
}
