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
package com.dremio.service.coordinator;

/** To listen on the events in TaskLeadershipService and react to it E.g. in SchedulerService */
public interface TaskLeaderChangeListener {

  /** In SchedulerService it would mean to start scheduling */
  void onLeadershipGained();

  /**
   * in SchedulerService it would mean to stop scheduling and if task is running cancel this task
   */
  void onLeadershipLost();

  /**
   * in SchedulerService it would mean to let current task to complete and then allow relinquishing
   * to proceed This is blocking call
   */
  void onLeadershipRelinquished();
}
