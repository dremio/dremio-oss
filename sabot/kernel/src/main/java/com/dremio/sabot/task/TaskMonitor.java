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
package com.dremio.sabot.task;

/**
 * Implement this interface to inform observers about periodic task monitoring. The only events
 * supported for now are timer events that are invoked on a schedule
 */
public interface TaskMonitor {
  // Implement this method to add an observer
  default void addObserver(TaskMonitorObserver observer) {}

  // Implement this method to remove an observer
  default void removeObserver(TaskMonitorObserver observer) {}
}
