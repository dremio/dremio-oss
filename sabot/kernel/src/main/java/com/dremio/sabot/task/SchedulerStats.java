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
 * Gets and clears collected stats on demand.
 *
 * <p>
 * Assumption: Currently Get stats as a printable/loggable string. In the future we can get it as a Json or
 * protobuf objects
 * </p>
 */
public interface SchedulerStats {
  /**
   * Does a fast dirty check to see if system is idle but has stats in the last cycle
   */
  boolean currentlyIdleAndHasStats();

  /**
   * Gets stats as a well formed loggable string.
   */
  String getStats(boolean force);

  /**
   * Clears stats.
   */
  void clearStats();
}
