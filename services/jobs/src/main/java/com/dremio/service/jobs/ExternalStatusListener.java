/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.jobs;

/**
 * Informs an interested party about job events.
 */
public interface ExternalStatusListener {
  /**
   * Called one or more times when a profile is updated (prior to job completion).
   * @param profile The updated job.
   */
  void profileUpdated(Job job);

  /**
   * Called when job is completed. Provides final job object.
   * @param result
   * @param profile
   */
  void queryCompleted(Job job);
}
