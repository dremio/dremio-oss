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
package com.dremio.service.reflection;

import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.reflection.ReflectionManager.WakeUpCallback;
import com.google.common.base.Preconditions;

/**
 * {@link JobStatusListener} implementation that wakes up the
 * {@link ReflectionManager} when the job is done.
 */
public class WakeUpManagerWhenJobDone extends NoOpJobStatusListener {
  private final WakeUpCallback wakeUpCallback;
  private final String jobName;

  public WakeUpManagerWhenJobDone(WakeUpCallback wakeUpCallback, String jobName) {
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
    this.jobName = Preconditions.checkNotNull(jobName, "job name required");
  }

  @Override
  public void jobFailed(Exception e) {
    wakeUpCallback.wakeup(jobName + " failed");
  }

  @Override
  public void jobCompleted() {
    wakeUpCallback.wakeup(jobName + " completed");
  }

  @Override
  public void jobCancelled(String reason) {
    if (reason != null) {
      wakeUpCallback.wakeup(jobName + " cancelled for reason: " + reason);
      return;
    }
    wakeUpCallback.wakeup(jobName + " cancelled");
  }
}
