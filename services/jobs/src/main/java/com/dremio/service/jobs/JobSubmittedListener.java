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
package com.dremio.service.jobs;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link JobStatusListener} that allows the caller to block until the job has been submitted.
 */
public class JobSubmittedListener implements JobStatusListener {

  private final CountDownLatch latch = new CountDownLatch(1);
  private RuntimeException exception;

  @Override
  public void jobSubmitted() {
    latch.countDown();
  }

  @Override
  public void submissionFailed(RuntimeException e) {
    exception = e;
    latch.countDown();
  }

  /**
   * blocks the thread until the job has been submitted or the timeout is exceeded
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   *
   * @return true if job has been submitted before the timeout, false otherwise
   */
  public boolean await(long timeout, TimeUnit unit) {
    boolean submitted;
    try {
      submitted = latch.await(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Caller interrupted while waiting for job submission");
    }

    if (exception != null) {
      throw exception;
    }

    return submitted;
  }

  /**
   * blocks the thread until the job has been submitted.
   */
  public void await() {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Caller interrupted while waiting for job submission");
    }

    if (exception != null) {
      throw exception;
    }
  }
}
