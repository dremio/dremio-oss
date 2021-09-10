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

import com.google.common.base.Throwables;

/**
 * {@link JobStatusListener} that can block until the job is done successfully or not.
 * If the job fails, calling {@link #await()} will throw an exception
 */
public class CompletionListener implements JobStatusListener {
  private final CountDownLatch latch = new CountDownLatch(1);

  private volatile Exception ex;
  private volatile boolean success = false;
  private final boolean throwEx;

  public CompletionListener(boolean throwEx) {
    this.throwEx = throwEx;
  }

  public CompletionListener() {
    this(true);
  }


  /**
   * @return true if the job completed successfully, false if failed or was cancelled
   */
  public boolean isCompleted() {
    return success;
  }

  /**
   * blocks until the job finishes successfully or not
   * @throws Exception if the job failed
   */
  public void await() throws Exception {
    latch.await();

    if (ex != null && throwEx) {
      throw ex;
    }
  }

  /**
   * blocks until the job finishes successfully or not
   * if job failed with a checked exception, throws the exception wrapped into RuntimeException
   */
  public void awaitUnchecked() {
    try {
      await();
    } catch (Exception e) {
      if (throwEx) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      } else {
        ex = e;
      }
    }
  }

  public void await(long timeout, TimeUnit unit) throws Exception {
    latch.await(timeout, unit);

    if (ex != null && throwEx) {
      throw ex;
    }
  }

  @Override
  public void jobFailed(Exception e) {
    ex = e;
    latch.countDown();
  }

  @Override
  public void submissionFailed(RuntimeException e) {
    ex = e;
    latch.countDown();
  }

  @Override
  public void jobCompleted() {
    success = true;
    latch.countDown();
  }

  @Override
  public void jobCancelled(String reason) {
    latch.countDown();
  }

  public Exception getException() {
    return ex;
  }
}
