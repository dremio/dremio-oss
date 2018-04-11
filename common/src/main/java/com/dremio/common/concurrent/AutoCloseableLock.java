/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.common.concurrent;

import java.util.concurrent.locks.Lock;

import com.dremio.common.util.Closeable;

import java.util.concurrent.TimeUnit;

/**
 * Simple wrapper class that allows Locks to be released via an try-with-resources block.
 */
public class AutoCloseableLock implements Closeable {

  private final Lock lock;

  public AutoCloseableLock(Lock lock) {
    this.lock = lock;
  }

  public AutoCloseableLock open() {
    lock.lock();
    return this;
  }

  /**
   * Acquires the lock if it is free within the given waiting time and the
   * current thread has not been interrupted.
   *
   * @param time    Time to wait.
   * @param unit    Unit of time.
   * @return The AutoCloseableLock instance or null if the wait time expired.
   * @throws InterruptedException
   */
  public AutoCloseableLock tryOpen(long time, TimeUnit unit) throws InterruptedException {
    return lock.tryLock(time, unit) ? this : null;
  }

  @Override
  public void close() {
    lock.unlock();
  }

}
