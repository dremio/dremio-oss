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
package com.dremio.common.concurrent;

import com.dremio.common.util.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * Simple wrapper class that allows Locks to be released via an try-with-resources block. Also
 * ensures idempotence.
 */
public class AutoCloseableLock implements Closeable {

  private final boolean singleUse;
  private final AtomicBoolean closed = new AtomicBoolean(true);
  private final Lock lock;

  public AutoCloseableLock(Lock lock) {
    this(lock, false);
  }

  public AutoCloseableLock(Lock lock, boolean singleUse) {
    this.lock = lock;
    this.singleUse = singleUse;
  }

  public AutoCloseableLock open() {
    startOpen();
    lock.lock();
    return this;
  }

  private void startOpen() {
    if (singleUse && !closed.compareAndSet(true, false)) {
      throw new IllegalStateException(
          "Trying to open an already opened lock. AutoCloseableLock allows only one lock count to be held at a time.");
    }
  }

  private boolean shouldClose() {
    return !singleUse || closed.compareAndSet(false, true);
  }

  /**
   * Acquires the lock if it is free within the given waiting time and the current thread has not
   * been interrupted.
   *
   * @param time Time to wait.
   * @param unit Unit of time.
   * @return optional AutoCloseableLock
   * @throws InterruptedException
   */
  public Optional<AutoCloseableLock> tryOpen(long time, TimeUnit unit) throws InterruptedException {
    startOpen();
    if (lock.tryLock(time, unit)) {
      return Optional.of(this);
    } else {
      shouldClose();
      return Optional.empty();
    }
  }

  /** Close the lock. Will not close the lock if it has already been closed. */
  @Override
  public void close() {
    if (shouldClose()) {
      lock.unlock();
    }
  }

  public static AutoCloseableLock of(Lock lock, boolean singleUse) {
    return new AutoCloseableLock(lock, singleUse);
  }

  public static AutoCloseableLock ofAlreadyOpen(Lock lock, boolean singleUse) {
    AutoCloseableLock acl = new AutoCloseableLock(lock, singleUse);
    acl.startOpen();
    return acl;
  }

  public static AutoCloseableLock lockAndWrap(Lock lock, boolean singleUse) {
    lock.lock();
    return ofAlreadyOpen(lock, singleUse);
  }
}
