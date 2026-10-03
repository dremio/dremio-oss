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
package com.dremio.plugins.icebergcatalog.dfs;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileSystem;

/** Wraps a Hadoop FS and associates a reference counter to it */
class LockableHadoopFileSystem {
  private final FileSystem fs;
  private final AtomicInteger refCount = new AtomicInteger(0);
  private final Object monitor = new Object();

  /**
   * Wall clock time (epoch millis) after which the FS must not be used any more, e.g. because the
   * temporary credentials it was created with expire. {@link Long#MAX_VALUE} if unbounded.
   */
  private final long expiresAtMillis;

  LockableHadoopFileSystem(FileSystem fs) {
    this(fs, Long.MAX_VALUE);
  }

  LockableHadoopFileSystem(FileSystem fs, long expiresAtMillis) {
    this.fs = fs;
    this.expiresAtMillis = expiresAtMillis;
  }

  long getExpiresAtMillis() {
    return expiresAtMillis;
  }

  @VisibleForTesting
  int getRefCount() {
    return refCount.get();
  }

  public FileSystem getFs() {
    return fs;
  }

  public void lockForUse() {
    refCount.incrementAndGet();
  }

  public void unlock() {
    int newCount = refCount.decrementAndGet();
    if (newCount < 0) {
      // Undo the decrement so an unbalanced unlock() cannot leave the count stuck below zero.
      refCount.incrementAndGet();
      throw new IllegalStateException("Negative reference count on FS " + this);
    }
    if (newCount == 0) {
      synchronized (monitor) {
        monitor.notifyAll(); // Notify waiters when count hits 0
      }
    }
  }

  /**
   * Makes current thread wait for reference count to reach 0 or timeout.
   *
   * @param timeoutMillis timeout in ms
   * @return true if timeout happened, false if properly dereferenced beforehand
   * @throws InterruptedException
   */
  public boolean waitForClose(long timeoutMillis) throws InterruptedException {
    long timeoutAt = System.currentTimeMillis() + timeoutMillis;
    synchronized (monitor) {
      while (refCount.get() != 0) {
        long remaining = timeoutAt - System.currentTimeMillis();
        if (remaining <= 0) {
          return true;
        }
        monitor.wait(remaining);
      }
      return false;
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(fs);
    sb.append("(refCnt=").append(refCount.get()).append(')');
    return sb.toString();
  }
}
