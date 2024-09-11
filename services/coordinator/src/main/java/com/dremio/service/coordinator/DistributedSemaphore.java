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

import java.util.concurrent.TimeUnit;

/** A distributed semaphore interface */
public interface DistributedSemaphore {
  /**
   * Try to acquire the semaphore
   *
   * @param time the duration to wait for the semaphore
   * @param unit the duration unit
   * @return the lease
   */
  default DistributedLease acquire(long time, TimeUnit unit) throws Exception {
    return acquire(1, time, unit);
  }

  /**
   * Try to acquire multiple permits in the semaphore
   *
   * @param numPermits the number of permits to acquire, must be a positive integer
   * @param time the duration to wait for the semaphore
   * @param unit the duration unit
   * @return the lease
   */
  DistributedLease acquire(int numPermits, long time, TimeUnit unit) throws Exception;

  /**
   * Determine the number of currently outstanding permits.
   *
   * @return number of permits
   */
  boolean hasOutstandingPermits();

  /**
   * Register a listener that is updated every time this semaphore changes.
   *
   * <p>This is a weak registration. If the requester no longer exists, the semaphore won't a
   * reference to the listener.
   *
   * <p>return true if successfully registered listener
   */
  boolean registerUpdateListener(UpdateListener listener);

  /** Listener for when a semaphore has changed state. */
  interface UpdateListener {

    /** Informed when the semaphore has changed (increased or decreased). */
    void updated();
  }

  /** The semaphore lease */
  interface DistributedLease extends AutoCloseable {}
}
