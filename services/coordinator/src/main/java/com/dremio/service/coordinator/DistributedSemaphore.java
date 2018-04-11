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
package com.dremio.service.coordinator;

import java.util.concurrent.TimeUnit;

/**
 * A distributed semaphore interface
 */
public interface DistributedSemaphore {
  /**
   * Try to acquire the semaphore
   *
   * @param time the duration to wait for the semaphore
   * @param unit the duration unit
   * @return the lease
   * @throws Exception
   */
  public DistributedLease acquire(long time, TimeUnit unit) throws Exception;

  /**
   * The semaphore lease
   */
  public interface DistributedLease extends AutoCloseable{}
}
