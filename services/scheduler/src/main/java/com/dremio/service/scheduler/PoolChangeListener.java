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
package com.dremio.service.scheduler;

/**
 * A listener who will be notified if a pool size change is requested by the user and hence the
 * caller/user of the {@code ClusteredSingletonTaskScheduler}.
 *
 * <p>On getting notified on this interface, the scheduler will re-adjust the pool size for
 * corresponding pool.
 */
public interface PoolChangeListener {
  /**
   * Callers of the scheduler library must invoke this method if they wish to change the pool sizes.
   *
   * @param currentSize current size of the pool. Must match the current size as understood by the
   *     scheduler
   * @param newSize new size of the pool
   */
  void changePoolSize(int currentSize, int newSize);
}
