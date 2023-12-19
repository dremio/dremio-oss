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
package com.dremio.common.perf;

/*
  A global, thread-safe registrar keeping a book of all threads which are eligible for stats collection.
 */
public class StatsCollectionEligibilityRegistrar {
  private static final ThreadLocal<Boolean> eligibleThreads = new ThreadLocal<>();

  /**
   * Adds the current thread to the registry of threads eligible for stats collection
   */
  public static void addSelf() {
    eligibleThreads.set(true);
  }

  /**
   * Checks if the current thread is eligible for stats collection by checking the {@code ThreadLocal} object.
   *
   * @return true if the current thread is eligible for stats collection, false otherwise
   */
  public static boolean isEligible() {
    return Boolean.TRUE.equals(eligibleThreads.get());
  }
}
