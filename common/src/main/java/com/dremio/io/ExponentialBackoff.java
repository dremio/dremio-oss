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
package com.dremio.io;

/**
 * This interface implements exponential backoff
 */
public interface ExponentialBackoff {
  /**
   * Perform a sleep based on the attempt number, where the sleep period increases exponentially and adds jitter.
   *
   * @param attemptNumber The retry attempt number, which directly affects the sleep period.
   */
  default void backoffWait(int attemptNumber) {
    try {
      // Algorithm taken from https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
      // sleep = min(cap, base * 2 ^ attemptNumber)
      final double sleep = Math.min(getMaxMillis(), getBaseMillis() * (1 << attemptNumber));
      Thread.sleep((long) (Math.random() * (sleep + 1)));
    } catch (InterruptedException e) {
      // Preserve interrupt status.
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Return the base millis to sleep as part of exponential backoff.
   *
   * @return base millis to wait in exponential backoff
   */
  int getBaseMillis();

  /**
   * Return the max millis to sleep as part of exponential backoff.
   *
   * @return max millis to wait in exponential backoff
   */
  int getMaxMillis();
}
