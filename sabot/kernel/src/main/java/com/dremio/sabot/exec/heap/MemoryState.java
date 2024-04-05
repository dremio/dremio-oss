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
package com.dremio.sabot.exec.heap;

public enum MemoryState {
  INACTIVE(-1, -1, -1, 0, 0),
  NORMAL(60000, -1, -1, 0, 0),
  BORDER(3000, 200000, 25000, 70, 1),
  LOW(1000, 25000, 500, 80, 2),
  VERY_LOW(500, 1000, 10, 100, 3);

  private final int waitTimeMillis;
  private final int maxOverheadFactor;
  private final int individualOverhead;
  private final int maxVictimsPercentage;
  private final int severity;

  MemoryState(
      int waitTimeMillis,
      int maxOverheadFactor,
      int individualOverhead,
      int maxVictimsPercentage,
      int severity) {
    this.waitTimeMillis = waitTimeMillis;
    this.maxOverheadFactor = maxOverheadFactor;
    this.individualOverhead = (individualOverhead >= 0) ? individualOverhead : Integer.MAX_VALUE;
    this.maxVictimsPercentage = maxVictimsPercentage;
    this.severity = severity;
  }

  int getWaitTimeMillis() {
    return waitTimeMillis;
  }

  long getTotalOverheadFactor(int sizeFactor) {
    return (maxOverheadFactor >= 0) ? (long) maxOverheadFactor * (long) sizeFactor : Long.MAX_VALUE;
  }

  int getMaxVictims(int total) {
    return (maxVictimsPercentage * total) / 100;
  }

  int getIndividualOverhead() {
    return individualOverhead;
  }

  int getSeverity() {
    return severity;
  }
}
