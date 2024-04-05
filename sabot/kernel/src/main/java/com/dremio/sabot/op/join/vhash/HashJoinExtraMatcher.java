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
package com.dremio.sabot.op.join.vhash;

/**
 * Matcher interface to evaluate the extra inequality expression that follows as conjuncts of the
 * equality expressions of an equijoin.
 */
public interface HashJoinExtraMatcher extends ExtraConditionStats {
  int SHIFT_SIZE = 16;
  int BATCH_OFFSET_SIZE = 2;
  int BATCH_INDEX_SIZE = 4;

  /** Sets up the extra filter matcher. */
  void setup();

  /**
   * Checks if there is a current match for the given probe index against the build record
   * represented by {@code buildBatch} and {@code buildOffset}.
   *
   * @param currentProbeIndex Index into record within the current probe batch
   * @return true if the probe record matched the build record, false otherwise
   */
  boolean checkCurrentMatch(int currentProbeIndex, int currentLinkBatch, int currentLinkOffset);
}
