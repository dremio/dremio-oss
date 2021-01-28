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
package com.dremio.exec.planner;

import org.apache.calcite.plan.RelOptListener;

/**
 * Listener to count number of matches in HepPlanner.
 * Also keeps a log of RelNode count and rules count in a phase
 */
public class MatchCountListener implements RelOptListener {
  private final long relNodeCount;
  private final long rulesCount;
  private final int matchLimit;

  private int matchCount = 0;

  public MatchCountListener(long relNodeCount, long rulesCount, int matchLimit) {
    this.relNodeCount = relNodeCount;
    this.rulesCount = rulesCount;
    this.matchLimit = matchLimit;
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    if (!event.isBefore()) {
      matchCount++;
    }
  }

  @Override
  public void relDiscarded(RelDiscardedEvent event) {
  }

  @Override
  public void relChosen(RelChosenEvent event) {
  }

  public int getMatchCount() {
    return matchCount;
  }

  public long getRelNodeCount() {
    return relNodeCount;
  }

  public long getRulesCount() {
    return rulesCount;
  }

  public int getMatchLimit() {
    return matchLimit;
  }
}
