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

import com.dremio.common.exceptions.UserException;

/**
 * Monitors planners and limits the amount of nodes generated.
 */
class MaxNodesListener implements RelOptListener {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaxNodesListener.class);

  private final long maxEquivalence;
  private long foundNodes = 0;

  public MaxNodesListener(long maxEquivalence) {
    super();
    this.maxEquivalence = maxEquivalence;
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
    foundNodes++;
    if(foundNodes > maxEquivalence) {
      throw UserException.resourceError()
      .message("Job was cancelled because the query went beyond system capacity during query planning. Please simplify the query.")
      .build(logger);
    }
  }

  public void reset() {
    foundNodes = 0;
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
  }

  @Override
  public void relDiscarded(RelDiscardedEvent event) {
    // discards happen in hep planner.
    foundNodes--;
  }

  @Override
  public void relChosen(RelChosenEvent event) {
  }

}
