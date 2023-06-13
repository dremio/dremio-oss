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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.RelOptListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

/**
 * Listener to count number of matches in HepPlanner.
 * Also keeps a log of RelNode count and rules count in a phase
 */
public class MatchCountListener implements RelOptListener {
  private static final Logger logger = LoggerFactory.getLogger(MatchCountListener.class);

  // Breakdown of how many times each rule has attempted and transformed a rel node and
  // how much total time is taken by each rule. This will only include total time if
  // the rule completes transformation (attempted and matched). Rule name is used as
  // key to all the maps below.
  private final Map<String, Integer> ruleToAttemptCount = new HashMap<>();
  private final Map<String, Integer> ruleToMatchCount = new HashMap<>();
  private final Map<String, Long> ruleToTotalTime = new HashMap<>();
  private final long relNodeCount;
  private final long rulesCount;
  private final int matchLimit;
  private final boolean verbose;

  private long attemptCount = 0; // How many times we tried to fire the rules
  private int matchCount = 0; // How many times the rules were successfully matched and transformed the rel node

  private String currentRule;
  private Stopwatch currentRuleStopwatch = Stopwatch.createUnstarted();

  public MatchCountListener(int matchLimit, boolean verbose) {
    this(0, 0, matchLimit, verbose);
  }

  public MatchCountListener(long relNodeCount, long rulesCount, int matchLimit, boolean verbose) {
    this.relNodeCount = relNodeCount;
    this.rulesCount = rulesCount;
    this.matchLimit = matchLimit;
    this.verbose = verbose;
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
    try {
      if (verbose) {
        currentRule = event.getRuleCall().getRule().toString();
        ruleToAttemptCount.put(currentRule, ruleToAttemptCount.getOrDefault(currentRule, 0) + 1);
        currentRuleStopwatch = Stopwatch.createStarted(); // Start the stopwatch
      }

      attemptCount++;
    } catch (Exception ex) {
      // This listener is for dumping useful stats purpose. It should not hinder planning. In case of
      // any exception, just log it.
      logger.debug("Exception in ruleAttempted method: ", ex);
    }
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    try {
      if (!event.isBefore()) {
        if (verbose) {
          ruleToMatchCount.put(currentRule, ruleToMatchCount.getOrDefault(currentRule, 0) + 1);
          ruleToTotalTime.put(currentRule, ruleToTotalTime.getOrDefault(currentRule, 0L) + currentRuleStopwatch.elapsed(TimeUnit.MILLISECONDS));
          currentRuleStopwatch.reset(); // Stop the stopwatch for next rule
        }
        matchCount++;
      }
    } catch (Exception ex) {
      // This listener is for dumping useful stats purpose. It should not hinder planning. In case of
      // any exception, just log it.
      logger.debug("Exception in ruleProductionSucceeded method: ", ex);
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

  public long getAttemptCount() {
    return attemptCount;
  }

  public int getMatchLimit() {
    return matchLimit;
  }

  public Map<String, Long> getRuleToTotalTime() {
    return ruleToTotalTime;
  }

  public void reset() {
    attemptCount = 0;
    matchCount = 0;

    if (verbose) {
      currentRule = null;
      currentRuleStopwatch.reset();
      ruleToAttemptCount.clear();
      ruleToTotalTime.clear();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RelNodes count: ").append(getRelNodeCount())
      .append("\nRules count: ").append(getRulesCount())
      .append("\nMatch limit: ").append(getMatchLimit())
      .append("\nAttempt count: ").append(getAttemptCount())
      .append("\nMatch count: ").append(getMatchCount()).append("\n\n");

    if (verbose) {
      for (String key : ruleToAttemptCount.keySet()) {
        sb.append("Rule: ").append(key)
          .append("\t\tAttempted times: ").append(ruleToAttemptCount.getOrDefault(key, 0))
          .append("\t\tMatched times: ").append(ruleToMatchCount.getOrDefault(key, 0))
          .append("\t\tTotal time spent: ").append(ruleToTotalTime.getOrDefault(key, 0L)).append("ms\n");
      }
    }
    sb.append("\n");
    return sb.toString();
  }
}
