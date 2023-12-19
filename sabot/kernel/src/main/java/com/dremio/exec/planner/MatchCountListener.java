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
  // how much total time is taken by each rule. This will include total time when
  // the rule completes transformation (attempted and matched calculated separately).
  // Rule name is used as key to all the maps below. We also keep track of the thread ID
  // which will distinguish different queries dumping logs at the same time.
  private final Map<String, Integer> ruleNameToMatchCount = new HashMap<>();
  private final Map<String, Integer> ruleNameToTransformCount = new HashMap<>();
  private final Map<String, Long> ruleMatchTime = new HashMap<>(); // Time spent in the onMatch function
  private final Map<String, Long> ruleMatchToTransformTime = new HashMap<>(); // Time spent from after onMatch (if the rule was successful in transforming)
  private final long relNodeCount;
  private final long rulesCount;
  private final int matchLimit;
  private final boolean verbose;
  private final String threadName;

  private long matchCount = 0; // How many times a rule is matched
  private int transformCount = 0; // How many times a rule successfully transformed the rel node

  private Stopwatch currentRuleStopwatch = Stopwatch.createUnstarted();

  public MatchCountListener(int matchLimit, boolean verbose, String threadName) {
    this(0, 0, matchLimit, verbose, threadName);
  }

  public MatchCountListener(long relNodeCount, long rulesCount, int matchLimit, boolean verbose, String threadName) {
    this.relNodeCount = relNodeCount;
    this.rulesCount = rulesCount;
    this.matchLimit = matchLimit;
    this.verbose = verbose;
    this.threadName = threadName;
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
    try {
      final String currentRule = event.getRuleCall().getRule().toString();
      if (event.isBefore()) {
        if (verbose) {
          ruleNameToMatchCount.put(currentRule, ruleNameToMatchCount.getOrDefault(currentRule, 0) + 1);
        }
        currentRuleStopwatch = Stopwatch.createStarted(); // Start the stopwatch
        matchCount++;
      } else {
        ruleMatchTime.put(currentRule, ruleMatchTime.getOrDefault(currentRule, 0L) + currentRuleStopwatch.elapsed(TimeUnit.MILLISECONDS));
        currentRuleStopwatch.reset(); // Stop the stopwatch

        // Start the stopwatch again to measure how much time is spent from the time this rule finished onMatch
        // to the time when we register the transformed results.
        currentRuleStopwatch = Stopwatch.createStarted();
      }
    } catch (Exception ex) {
      // This listener is for dumping useful stats purpose. It should not hinder planning. In case of
      // any exception, just log it.
      logger.debug("Exception in ruleAttempted method: ", ex);
    }
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    try {
      final String currentRule = event.getRuleCall().getRule().toString();
      if (!event.isBefore()) {
        if (verbose) {
          ruleNameToTransformCount.put(currentRule, ruleNameToTransformCount.getOrDefault(currentRule, 0) + 1);
        }
        ruleMatchToTransformTime.put(currentRule, ruleMatchToTransformTime.getOrDefault(currentRule, 0L) + currentRuleStopwatch.elapsed(TimeUnit.MILLISECONDS));
        currentRuleStopwatch.reset(); // Stop the stopwatch for next rule
        transformCount++;
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

  public int getTransformCount() {
    return transformCount;
  }

  public long getRelNodeCount() {
    return relNodeCount;
  }

  public long getRulesCount() {
    return rulesCount;
  }

  public long getMatchCount() {
    return matchCount;
  }

  public int getMatchLimit() {
    return matchLimit;
  }

  public Map<String, Long> getRuleToTotalTime() {
    final Map<String, Long> ruleToTotalTime = new HashMap<>();
    try {
      for (String key : ruleNameToMatchCount.keySet()) {
        long time = ruleMatchTime.getOrDefault(key, 0L) +
          ruleMatchToTransformTime.getOrDefault(key, 0L);
        ruleToTotalTime.put(key, time);
      }
    } catch (Exception ex) {
      // This listener is for dumping useful stats purpose. It should not hinder planning. In case of
      // any exception, just log it.
      logger.debug("Exception in getRuleToTotalTime method: ", ex);
    }

    return ruleToTotalTime;
  }

  public void reset() {
    matchCount = 0;
    transformCount = 0;
    currentRuleStopwatch.reset();
    ruleMatchTime.clear();
    ruleMatchToTransformTime.clear();
    if (verbose) {
      ruleNameToMatchCount.clear();
      ruleNameToTransformCount.clear();
    }
  }

  @Override
  public String toString() {
    try {
      final Map<String, Long> ruleToTotalTime = getRuleToTotalTime();
      final StringBuilder sb = new StringBuilder();
      sb.append("Thread name: ").append(threadName)
        .append("\nRelNodes count: ").append(getRelNodeCount())
        .append("\nRules count: ").append(getRulesCount())
        .append("\nMatch limit: ").append(getMatchLimit())
        .append("\nMatch count: ").append(getMatchCount())
        .append("\nTransform count: ").append(getTransformCount()).append("\n");
      for (String key : ruleNameToMatchCount.keySet()) {
        sb.append("Rule: ").append(key)
          .append("\t\tTotal time spent: ").append(ruleToTotalTime.getOrDefault(key, 0L)).append(" ms");
        if (verbose) {
          sb.append("\t\tMatched times: ").append(ruleNameToMatchCount.getOrDefault(key, 0))
            .append("\t\tTransformed times: ").append(ruleNameToTransformCount.getOrDefault(key, 0));
        }
        sb.append("\n");
      }
      return sb.toString();
    } catch (Exception ex) {
      // This listener is for dumping useful stats purpose. It should not hinder planning. In case of
      // any exception, just log it.
      logger.debug("Exception in toString method: ", ex);
      return "";
    }
  }
}
