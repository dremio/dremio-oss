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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Stopwatch;

/**
 * Implements HashJoinExtraMatcher using Java code to build and evaluate the expression.
 */
public class JoinExtraConditionMatcher implements HashJoinExtraMatcher {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinExtraConditionMatcher.class);

  private final LogicalExpression extraCondition;
  private final OperatorContext context;
  private final VectorAccessible probeBatch;
  private final VectorAccessible buildBatch;
  private final Map<String, String> build2ProbeKeys;

  private long totalEvaluations;
  private long totalMatches;
  private final Stopwatch setupWatch = Stopwatch.createUnstarted();

  private ExtraConditionEvaluator matchGenerator;

  public JoinExtraConditionMatcher(OperatorContext context, LogicalExpression extraCondition,
                                   Map<String, String> build2ProbeKeys,
                                   VectorAccessible probeBatch, ExpandableHyperContainer buildBatch) {
    this.extraCondition = extraCondition;
    this.context = context;
    this.probeBatch = probeBatch;
    this.buildBatch = buildBatch;
    this.build2ProbeKeys = build2ProbeKeys;
    this.totalEvaluations = 0;
    this.totalMatches = 0;
  }

  @Override
  public void setup() {
    setupWatch.start();
    matchGenerator = ExtraConditionEvaluator.generate(extraCondition, context.getClassProducer(), probeBatch,
      buildBatch, build2ProbeKeys);
    matchGenerator.setup(context.getFunctionContext(), probeBatch, buildBatch);
    setupWatch.stop();
  }

  @Override
  public boolean checkCurrentMatch(int currentProbeIndex, int currentLinkBatch, int currentLinkOffset) {
    this.totalEvaluations++;
    final boolean matched = matchGenerator.doEval(currentProbeIndex,
      (currentLinkBatch << 16) | (currentLinkOffset & 65535));
    if (matched) {
      this.totalMatches++;
    }
    return matched;
  }

  @Override
  public long getEvaluationCount() {
    return totalEvaluations;
  }

  @Override
  public long getEvaluationMatchedCount() {
    return totalMatches;
  }

  @Override
  public long getSetupNanos() {
    return setupWatch.elapsed(TimeUnit.NANOSECONDS);
  }
}
