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
package com.dremio.exec.expr;

import static com.dremio.exec.ExecConstants.DISABLED_GANDIVA_FUNCTIONS;
import static com.dremio.exec.ExecConstants.MAX_SPLITS_PER_EXPRESSION;
import static com.dremio.exec.ExecConstants.QUERY_EXEC_OPTION;
import static com.dremio.exec.ExecConstants.SPLIT_CACHING_ENABLED;
import static com.dremio.exec.ExecConstants.SPLIT_ENABLED;
import static com.dremio.exec.ExecConstants.WORK_THRESHOLD_FOR_SPLIT;
import static com.dremio.exec.expr.fn.GandivaFunctionRegistry.toLowerCaseSet;

import java.util.Set;

import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;

public class ExpressionSplitterOptionsChangeListener implements OptionChangeListener {
  private final OptionManager optionManager;
  private final ExpressionSplitCache expressionSplitCache;

  private volatile Set<String> blackListedFunctions;
  private volatile String codeGenOption;
  private volatile boolean isSplitEnabled;
  private volatile long maxSplitsPerExp;
  private volatile double avgWorkThresholdForSplit;
  private volatile boolean splitCachingEnabled;

  public ExpressionSplitterOptionsChangeListener(OptionManager optionManager, ExpressionSplitCache expressionSplitCache) {
    this.optionManager = optionManager;
    this.blackListedFunctions = toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS));
    this.codeGenOption = optionManager.getOption(QUERY_EXEC_OPTION);
    this.isSplitEnabled = optionManager.getOption(SPLIT_ENABLED);
    this.maxSplitsPerExp = optionManager.getOption(MAX_SPLITS_PER_EXPRESSION);
    this.avgWorkThresholdForSplit = optionManager.getOption(WORK_THRESHOLD_FOR_SPLIT);
    this.expressionSplitCache = expressionSplitCache;
    this.splitCachingEnabled = optionManager.getOption(SPLIT_CACHING_ENABLED);
  }

  @Override
  public void onChange() {
    final String newCodeGenOption = optionManager.getOption(QUERY_EXEC_OPTION);
    final boolean newIsSplitEnabled = optionManager.getOption(SPLIT_ENABLED);
    final long newMaxSplitsPerExp = optionManager.getOption(MAX_SPLITS_PER_EXPRESSION);
    final double newAvgWorkThresholdForSplit = optionManager.getOption(WORK_THRESHOLD_FOR_SPLIT);
    final boolean newSplitCachingEnabled = optionManager.getOption(SPLIT_CACHING_ENABLED);
    final Set<String> newBlackListedFunctions = toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS));
    if (didValuesChange(newBlackListedFunctions, newCodeGenOption, newIsSplitEnabled, newMaxSplitsPerExp,
      newAvgWorkThresholdForSplit, newSplitCachingEnabled)) {
      expressionSplitCache.invalidateCache();
      setNewValues(newBlackListedFunctions, newCodeGenOption, newIsSplitEnabled, newMaxSplitsPerExp,
        newAvgWorkThresholdForSplit, newSplitCachingEnabled);
    }
  }

  private boolean didValuesChange(Set<String> newBlackListedFunctions, String newCodeGenOption,
                                  boolean newIsSplitEnabled, long newMaxSplitsPerExp,
                                  double newAvgWorkThresholdForSplit,
                                  boolean newSplitCachingEnabled) {
    return this.isSplitEnabled != newIsSplitEnabled
      || this.maxSplitsPerExp != newMaxSplitsPerExp
      || Double.compare(this.avgWorkThresholdForSplit, newAvgWorkThresholdForSplit) != 0
      || this.splitCachingEnabled != newSplitCachingEnabled
      || !this.codeGenOption.equals(newCodeGenOption)
      || !this.blackListedFunctions.equals(newBlackListedFunctions);
  }

  private void setNewValues(Set<String> newBlackListedFunctions, String newCodeGenOption, boolean newIsSplitEnabled,
                            long newMaxSplitsPerExp, double newAvgWorkThresholdForSplit,
                            boolean newSplitCachingEnabled) {
    this.blackListedFunctions = newBlackListedFunctions;
    this.codeGenOption = newCodeGenOption;
    this.isSplitEnabled = newIsSplitEnabled;
    this.maxSplitsPerExp = newMaxSplitsPerExp;
    this.avgWorkThresholdForSplit = newAvgWorkThresholdForSplit;
    this.splitCachingEnabled = newSplitCachingEnabled;
  }
}
