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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;

public class GandivaBlackListFunctionsAndExpessionSplitterOptionsChangeListener implements OptionChangeListener {
  private final OptionManager optionManager;
  private final ExpressionSplitCache expressionSplitCache;

  private volatile Set<String> blackListedFunctions;
  private volatile String codeGenOption;
  private volatile boolean isSplitEnabled;
  private volatile long maxSplitsPerExp;
  private volatile double avgWorkThresholdForSplit;
  private volatile boolean splitCachingEnabled;

  public GandivaBlackListFunctionsAndExpessionSplitterOptionsChangeListener(OptionManager optionManager, ExpressionSplitCache expressionSplitCache) {
    this.optionManager = optionManager;
    String disabledFunctions = optionManager.getOption(ExecConstants.DISABLED_GANDIVA_FUNCTIONS);
    this.blackListedFunctions = Arrays.stream(disabledFunctions.split(";")).map(String::toLowerCase).map(String::trim).collect(Collectors.toSet());
    this.codeGenOption = optionManager.getOption(ExecConstants.QUERY_EXEC_OPTION);
    this.isSplitEnabled = optionManager.getOption(ExecConstants.SPLIT_ENABLED);
    this.maxSplitsPerExp = optionManager.getOption(ExecConstants.MAX_SPLITS_PER_EXPRESSION);
    this.avgWorkThresholdForSplit =  optionManager.getOption(ExecConstants.WORK_THRESHOLD_FOR_SPLIT);
    this.expressionSplitCache = expressionSplitCache;
    this.splitCachingEnabled = optionManager.getOption(ExecConstants.SPLIT_CACHING_ENABLED);
  }

  @Override
  public void onChange() {
    String newDisabledFunctions = optionManager.getOption(ExecConstants.DISABLED_GANDIVA_FUNCTIONS);
    String newCodeGenOption = optionManager.getOption(ExecConstants.QUERY_EXEC_OPTION);
    boolean newIsSplitEnabled = optionManager.getOption(ExecConstants.SPLIT_ENABLED);
    long newMaxSplitsPerExp = optionManager.getOption(ExecConstants.MAX_SPLITS_PER_EXPRESSION);
    double newAvgWorkThresholdForSplit = optionManager.getOption(ExecConstants.WORK_THRESHOLD_FOR_SPLIT);
    boolean newSplitCachingEnabled = optionManager.getOption(ExecConstants.SPLIT_CACHING_ENABLED);
    Set<String> newBlackListedFunctions = Arrays.stream(newDisabledFunctions.split(";")).map(String::toLowerCase).map(String::trim).collect(Collectors.toSet());
    if (didValuesChange(newBlackListedFunctions, newCodeGenOption, newIsSplitEnabled, newMaxSplitsPerExp, newAvgWorkThresholdForSplit, newSplitCachingEnabled)) {
      expressionSplitCache.invalidateCache();
      setNewValues(newBlackListedFunctions, newCodeGenOption, newIsSplitEnabled, newMaxSplitsPerExp, newAvgWorkThresholdForSplit, newSplitCachingEnabled);
    }
  }

  private boolean didValuesChange(Set<String> newBlackListedFunctions, String newCodeGenOption, boolean newIsSplitEnabled, long newMaxSplitsPerExp, double newAvgWorkThresholdForSplit, boolean newSplitCachingEnabled) {
    if (!this.blackListedFunctions.equals(newBlackListedFunctions) || this.codeGenOption != newCodeGenOption || this.isSplitEnabled != newIsSplitEnabled || this.maxSplitsPerExp != newMaxSplitsPerExp
      || Double.compare(this.avgWorkThresholdForSplit, newAvgWorkThresholdForSplit) != 0 || this.splitCachingEnabled != newSplitCachingEnabled) {
      return true;
    }
    return false;
  }

  private void setNewValues(Set<String> newBlackListedFunctions, String newCodeGenOption, boolean newIsSplitEnabled, long newMaxSplitsPerExp, double newAvgWorkThresholdForSplit, boolean newSplitCachingEnabled) {
    this.blackListedFunctions = newBlackListedFunctions;
    this.codeGenOption = newCodeGenOption;
    this.isSplitEnabled = newIsSplitEnabled;
    this.maxSplitsPerExp = newMaxSplitsPerExp;
    this.avgWorkThresholdForSplit = newAvgWorkThresholdForSplit;
    this.splitCachingEnabled = newSplitCachingEnabled;
  }
}
