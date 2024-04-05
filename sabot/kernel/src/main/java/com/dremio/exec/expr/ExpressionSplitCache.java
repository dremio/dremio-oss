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

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ExpressionSplitCache {
  private final LoadingCache<ExpAndCodeGenEngineHolder, ExpressionSplitsHolder>
      expressionSplitsCache;
  private volatile boolean listenerAdded = false;
  private final OptionManager optionManager;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  public ExpressionSplitCache(final OptionManager optionManager, SabotConfig config) {
    final long cacheMaxSize = config.getInt(ExecConstants.MAX_SPLIT_CACHE_SIZE_CONFIG);
    this.optionManager = optionManager;
    this.expressionSplitsCache =
        CacheBuilder.newBuilder()
            .softValues()
            .maximumSize(cacheMaxSize)
            .build(new ExpToExpressionSplitsCacheLoader());
  }

  public ExpressionSplitsHolder getSplitsFromCache(
      ExpAndCodeGenEngineHolder expAndCodeGenEngineHolder) throws ExecutionException {
    if (!listenerAdded) {
      addListener();
    }
    return expressionSplitsCache.get(expAndCodeGenEngineHolder);
  }

  private void addListener() {
    optionManager.addOptionChangeListener(
        new ExpressionSplitterOptionsChangeListener(optionManager, this));
    listenerAdded = true;
  }

  private static class ExpToExpressionSplitsCacheLoader
      extends CacheLoader<ExpAndCodeGenEngineHolder, ExpressionSplitsHolder> {
    @Override
    public ExpressionSplitsHolder load(final ExpAndCodeGenEngineHolder expAndCodeGenEngineHolder)
        throws Exception {
      int initialOutPutFieldCounter =
          expAndCodeGenEngineHolder.getExpressionSplitter().getOutputFieldCounter();
      ExpressionSplitter expressionSplitter = expAndCodeGenEngineHolder.getExpressionSplitter();
      ExpressionSplitsHolder expressionSplitsHolder =
          expressionSplitter.splitExpressionWhenCacheIsEnabled(
              expAndCodeGenEngineHolder.getNamedExpression());
      expAndCodeGenEngineHolder
          .getExpressionSplitter()
          .setOutputFieldCounter(initialOutPutFieldCounter);
      expAndCodeGenEngineHolder.setExpressionSplitter(null);
      return expressionSplitsHolder;
    }
  }

  public void invalidateCache() {
    this.expressionSplitsCache.invalidateAll();
  }

  static class ExpressionSplitsHolder {
    public ExpressionSplitsHolder(
        CachableExpressionSplit finalExpressionSplit,
        List<CachableExpressionSplit> expressionSplits) {
      this.finalExpressionSplit = finalExpressionSplit;
      if (expressionSplits.isEmpty()) {
        this.expressionSplits = Collections.emptyList();
      } else {
        this.expressionSplits = ImmutableList.copyOf(expressionSplits);
      }
    }

    public CachableExpressionSplit getFinalExpressionSplit() {
      return finalExpressionSplit;
    }

    public List<CachableExpressionSplit> getExpressionSplits() {
      return expressionSplits;
    }

    private final CachableExpressionSplit finalExpressionSplit;
    private final List<CachableExpressionSplit> expressionSplits;
  }
}
