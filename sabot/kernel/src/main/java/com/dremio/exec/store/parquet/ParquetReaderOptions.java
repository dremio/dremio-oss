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
package com.dremio.exec.store.parquet;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionResolver;

/**
 * Provides common options used in Parquet reader implementations.
 */
public class ParquetReaderOptions {

  private boolean vectorizationEnabled;
  private boolean autoCorrectCorruptDatesEnabled;
  private boolean readInt96AsTimestampEnabled;
  private boolean detailedTracingEnabled;
  private boolean prefetchingEnabled;
  private int numSplitsToPrefetch;
  private boolean arrowCachingEnabled;

  public boolean isVectorizationEnabled() {
    return vectorizationEnabled;
  }

  public boolean isAutoCorrectCorruptDatesEnabled() {
    return autoCorrectCorruptDatesEnabled;
  }

  public boolean isReadInt96AsTimestampEnabled() {
    return readInt96AsTimestampEnabled;
  }

  public boolean isDetailedTracingEnabled() {
    return detailedTracingEnabled;
  }

  public boolean isPrefetchingEnabled() {
    return prefetchingEnabled;
  }

  public int getNumSplitsToPrefetch() {
    return numSplitsToPrefetch;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(OptionResolver optionResolver) {
    return new Builder(optionResolver);
  }

  public static ParquetReaderOptions from(OptionResolver optionResolver) {
    return builder(optionResolver).build();
  }

  public static class Builder {

    private final ParquetReaderOptions options;

    public Builder() {
      options = new ParquetReaderOptions();
    }

    public Builder(OptionResolver optionResolver) {
      options = new ParquetReaderOptions();
      options.vectorizationEnabled = optionResolver.getOption(ExecConstants.PARQUET_READER_VECTORIZE);
      options.autoCorrectCorruptDatesEnabled =
        optionResolver.getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);
      options.readInt96AsTimestampEnabled =
        optionResolver.getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
      options.detailedTracingEnabled = optionResolver.getOption(ExecConstants.ENABLED_PARQUET_TRACING);
      options.prefetchingEnabled = optionResolver.getOption(ExecConstants.PREFETCH_READER);
      options.numSplitsToPrefetch = (int) optionResolver.getOption(ExecConstants.NUM_SPLITS_TO_PREFETCH);
      options.arrowCachingEnabled = optionResolver.getOption(ExecConstants.ENABLE_PARQUET_ARROW_CACHING);
    }

    public Builder enableVectorization(boolean value) {
      options.vectorizationEnabled = value;
      return this;
    }

    public Builder enableAutoCorrectCorruptDates(boolean value) {
      options.autoCorrectCorruptDatesEnabled = value;
      return this;
    }

    public Builder enableReadInt96AsTimestamp(boolean value) {
      options.readInt96AsTimestampEnabled = value;
      return this;
    }

    public Builder enableDetailedTracing(boolean value) {
      options.detailedTracingEnabled = value;
      return this;
    }

    public Builder enablePrefetching(boolean value) {
      options.prefetchingEnabled = value;
      return this;
    }

    public Builder setNumSplitsToPrefetch(int value) {
      options.numSplitsToPrefetch = value;
      return this;
    }

    public Builder enableArrowCaching(boolean value) {
      options.arrowCachingEnabled = value;
      return this;
    }

    public ParquetReaderOptions build() {
      return options;
    }
  }
}
