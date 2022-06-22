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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;

public class TestParquetReaderOptions {

  @Test
  public void testBuild() {
    ParquetReaderOptions options = ParquetReaderOptions.builder()
      .enableVectorization(true)
      .enableAutoCorrectCorruptDates(true)
      .enableReadInt96AsTimestamp(true)
      .enableDetailedTracing(true)
      .enablePrefetching(true)
      .setNumSplitsToPrefetch(2)
      .enableArrowCaching(true)
      .build();

    assertThat(options.isVectorizationEnabled()).isTrue();
    assertThat(options.isAutoCorrectCorruptDatesEnabled()).isTrue();
    assertThat(options.isReadInt96AsTimestampEnabled()).isTrue();
    assertThat(options.isDetailedTracingEnabled()).isTrue();
    assertThat(options.isPrefetchingEnabled()).isTrue();
    assertThat(options.getNumSplitsToPrefetch()).isEqualTo(2);
    assertThat(options.isArrowCachingEnabled()).isTrue();
  }

  @Test
  public void testBuildFromOptions() {
    final OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec()
      .addOption(ExecConstants.PARQUET_READER_VECTORIZE, true)
      .addOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR, true)
      .addOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR, true)
      .addOption(ExecConstants.ENABLED_PARQUET_TRACING, true)
      .addOption(ExecConstants.PREFETCH_READER, true)
      .addOption(ExecConstants.NUM_SPLITS_TO_PREFETCH, 2)
      .addOption(ExecConstants.ENABLE_PARQUET_ARROW_CACHING, true));

    ParquetReaderOptions options = ParquetReaderOptions.from(optionResolver);

    assertThat(options.isVectorizationEnabled()).isTrue();
    assertThat(options.isAutoCorrectCorruptDatesEnabled()).isTrue();
    assertThat(options.isReadInt96AsTimestampEnabled()).isTrue();
    assertThat(options.isDetailedTracingEnabled()).isTrue();
    assertThat(options.isPrefetchingEnabled()).isTrue();
    assertThat(options.getNumSplitsToPrefetch()).isEqualTo(2);
    assertThat(options.isArrowCachingEnabled()).isTrue();
  }
}
