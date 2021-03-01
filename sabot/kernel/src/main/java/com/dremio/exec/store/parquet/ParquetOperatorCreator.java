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

import java.util.concurrent.TimeUnit;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.dfs.PrefetchingIterator;
import com.dremio.options.Options;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.google.common.base.Stopwatch;

/**
 * Parquet scan batch creator from dataset config
 */
@Options
public class ParquetOperatorCreator implements Creator<ParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetOperatorCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    final Stopwatch watch = Stopwatch.createStarted();
    try {
      ParquetSplitReaderCreatorIterator creator = new ParquetSplitReaderCreatorIterator(fragmentExecContext, context, config, true);
      logger.debug("Took {} ms to create Parquet Scan.", watch.elapsed(TimeUnit.MILLISECONDS));
      return creator.createScan();
    } catch (Exception ex) {
      throw new ExecutionSetupException("Failed to create scan operator.", ex);
    }
  }

  public RecordReaderIterator getReaders(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    ParquetSplitReaderCreatorIterator creator = new ParquetSplitReaderCreatorIterator(fragmentExecContext, context, config, true);
    return new PrefetchingIterator(creator);
  }

}
