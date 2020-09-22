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
package com.dremio.sabot.op.boost;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.BoostPOP;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.store.parquet.ParquetOperatorCreator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.ProducerOperator;

public class BoostOperatorCreator implements ProducerOperator.Creator<BoostPOP> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoostOperatorCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, BoostPOP config) throws ExecutionSetupException {
    final Collection<List<String>> referencedTables = config.getReferencedTables();
    List<String> dataset = referencedTables == null || referencedTables.isEmpty() ? null : referencedTables.iterator().next();

    if (dataset == null) {
      throw new ExecutionSetupException("ReferencedTables property missing in Boost config");
    }

    FileSystemPlugin<?> plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    FileSystem fs;
    try {
      fs = plugin.createFS(config.getProps().getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }

    final Iterator<RecordReader> readers;
    if (!fs.supportsBoosting()) {
      logger.error("Provided Filesystem does not support boosting, creating empty boost operator");
      config = config.withEmptyColumnsToBoost();
      readers = Collections.emptyIterator(); // avoid footer read
    } else {
      /* below readers and splits in config are in same order i.e., first reader is to read first split,...*/
      readers = new ParquetOperatorCreator().getReaders(fragmentExecContext, context, config.asParquetSubScan());
    }

    return new BoostOperator(config, context, readers, GlobalDictionaries.create(context, fs, config.getGlobalDictionaryEncodedColumns()), fs);
  }
}
