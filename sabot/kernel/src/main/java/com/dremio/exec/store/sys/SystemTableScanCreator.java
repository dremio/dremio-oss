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
package com.dremio.exec.store.sys;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

/**
 * This class creates batches based on the the type of {@link com.dremio.exec.store.sys.SystemTable}.
 */
public class SystemTableScanCreator implements ProducerOperator.Creator<SystemSubScan> {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, SystemSubScan config) throws ExecutionSetupException {
    final SystemTable table = config.getTable();
    final SystemStoragePlugin plugin2 = fec.getStoragePlugin(config.getPluginId());
    final RecordReader reader = new PojoRecordReader(table.getPojoClass(), table.getIterator
            (plugin2.getSabotContext(), context), config.getColumns(), context.getTargetBatchSize());

    return new ScanOperator(config, context, RecordReaderIterator.from(reader));
  }
}
