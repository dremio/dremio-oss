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
package com.dremio.exec.store.hive.exec;

import java.util.List;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.hive.StoragePluginCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.ScanTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

/**
 * Hive implementation of ScanTableFunction
 */
public class HiveScanTableFunction extends ScanTableFunction {

  private final HiveProxiedScanBatchCreator hiveProxiedScanBatchCreator;

  public HiveScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);

    try {
      final SupportsPF4JStoragePlugin pf4JStoragePlugin = fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
      final StoragePluginCreator.PF4JStoragePlugin plugin = pf4JStoragePlugin.getPF4JStoragePlugin();
      hiveProxiedScanBatchCreator = plugin.createScanBatchCreator(fec, context, props, functionConfig);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException("Error creating HiveProxiedScanBatchCreator", e);
    }
  }

  @Override
  protected RecordReaderIterator createRecordReaderIterator() {
    return hiveProxiedScanBatchCreator.createRecordReaderIterator();
  }

  @Override
  protected RecordReaderIterator getRecordReaderIterator() {
    return hiveProxiedScanBatchCreator.getRecordReaderIterator();
  }

  @Override
  protected void addSplits(List<SplitAndPartitionInfo> splits) {
    hiveProxiedScanBatchCreator.addSplits(splits);
  }

  @Override
  public void close() throws Exception {
    RecordReaderIterator recordReaderIterator = hiveProxiedScanBatchCreator.getRecordReaderIterator();
    AutoCloseables.close(super::close, recordReaderIterator);
  }
}
