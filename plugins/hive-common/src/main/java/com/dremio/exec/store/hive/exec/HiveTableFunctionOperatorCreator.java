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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.hive.StoragePluginCreator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.tablefunction.InternalTableFunctionFactory;
import com.dremio.sabot.op.tablefunction.TableFunction;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;

/**
 * TableFunctionOperatorCreator for Hive tables
 */
public class HiveTableFunctionOperatorCreator implements SingleInputOperator.Creator<HiveTableFunctionPOP> {
  @Override
  public SingleInputOperator create(OperatorContext context, HiveTableFunctionPOP operator) throws ExecutionSetupException {
    throw new UnsupportedOperationException("Not Implemented");
  }

  @Override
  public SingleInputOperator create(FragmentExecutionContext fec, OperatorContext context, HiveTableFunctionPOP operator) throws ExecutionSetupException {
    return new TableFunctionOperator(fec, context, operator, new HiveTableFunctionFactory());
  }

  private static class HiveTableFunctionFactory extends InternalTableFunctionFactory {

    @Override
    public TableFunction createTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) throws ExecutionSetupException {
      switch (functionConfig.getType()) {
        case DATA_FILE_SCAN:
          return new HiveScanTableFunction(fec, context, props, functionConfig);
        case SPLIT_GEN_MANIFEST_SCAN:
          final SupportsPF4JStoragePlugin pf4JStoragePlugin = fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
          final StoragePluginCreator.PF4JStoragePlugin plugin = pf4JStoragePlugin.getPF4JStoragePlugin();
          BlockBasedSplitGenerator.SplitCreator splitCreator = plugin.createSplitCreator();
          return super.getManifestScanTableFunction(fec, context, props, functionConfig, splitCreator);
        default:
          throw new UnsupportedOperationException("Unknown table function type " + functionConfig.getType());
      }
    }
  }
}
