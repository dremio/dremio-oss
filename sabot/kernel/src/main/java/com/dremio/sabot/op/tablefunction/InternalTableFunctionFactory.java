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
package com.dremio.sabot.op.tablefunction;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.dfs.SplitGenTableFunction;
import com.dremio.exec.store.iceberg.ManifestScanTableFunction;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

/**
 * Internal factory that creates various scan table functions
 */
public class InternalTableFunctionFactory implements TableFunctionFactory{
  @Override
  public TableFunction createTableFunction(FragmentExecutionContext fec,
                                           OperatorContext context,
                                           OpProps props,
                                           TableFunctionConfig functionConfig) {
    switch (functionConfig.getType()) {
      case PARQUET_DATA_SCAN:
        return new ParquetScanTableFunction(fec, context, props, functionConfig);
      case ICEBERG_MANIFEST_SCAN:
        return new ManifestScanTableFunction(fec, context, functionConfig);
      case SPLIT_GENERATION:
        return new SplitGenTableFunction(fec, context, functionConfig);
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException("Unknown table function type");
    }
  }
}
