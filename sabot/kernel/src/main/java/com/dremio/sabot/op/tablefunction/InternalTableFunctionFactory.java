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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.dfs.SplitAssignmentTableFunction;
import com.dremio.exec.store.dfs.SplitGenTableFunction;
import com.dremio.exec.store.iceberg.ManifestFileProcessor;
import com.dremio.exec.store.iceberg.ManifestScanTableFunction;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.metadatarefresh.schemaagg.SchemaAggTableFunction;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.exec.store.parquet.ParquetSplitCreator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

/**
 * Internal factory that creates various scan table functions
 */
public class InternalTableFunctionFactory implements TableFunctionFactory {
  @Override
  public TableFunction createTableFunction(FragmentExecutionContext fec,
                                           OperatorContext context,
                                           OpProps props,
                                           TableFunctionConfig functionConfig) throws ExecutionSetupException {
    switch (functionConfig.getType()) {
      case DATA_FILE_SCAN:
        return new ParquetScanTableFunction(fec, context, props, functionConfig);
      case METADATA_REFRESH_MANIFEST_SCAN:
      case SPLIT_GEN_MANIFEST_SCAN:
        BlockBasedSplitGenerator.SplitCreator splitCreator = new ParquetSplitCreator();
        return getManifestScanTableFunction(fec, context, props, functionConfig, splitCreator);
      case SPLIT_GENERATION:
        return new SplitGenTableFunction(fec, context, functionConfig);
      case FOOTER_READER:
        return new FooterReadTableFunction(fec, context, props, functionConfig);
      case SCHEMA_AGG:
        return new SchemaAggTableFunction(context, functionConfig);
      case SPLIT_ASSIGNMENT:
        return new SplitAssignmentTableFunction(context, functionConfig);
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException("Unknown table function type " + functionConfig.getType());
    }
  }

  protected TableFunction getManifestScanTableFunction(FragmentExecutionContext fec,
                                                       OperatorContext context,
                                                       OpProps props,
                                                       TableFunctionConfig functionConfig,
                                                       BlockBasedSplitGenerator.SplitCreator splitCreator) {
    ManifestFileProcessor manifestFileProcessor = new ManifestFileProcessor(fec, context, props, functionConfig, splitCreator);
    return new ManifestScanTableFunction(context, functionConfig, manifestFileProcessor);
  }
}
