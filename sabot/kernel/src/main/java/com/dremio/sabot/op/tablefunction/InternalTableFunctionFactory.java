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
import com.dremio.exec.physical.config.CarryForwardAwareTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.deltalake.DeltaLakeHistoryScanTableFunction;
import com.dremio.exec.store.dfs.DirListingSplitGenTableFunction;
import com.dremio.exec.store.dfs.EasySplitGenTableFunction;
import com.dremio.exec.store.dfs.SplitAssignmentTableFunction;
import com.dremio.exec.store.dfs.SplitGenTableFunction;
import com.dremio.exec.store.easy.EasyScanTableFunction;
import com.dremio.exec.store.iceberg.DeletedFilesMetadataTableFunction;
import com.dremio.exec.store.iceberg.IcebergDeleteFileAggTableFunction;
import com.dremio.exec.store.iceberg.IcebergDmlMergeDuplicateCheckTableFunction;
import com.dremio.exec.store.iceberg.IcebergIncrementalRefreshJoinKeyTableFunction;
import com.dremio.exec.store.iceberg.IcebergLocationFinderTableFunction;
import com.dremio.exec.store.iceberg.IcebergOrphanFileDeleteTableFunction;
import com.dremio.exec.store.iceberg.IcebergPartitionTransformTableFunction;
import com.dremio.exec.store.iceberg.IcebergSplitGenTableFunction;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.InputCarryForwardTableFunctionDecorator;
import com.dremio.exec.store.iceberg.ManifestFileProcessor;
import com.dremio.exec.store.iceberg.ManifestListScanTableFunction;
import com.dremio.exec.store.iceberg.ManifestScanTableFunction;
import com.dremio.exec.store.iceberg.OptimizeManifestsTableFunction;
import com.dremio.exec.store.iceberg.PartitionStatsScanTableFunction;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingTableFunction;
import com.dremio.exec.store.metadatarefresh.schemaagg.SchemaAggTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.boost.BoostTableFunction;

/** Internal factory that creates various scan table functions */
public class InternalTableFunctionFactory implements TableFunctionFactory {
  @Override
  public TableFunction createTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig)
      throws ExecutionSetupException {
    TableFunction tableFunction = createTableFunctionBase(fec, context, props, functionConfig);
    return applyCarryForwardIfNecessary(functionConfig.getFunctionContext(), tableFunction);
  }

  private TableFunction applyCarryForwardIfNecessary(
      TableFunctionContext functionContext, TableFunction tableFunction) {
    if (functionContext instanceof CarryForwardAwareTableFunctionContext) {
      CarryForwardAwareTableFunctionContext carryFwdFunCtx =
          ((CarryForwardAwareTableFunctionContext) functionContext);
      if (carryFwdFunCtx.isCarryForwardEnabled()) {
        return new InputCarryForwardTableFunctionDecorator(
            tableFunction,
            SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_COLS,
            carryFwdFunCtx.getInputColMap(),
            carryFwdFunCtx.getConstValCol(),
            carryFwdFunCtx.getConstVal());
      }
    }
    return tableFunction;
  }

  private TableFunction createTableFunctionBase(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig)
      throws ExecutionSetupException {
    switch (functionConfig.getType()) {
      case DATA_FILE_SCAN:
        SupportsInternalIcebergTable plugin =
            IcebergUtils.getSupportsInternalIcebergTablePlugin(
                fec, functionConfig.getFunctionContext().getPluginId());
        return plugin.createScanTableFunction(fec, context, props, functionConfig);
      case EASY_DATA_FILE_SCAN:
        return getEasyScanTableFunction(fec, context, props, functionConfig);
      case SPLIT_GEN_MANIFEST_SCAN:
      case METADATA_MANIFEST_FILE_SCAN:
      case ICEBERG_MANIFEST_SCAN:
        ManifestFileProcessor manifestFileProcessor =
            new ManifestFileProcessor(fec, context, props, functionConfig);
        return new ManifestScanTableFunction(context, functionConfig, manifestFileProcessor);
      case ICEBERG_MANIFEST_LIST_SCAN:
        return new ManifestListScanTableFunction(fec, context, props, functionConfig);
      case ICEBERG_PARTITION_STATS_SCAN:
        return new PartitionStatsScanTableFunction(fec, context, props, functionConfig);
      case SPLIT_GENERATION:
        return new SplitGenTableFunction(fec, context, functionConfig);
      case DIR_LISTING_SPLIT_GENERATION:
        return new DirListingSplitGenTableFunction(fec, context, functionConfig);
      case EASY_SPLIT_GENERATION:
        return new EasySplitGenTableFunction(fec, context, functionConfig);
      case FOOTER_READER:
        SupportsInternalIcebergTable internalIcebergTablePlugin =
            IcebergUtils.getSupportsInternalIcebergTablePlugin(
                fec, functionConfig.getFunctionContext().getPluginId());
        return internalIcebergTablePlugin.getFooterReaderTableFunction(
            fec, context, props, functionConfig);
      case SCHEMA_AGG:
        return new SchemaAggTableFunction(context, functionConfig);
      case SPLIT_ASSIGNMENT:
        return new SplitAssignmentTableFunction(fec, context, props, functionConfig);
      case BOOST_TABLE_FUNCTION:
        return new BoostTableFunction(fec, context, props, functionConfig);
      case ICEBERG_PARTITION_TRANSFORM:
        return new IcebergPartitionTransformTableFunction(context, functionConfig);
      case DELETED_FILES_METADATA:
        return new DeletedFilesMetadataTableFunction(context, functionConfig);
      case ICEBERG_SPLIT_GEN:
        return new IcebergSplitGenTableFunction(fec, context, functionConfig);
      case ICEBERG_DELETE_FILE_AGG:
        return new IcebergDeleteFileAggTableFunction(context, functionConfig);
      case ICEBERG_DML_MERGE_DUPLICATE_CHECK:
        return new IcebergDmlMergeDuplicateCheckTableFunction(context, functionConfig);
      case ICEBERG_OPTIMIZE_MANIFESTS:
        return new OptimizeManifestsTableFunction(fec, context, props, functionConfig);
      case ICEBERG_ORPHAN_FILE_DELETE:
        return new IcebergOrphanFileDeleteTableFunction(fec, context, props, functionConfig);
      case DIR_LISTING:
        return new DirListingTableFunction(fec, context, props, functionConfig);
      case ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY:
        return new IcebergIncrementalRefreshJoinKeyTableFunction(
            fec, context, props, functionConfig);
      case ICEBERG_TABLE_LOCATION_FINDER:
        return new IcebergLocationFinderTableFunction(fec, context, props, functionConfig);
      case DELTALAKE_HISTORY_SCAN:
        return new DeltaLakeHistoryScanTableFunction(fec, context, props, functionConfig);
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException(
            "Unknown table function type " + functionConfig.getType());
    }
  }

  public static TableFunction getEasyScanTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    return new EasyScanTableFunction(fec, context, props, functionConfig);
  }
}
