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

import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.ManifestListScanTableFunctionContext;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.EasySplitGenTableFunction;
import com.dremio.exec.store.dfs.SplitAssignmentTableFunction;
import com.dremio.exec.store.dfs.SplitGenTableFunction;
import com.dremio.exec.store.easy.EasyScanTableFunction;
import com.dremio.exec.store.iceberg.DeletedFilesMetadataTableFunction;
import com.dremio.exec.store.iceberg.IcebergDeleteFileAggTableFunction;
import com.dremio.exec.store.iceberg.IcebergDmlMergeDuplicateCheckTableFunction;
import com.dremio.exec.store.iceberg.IcebergFileType;
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
import com.dremio.exec.store.metadatarefresh.schemaagg.SchemaAggTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.boost.BoostTableFunction;
import com.google.common.collect.ImmutableMap;

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
        SupportsInternalIcebergTable plugin = IcebergUtils.getSupportsInternalIcebergTablePlugin(fec, functionConfig.getFunctionContext().getPluginId());
        return plugin.createScanTableFunction(fec, context, props, functionConfig);
      case EASY_DATA_FILE_SCAN:
        return getEasyScanTableFunction(fec, context, props, functionConfig);
      case SPLIT_GEN_MANIFEST_SCAN:
      case METADATA_MANIFEST_FILE_SCAN:
      case ICEBERG_MANIFEST_SCAN:
        ManifestFileProcessor manifestFileProcessor = new ManifestFileProcessor(fec, context, props, functionConfig);
        TableFunction manifestScanTF = new ManifestScanTableFunction(context, functionConfig, manifestFileProcessor);
        if (((ManifestScanTableFunctionContext) functionConfig.getFunctionContext()).isCarryForwardEnabled()) {
          Map<SchemaPath, SchemaPath> inputMapping = ImmutableMap.of(SchemaPath.getCompoundPath(SystemSchemas.SPLIT_IDENTITY, SplitIdentity.PATH), SchemaPath.getSimplePath(SystemSchemas.FILE_PATH));
          return new InputCarryForwardTableFunctionDecorator(manifestScanTF, SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_COLS,
            inputMapping, SystemSchemas.FILE_TYPE, IcebergFileType.MANIFEST.name());
        }
        return manifestScanTF;
      case ICEBERG_MANIFEST_LIST_SCAN:
        TableFunction manifestListScanTF = new ManifestListScanTableFunction(fec, context, props, functionConfig);
        if (((ManifestListScanTableFunctionContext) functionConfig.getFunctionContext()).isCarryForwardEnabled()) {
          Map<SchemaPath, SchemaPath> inputMapping = ImmutableMap.of(SchemaPath.getSimplePath(SystemSchemas.MANIFEST_LIST_PATH), SchemaPath.getSimplePath(SystemSchemas.FILE_PATH));
          return new InputCarryForwardTableFunctionDecorator(manifestListScanTF, SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_COLS, inputMapping, SystemSchemas.FILE_TYPE, IcebergFileType.MANIFEST_LIST.name());
        }
        return manifestListScanTF;
      case ICEBERG_PARTITION_STATS_SCAN:
        return new PartitionStatsScanTableFunction(fec, context, props, functionConfig);
      case SPLIT_GENERATION:
        return new SplitGenTableFunction(fec, context, functionConfig);
      case EASY_SPLIT_GENERATION:
        return new EasySplitGenTableFunction(fec, context, functionConfig);
      case FOOTER_READER:
        SupportsInternalIcebergTable internalIcebergTablePlugin = IcebergUtils.getSupportsInternalIcebergTablePlugin(fec, functionConfig.getFunctionContext().getPluginId());
        return internalIcebergTablePlugin.getFooterReaderTableFunction(fec, context, props, functionConfig);
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
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException("Unknown table function type " + functionConfig.getType());
    }
  }

  public static TableFunction getEasyScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    return new EasyScanTableFunction(fec, context, props, functionConfig);
  }
}
