/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.hadoop.hive.conf.HiveConf;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HiveStoragePlugin;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("unused")
public class HiveScanBatchCreator implements ProducerOperator.Creator<HiveSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScanBatchCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveSubScan config) throws ExecutionSetupException {
    final HiveStoragePlugin storagePlugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    final HiveConf conf = storagePlugin.getHiveConf();

    final HiveTableXattr tableAttr;
    try {
      tableAttr = HiveTableXattr.parseFrom(config.getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure parsing table extended properties.", e);
    }

    final CompositeReaderConfig compositeConfig = CompositeReaderConfig.getCompound(config.getSchema(), config.getColumns(), config.getPartitionColumns());
    switch(tableAttr.getReaderType()){
    case NATIVE_PARQUET:
      return ScanWithDremioReader.createProducer(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
    case BASIC:
      return ScanWithHiveReader.createProducer(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
    default:
      throw new UnsupportedOperationException(tableAttr.getReaderType().name());
    }
  }
}
