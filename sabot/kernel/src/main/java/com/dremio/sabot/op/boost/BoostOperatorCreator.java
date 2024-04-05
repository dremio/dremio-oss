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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.BoostPOP;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.easy.arrow.ArrowRecordReader;
import com.dremio.exec.store.parquet.ParquetOperatorCreator;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.service.spill.SpillServiceImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;

public class BoostOperatorCreator implements ProducerOperator.Creator<BoostPOP> {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BoostOperatorCreator.class);

  @Override
  public ProducerOperator create(
      FragmentExecutionContext fragmentExecContext, OperatorContext context, BoostPOP config)
      throws ExecutionSetupException {
    final Collection<List<String>> referencedTables = config.getReferencedTables();
    List<String> dataset =
        referencedTables == null || referencedTables.isEmpty()
            ? null
            : referencedTables.iterator().next();

    if (dataset == null) {
      throw new ExecutionSetupException("ReferencedTables property missing in Boost config");
    }

    if (config.isUnlimitedSplitsBoost()) {
      return createBoostArrowFileScanOperator(fragmentExecContext, context, config);
    } else {
      return createBoostOperator(fragmentExecContext, context, config);
    }
  }

  private ProducerOperator createBoostOperator(
      FragmentExecutionContext fragmentExecContext, OperatorContext context, BoostPOP config)
      throws ExecutionSetupException {
    FileSystemPlugin<?> plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    FileSystem fs;
    try {
      fs = plugin.createFS(config.getProps().getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }

    final RecordReaderIterator readers;
    if (!fs.supportsBoosting()) {
      logger.error("Provided Filesystem does not support boosting, creating empty boost operator");
      config = config.withEmptyColumnsToBoost();
      readers = RecordReaderIterator.from(Collections.emptyIterator()); // avoid footer read
    } else {
      /* below readers and splits in config are in same order i.e., first reader is to read first split,...*/
      readers =
          new ParquetOperatorCreator()
              .getReaders(fragmentExecContext, context, config.asParquetSubScan());
    }
    return new BoostOperator(fragmentExecContext, config, context, readers, fs);
  }

  private ProducerOperator createBoostArrowFileScanOperator(
      FragmentExecutionContext fragmentExecContext, OperatorContext context, BoostPOP config)
      throws ExecutionSetupException {
    FileSystem localFileSystem, distFileSystem;
    FileSystemPlugin<?> plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());

    Path arrowFilePath;

    try {
      EasyProtobuf.EasyDatasetSplitXAttr extended =
          LegacyProtobufSerializer.parseFrom(
              EasyProtobuf.EasyDatasetSplitXAttr.PARSER,
              config.getSplits().get(0).getDatasetSplitInfo().getExtendedProperty());
      arrowFilePath = new Path(extended.getPath());
      localFileSystem =
          HadoopFileSystem.get(arrowFilePath.getFileSystem(SpillServiceImpl.getSpillingConfig()));
      distFileSystem = plugin.createFS(config.getProps().getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }

    final RecordReaderIterator readers;
    if (!distFileSystem.supportsBoosting()) {
      logger.error("Provided Filesystem does not support boosting, creating empty boost operator");
      readers =
          RecordReaderIterator.from(
              Collections.emptyIterator()); // Empty easy Scan Operator which will not read anything
    } else {
      readers =
          RecordReaderIterator.from(
              new ArrowRecordReader(
                  context,
                  localFileSystem,
                  com.dremio.io.file.Path.of(arrowFilePath.toString()),
                  config.getColumns()));
    }
    return new ScanOperator(fragmentExecContext, config, context, readers);
  }
}
