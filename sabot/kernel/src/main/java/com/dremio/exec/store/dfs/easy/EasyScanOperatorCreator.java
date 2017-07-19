/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs.easy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemStoragePlugin2;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.EasyDatasetXAttr;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

/**
 * Easy scan batch creator from dataset config.
 */
public class EasyScanOperatorCreator implements ProducerOperator.Creator<EasySubScan>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(com.dremio.exec.store.dfs.easy.EasyScanOperatorCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, EasySubScan config) throws ExecutionSetupException {
    final FileSystemStoragePlugin2 registry = (FileSystemStoragePlugin2) fragmentExecContext.getStoragePlugin(config.getPluginId());
    final FileSystemPlugin fsPlugin = registry.getFsPlugin();

    final FileSystemWrapper fs = registry.getFs();
    final FormatPluginConfig formatConfig = PhysicalDatasetUtils.toFormatPlugin(config.getFileConfig(), Collections.<String>emptyList());
    final EasyFormatPlugin formatPlugin = (EasyFormatPlugin)fsPlugin.getFormatPlugin(formatConfig);

    final ImplicitFilesystemColumnFinder explorer = new ImplicitFilesystemColumnFinder(context.getOptions(), fs, config.getColumns());

    FluentIterable<FileWork> fileWorkIterable = FluentIterable.from(config.getSplits())
      .transform(new Function<DatasetSplit, FileWork>() {
        @Override
        public FileWork apply(DatasetSplit split) {
          final EasyDatasetSplitXAttr xAttr = EasyDatasetXAttrSerDe.EASY_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray());
          return new CompleteFileWork.FileWorkImpl(xAttr.getStart(), xAttr.getLength(), xAttr.getPath());
        }
      });

    boolean sortReaders = context.getOptions().getOption(ExecConstants.SORT_FILE_BLOCKS);

    Comparator<FileWork> comparator = new Comparator<FileWork>() {
      @Override
      public int compare(FileWork o1, FileWork o2) {
        // sort by path, and then by start. The most important point is to ensure that the first line of a file is read first,
        // as it may contain a header.
        if (o1.getPath().compareTo(o2.getPath()) != 0) {
          return o1.getPath().compareTo(o2.getPath());
        } else {
          return Long.compare(o1.getStart(), o2.getStart());
        }
      }
    };

    List<FileWork> fileWorkList = sortReaders ?  fileWorkIterable.toSortedList(comparator) : fileWorkIterable.toList();

    final List<RecordReader> readers = new ArrayList<>();
    final List<String> paths = new ArrayList<>();

    for (FileWork fileWork : fileWorkList) {
      paths.add(fileWork.getPath());
      readers.add(formatPlugin.getRecordReader(context, fs, fileWork, explorer.getRealFields()));
    }

    if(!explorer.hasImplicitColumns()){
      return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator());
    }

    EasyDatasetXAttr easyDatasetXAttr = EasyDatasetXAttrSerDe.EASY_DATASET_XATTR_SERIALIZER.revert(config.getReadDefinition().getExtendedProperty().toByteArray());
    final List<List<NameValuePair<?>>> partitionPairs = explorer.getImplicitFields(easyDatasetXAttr.getSelectionRoot(), paths);
    final List<RecordReader> wrappedReaders = new ArrayList<>();
    Preconditions.checkArgument(readers.size() == partitionPairs.size(), "Number of partition values (%s) doesn't match number of readers (%s). ", partitionPairs.size(), readers.size());

    for(int i = 0; i < readers.size(); i++){
      wrappedReaders.add(new AdditionalColumnsRecordReader(readers.get(i), partitionPairs.get(i)));
    }

    return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, wrappedReaders.iterator());
  }

}
