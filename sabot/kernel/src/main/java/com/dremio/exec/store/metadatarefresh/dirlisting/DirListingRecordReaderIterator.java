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

package com.dremio.exec.store.metadatarefresh.dirlisting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.google.protobuf.InvalidProtocolBufferException;

public class DirListingRecordReaderIterator implements RecordReaderIterator {

  private final OperatorContext context;
  private final DirListingSubScan config;
  private final List<SplitAndPartitionInfo> splits;
  private final SupportsInternalIcebergTable plugin;
  private final List<DirListInputSplitProto.DirListInputSplit> dirListInputSplits;

  private FileSystem fs;
  int current = 0;

  public DirListingRecordReaderIterator(OperatorContext context, FragmentExecutionContext fragmentExecContext, DirListingSubScan config) throws ExecutionSetupException {
    this.context = context;
    this.config = config;
    splits = config.getSplits();
    plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    dirListInputSplits = new ArrayList<>();
    populateDirListInputSplit(dirListInputSplits, splits);
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RuntimeFilter> getRuntimeFilters() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void produceFromBuffered(boolean toProduce) {
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public boolean hasNext() {
    return current < dirListInputSplits.size();
  }

  @Override
  public RecordReader next() {
    DirListInputSplitProto.DirListInputSplit dirListInputSplit = dirListInputSplits.get(current);
    List<PartitionProtobuf.PartitionValue> partitionValueList = splits.get(current).getPartitionInfo().getValuesList();
    current++;

    if (fs == null) {
      try {
        fs = plugin.createFSWithoutHDFSCache(dirListInputSplit.getRootPath(), config.getProps().getUserName(), context);
      }
      catch (IOException e) {
        throw UserException.ioExceptionError(e).buildSilently();
      }
    }
    return plugin.createDirListRecordReader(context, fs, dirListInputSplit, config.isAllowRecursiveListing(), config.getTableSchema(), partitionValueList);
  }

  private static void populateDirListInputSplit(List<DirListInputSplitProto.DirListInputSplit> dirListInputSplits, List<SplitAndPartitionInfo> splits) throws ExecutionSetupException {
    for (SplitAndPartitionInfo split : splits) {
      try {
        DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.parseFrom(split.getDatasetSplitInfo().getExtendedProperty().toByteArray());
        dirListInputSplits.add(dirListInputSplit);
      } catch (InvalidProtocolBufferException e) {
        throw new ExecutionSetupException(e);
      }
    }
  }
}
