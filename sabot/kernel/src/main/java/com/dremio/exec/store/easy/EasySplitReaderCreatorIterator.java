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
package com.dremio.exec.store.easy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.EasyScanTableFunctionContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.EasyRecordReaderIterator;
import com.dremio.exec.store.dfs.EmptySplitReaderCreator;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.store.parquet.SplitReaderCreatorIterator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;

import io.protostuff.ByteString;

/**
 * An object that holds the relevant creation fields so we don't have to have an really long lambda.
 */
public class EasySplitReaderCreatorIterator implements SplitReaderCreatorIterator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasySplitReaderCreatorIterator.class);
  protected final SupportsIcebergRootPointer plugin;
  protected final FileSystem fs;
  private final boolean prefetchReader;
  protected final OperatorContext context;
  private final FragmentExecutionContext fragmentExecutionContext;
  protected final List<List<String>> tablePath;
  protected List<SchemaPath> columns;
  private List<SplitAndPartitionInfo> inputSplits;

  protected Iterator<ParquetProtobuf.ParquetDatasetSplitScanXAttr> rowGroupSplitIterator;
  protected Iterator<SplitAndPartitionInfo> splitAndPartitionInfoIterator; // used only if the main splits are row group based

  private SplitReaderCreator first;
  protected SplitAndPartitionInfo currentSplitInfo;
  private InputStreamProvider lastInputStreamProvider;
  private final List<RuntimeFilter> runtimeFilters = new ArrayList<>();

  protected final FormatPlugin formatPlugin;
  protected final ExtendedFormatOptions extendedFormatOptions;
  private final ByteString extendedProperty;

  public EasySplitReaderCreatorIterator(FragmentExecutionContext fragmentExecContext, final OperatorContext context, OpProps props, final TableFunctionConfig config, boolean produceFromBufferedSplits) throws ExecutionSetupException {
    this.inputSplits = null;
    this.tablePath = config.getFunctionContext().getTablePath();
    this.columns = config.getFunctionContext().getColumns();
    this.context = context;
    this.prefetchReader = context.getOptions().getOption(ExecConstants.PREFETCH_READER);
    this.plugin = fragmentExecContext.getStoragePlugin(config.getFunctionContext().getPluginId());
    this.extendedFormatOptions = ((EasyScanTableFunctionContext) config.getFunctionContext()).getExtendedFormatOptions();
    this.extendedProperty = config.getFunctionContext().getExtendedProperty();
    try {
        this.fs = plugin.createFS(config.getFunctionContext().getFormatSettings().getLocation(),
                props.getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot access plugin filesystem", e);
    }
    this.fragmentExecutionContext = fragmentExecContext;
    FileConfig formatSettings = config.getFunctionContext().getFormatSettings();
    this.formatPlugin = plugin.getFormatPlugin(PhysicalDatasetUtils.toFormatPlugin(formatSettings, Collections.<String>emptyList()));

    processSplits();
    if (prefetchReader) {
      initSplits(null);
    }
  }

  private void processSplits() {
    if (inputSplits == null) {
      rowGroupSplitIterator = Collections.emptyIterator();
      return;
    }

    rowGroupSplitIterator = Collections.emptyIterator();
    splitAndPartitionInfoIterator = inputSplits.iterator();
    currentSplitInfo = splitAndPartitionInfoIterator.hasNext() ? splitAndPartitionInfoIterator.next() : null;
  }

  public RecordReaderIterator getRecordReaderIterator() {
    return new EasyRecordReaderIterator(this);
  }

  public void addSplits(List<SplitAndPartitionInfo> splits) {
    this.inputSplits = splits;
    processSplits();
    initSplits(null);
  }

  private void initSplits(SplitReaderCreator curr) {
      if (currentSplitInfo != null) {
        first = createSplitReaderCreator();
        curr = first;
     }
      while (currentSplitInfo  != null) {
        SplitReaderCreator creator = createSplitReaderCreator();
        curr.setNext(creator);
        curr = creator;
    }
  }

  @Override
  public List<RuntimeFilter> getRuntimeFilters() {
    return runtimeFilters;
  }

  @Override
  public boolean hasNext() {
    return rowGroupSplitIterator.hasNext() || (first != null);
  }

  @Override
  public SplitReaderCreator next() {
    Preconditions.checkArgument(hasNext());
    if (first == null) {
      Preconditions.checkArgument(!rowGroupSplitIterator.hasNext());
      return new EmptySplitReaderCreator(null, lastInputStreamProvider);
    }
    SplitReaderCreator curr = first;
    first = first.getNext();
    return curr;
  }

  protected SplitReaderCreator createSplitReaderCreator() {

    SplitReaderCreator creator = new EasySplitReaderCreator(context,
      fs, currentSplitInfo, tablePath, columns, formatPlugin,
      ((FileSystemPlugin<?>) this.plugin).getCompressionCodecFactory(), extendedFormatOptions, extendedProperty);
    if (splitAndPartitionInfoIterator.hasNext()) {
      currentSplitInfo = splitAndPartitionInfoIterator.next();
    } else {
      Preconditions.checkArgument(!rowGroupSplitIterator.hasNext());
      currentSplitInfo = null;
    }
    return creator;
  }

  public void setLastInputStreamProvider(InputStreamProvider lastInputStreamProvider) {
    this.lastInputStreamProvider = lastInputStreamProvider;
  }

  @Override
  public void close() throws Exception {
    List<SplitReaderCreator> remainingCreators = new ArrayList<>();
    SplitReaderCreator curr = first;
    while (curr != null) {
      remainingCreators.add(curr);
      curr = curr.getNext();
    }
    com.dremio.common.AutoCloseables.close(remainingCreators);
  }
}
