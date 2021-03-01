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
package com.dremio.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.BaseFormatPlugin;
import com.dremio.exec.store.dfs.BasicFormatMatcher;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.easy.EasyFormatDatasetAccessor;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.dremio.sabot.op.writer.WriterOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> extends BaseFormatPlugin {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final SabotContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  protected final FormatPluginConfig formatConfig;
  private final String name;
  private final boolean compressible;

  protected EasyFormatPlugin(String name, SabotContext context,
      T formatConfig, boolean readable, boolean writable, boolean blockSplittable,
      boolean compressible, List<String> extensions, String defaultName, FileSystemPlugin<?> fsPlugin) {
    super(context, fsPlugin);
    this.matcher = new BasicFormatMatcher(this, extensions, compressible);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.compressible = compressible;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SabotContext getContext() {
    return context;
  }

  public abstract boolean supportsPushDown();

  /**
   * Whether or not you can split the format based on blocks within file boundaries. If not, the simple format engine will
   * only split on file boundaries.
   *
   * @return True if splittable.
   */
  public boolean isBlockSplittable() {
    return blockSplittable;
  }

  /** Method indicates whether or not this format could also be in a compression container (for example: csv.gz versus csv).
   * If this format uses its own internal compression scheme, such as Parquet does, then this should return false.
   */
  public boolean isCompressible() {
    return compressible;
  }

  public abstract RecordReader getRecordReader(
      OperatorContext context,
      FileSystem dfs,
      EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns) throws ExecutionSetupException;

  public RecordReader getRecordReader(
          OperatorContext context,
          FileSystem dfs,
          SplitAndPartitionInfo split,
          EasyDatasetSplitXAttr splitAttributes,
          List<SchemaPath> columns,
          FragmentExecutionContext fec,
          EasySubScan config) throws ExecutionSetupException {
    return getRecordReader(context, dfs, splitAttributes, columns);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, FileAttributes attributes) throws ExecutionSetupException {
    EasyDatasetSplitXAttr attr = EasyDatasetSplitXAttr.newBuilder()
        .setPath(attributes.getPath().toString())
        .setStart(0L)
        .setLength(attributes.size())
        .build();

    return getRecordReader(context, dfs, attr, GroupScan.ALL_COLUMNS);
  }

  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException{
    throw new UnsupportedOperationException("unimplemented");
  }

  public WriterOperator getWriterBatch(OperatorContext context, EasyWriter writer)
      throws ExecutionSetupException {
    try {
      return new WriterOperator(context, writer.getOptions(), getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String location, FileSystemPlugin<?> plugin, WriterOptions options, OpProps props) throws IOException{
    return new EasyWriter(props, child, props.getUserName(), location, options, plugin, this);
  }

  public EasyGroupScanUtils getGroupScan(
    String userName,
    FileSystemPlugin<?> plugin,
    FileSelection selection,
    List<SchemaPath> columns) throws IOException {
    return new EasyGroupScanUtils(userName, selection, plugin, this, columns, selection.getSelectionRoot(), false);
  }

  @Override
  public FormatPluginConfig getConfig() {
    return formatConfig;
  }

  @Override
  public boolean supportsRead() {
    return readable;
  }

  @Override
  public boolean supportsWrite() {
    return writable;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return matcher;
  }

  public abstract int getReaderOperatorType();
  public abstract int getWriterOperatorType();

  @Override
  public FileDatasetHandle getDatasetAccessor(DatasetType type, PreviousDatasetInfo previousInfo, FileSystem fs,
      FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, FileUpdateKey updateKey,
      int maxLeafColumns) {
    return new EasyFormatDatasetAccessor(type, fs, fileSelection, fsPlugin, tableSchemaPath, updateKey, this, previousInfo, maxLeafColumns);
  }

  protected ScanStats getScanStats(final EasyGroupScanUtils scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }

    final long estRowCount = data / 100;
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, estRowCount, data);
  }

}
