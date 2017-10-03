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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.exec.store.dfs.BaseFormatPlugin;
import com.dremio.exec.store.dfs.BasicFormatMatcher;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemDatasetAccessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.easy.EasyFormatDatasetAccessor;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.writer.WriterOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.google.common.collect.ImmutableSet;

public abstract class EasyFormatPlugin<T extends FormatPluginConfig> extends BaseFormatPlugin {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  private final BasicFormatMatcher matcher;
  private final SabotContext context;
  private final boolean readable;
  private final boolean writable;
  private final boolean blockSplittable;
  private final StoragePluginConfig storageConfig;
  protected final FormatPluginConfig formatConfig;
  private final String name;
  private final boolean compressible;

  protected EasyFormatPlugin(String name, SabotContext context,
      StoragePluginConfig storageConfig, T formatConfig, boolean readable, boolean writable, boolean blockSplittable,
      boolean compressible, List<String> extensions, String defaultName, FileSystemPlugin fsPlugin) {
    super(context, fsPlugin);
    this.matcher = new BasicFormatMatcher(this, extensions, compressible);
    this.readable = readable;
    this.writable = writable;
    this.context = context;
    this.blockSplittable = blockSplittable;
    this.compressible = compressible;
    this.storageConfig = storageConfig;
    this.formatConfig = formatConfig;
    this.name = name == null ? defaultName : name;
  }

  @Override
  public String getName() {
    return name;
  }

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
      FileSystemWrapper dfs,
      EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns) throws ExecutionSetupException;

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
  public AbstractWriter getWriter(PhysicalOperator child, String userName, String location, FileSystemPlugin plugin,
      WriterOptions options) throws IOException {
    return new EasyWriter(child, userName, location, options, plugin, this);
  }

  public EasyGroupScanUtils getGroupScan(
    String userName,
    FileSystemPlugin plugin,
    FileSelection selection,
    List<SchemaPath> columns) throws IOException {
    return new EasyGroupScanUtils(userName, selection, plugin, this, columns, selection.getSelectionRoot(), false);
  }

  @Override
  public FormatPluginConfig getConfig() {
    return formatConfig;
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
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

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of();
  }

  public abstract int getReaderOperatorType();
  public abstract int getWriterOperatorType();

  @Override
  public FileSystemDatasetAccessor getDatasetAccessor(DatasetConfig oldConfig, FileSystemWrapper fs, FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, String tableName, FileUpdateKey updateKey) {
    return new EasyFormatDatasetAccessor(fs, fileSelection, fsPlugin, tableSchemaPath, tableName, updateKey, this, oldConfig);
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
