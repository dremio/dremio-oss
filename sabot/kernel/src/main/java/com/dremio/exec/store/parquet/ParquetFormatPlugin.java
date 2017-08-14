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
package com.dremio.exec.store.parquet;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ClassPathFileSystem;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.exec.store.dfs.BaseFormatPlugin;
import com.dremio.exec.store.dfs.BasicFormatMatcher;
import com.dremio.exec.store.dfs.DefaultPathFilter;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemDatasetAccessor;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.MagicString;
import com.dremio.exec.util.GlobalDictionaryBuilder;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.writer.WriterOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.DictionaryEncodedColumns;
import com.dremio.service.namespace.file.proto.FileUpdateKey;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class ParquetFormatPlugin extends BaseFormatPlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFormatPlugin.class);

  public static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final String DEFAULT_NAME = "parquet";

  private static final List<Pattern> PATTERNS = Lists.newArrayList(
      Pattern.compile(".*\\.parquet$"),
      Pattern.compile(".*/" + ParquetFileWriter.PARQUET_METADATA_FILE));
  private static final List<MagicString> MAGIC_STRINGS = Lists.newArrayList(new MagicString(0, ParquetFileWriter.MAGIC));

  private final SabotContext context;
  private final ParquetFormatMatcher formatMatcher;
  private final ParquetFormatConfig config;
  private final StoragePluginConfig storageConfig;
  private final String name;
  private FileSystemPlugin fsPlugin;

  public ParquetFormatPlugin(String name, SabotContext context, StoragePluginConfig storageConfig, FileSystemPlugin fsPlugin){
    this(name, context, storageConfig, new ParquetFormatConfig(), fsPlugin);
    this.fsPlugin = (FileSystemPlugin) fsPlugin;
  }

  public ParquetFormatPlugin(String name, SabotContext context, StoragePluginConfig storageConfig,
      ParquetFormatConfig formatConfig, FileSystemPlugin fsPlugin){
    super(context, fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.formatMatcher = new ParquetFormatMatcher(this, config);
    this.storageConfig = storageConfig;
    this.name = name == null ? DEFAULT_NAME : name;
    this.fsPlugin = (FileSystemPlugin) fsPlugin;
  }

  @Override
  public ParquetFormatConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of();
  }

  @Override
  public AbstractWriter getWriter(PhysicalOperator child, String userName, String location, FileSystemPlugin plugin,
      WriterOptions options) throws IOException {
    return new ParquetWriter(child, userName, location, options, plugin, this);
  }

  public RecordWriter getRecordWriter(OperatorContext context, ParquetWriter writer) throws IOException, OutOfMemoryException {
    return new ParquetRecordWriter(context, writer, config);
  }

  public WriterOperator getWriterBatch(OperatorContext context, ParquetWriter writer)
          throws ExecutionSetupException {
    try {
      return new WriterOperator(context, writer.getOptions(), getRecordWriter(context, writer));
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create the WriterRecordBatch. %s", e.getMessage()), e);
    }
  }

  public ParquetGroupScanUtils getGroupScan(
      String userName,
      FileSystemPlugin plugin,
      FileSelection selection,
      List<String> tableSchemaPath,
      List<SchemaPath> columns,
      BatchSchema schema,
      Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns) throws IOException {
    return new ParquetGroupScanUtils(userName, selection, plugin, this, selection.selectionRoot, columns, schema,
      globalDictionaryColumns, null);
  }

  @Override
  public StoragePluginConfig getStorageConfig() {
    return storageConfig;
  }

  @Override
  public String getName(){
    return name;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public boolean supportsAutoPartitioning() {
    return true;
  }

  public SabotContext getContext() {
    return context;
  }

  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }


  //       ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(footer, columns, autoCorrectCorruptDates);

  private static class ParquetFormatMatcher extends BasicFormatMatcher{

    private final ParquetFormatConfig formatConfig;

    public ParquetFormatMatcher(ParquetFormatPlugin plugin, ParquetFormatConfig formatConfig) {
      super(plugin, PATTERNS, MAGIC_STRINGS);
      this.formatConfig = formatConfig;
    }

    @Override
    public boolean matches(FileSystemWrapper fs, FileStatus fileStatus, CompressionCodecFactory codecFactory) throws IOException {
      if (fileStatus.isDirectory() && isDirReadable(fs, fileStatus, codecFactory)){
        return true;
      }
      return super.isFileReadable(fs, fileStatus, codecFactory);
    }

    private boolean isDirReadable(FileSystemWrapper fs, FileStatus dir, CompressionCodecFactory codecFactory) {
      try {
        PathFilter filter = new DefaultPathFilter();

        FileStatus[] files = fs.listStatus(dir.getPath(), filter);
        if (files.length == 0) {
          return false;
        }
        return super.isFileReadable(fs, files[0], codecFactory);
      } catch (IOException e) {
        logger.info("Failure while attempting to check for Parquet metadata file.", e);
        return false;
      }
    }
  }

  /**
   * Check if any columns are dictionary encoded by looking up for .dict files
   * @param fs filesystem
   * @param selectionRoot root of table
   * @param batchSchema schema for this parquet table
   */
  public static DictionaryEncodedColumns scanForDictionaryEncodedColumns(FileSystem fs, String selectionRoot, BatchSchema batchSchema) {
    try {
      Path root = new Path(selectionRoot);
      if (!fs.isDirectory(root)) {
        root = root.getParent();
      }
      long version = GlobalDictionaryBuilder.getDictionaryVersion(fs, root);
      if (version != -1) {
        final List<String> columns = Lists.newArrayList();
        final DictionaryEncodedColumns dictionaryEncodedColumns = new DictionaryEncodedColumns();
        root = GlobalDictionaryBuilder.getDictionaryVersionedRootPath(fs, root, version);
        for (Field field : batchSchema.getFields()) {
          final Path dictionaryFilePath = GlobalDictionaryBuilder.getDictionaryFile(fs, root, field.getName());
          if (dictionaryFilePath != null) {
            columns.add(field.getName());
          }
        }
        if (!columns.isEmpty()) {
          dictionaryEncodedColumns.setVersion(version);
          dictionaryEncodedColumns.setRootPath(root.toString());
          dictionaryEncodedColumns.setColumnsList(columns);
          return dictionaryEncodedColumns;
        }
      }
    } catch (UnsupportedOperationException e) { // class path based filesystem doesn't support listing
      if (!ClassPathFileSystem.SCHEME.equals(fs.getUri().getScheme())) {
        throw e;
      }
    } catch (IOException ioe) {
      logger.warn(format("Failed to scan directory %s for global dictionary", selectionRoot), ioe);
    }
    return null;
  }

  public static VectorContainer loadDictionary(FileSystem fs, Path dictionaryFilePath, BufferAllocator bufferAllocator) throws IOException {
    return GlobalDictionaryBuilder.readDictionary(fs, dictionaryFilePath, bufferAllocator);
  }

  @Override
  public FileSystemDatasetAccessor getDatasetAccessor(DatasetConfig oldConfig, FileSystemWrapper fs, FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, String tableName, FileUpdateKey updateKey) {
    return new ParquetFormatDatasetAccessor(oldConfig, fs, fileSelection, fsPlugin, tableSchemaPath, tableName, updateKey, this);
  }
}
