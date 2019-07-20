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
package com.dremio.exec.store.parquet;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.ClassPathFileSystem;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.BaseFormatPlugin;
import com.dremio.exec.store.dfs.BasicFormatMatcher;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.MagicString;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.exec.util.GlobalDictionaryBuilder;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.DictionaryEncodedColumns;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.writer.WriterOperator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.base.Throwables;
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
  private final String name;
  private FileSystemPlugin fsPlugin;

  public ParquetFormatPlugin(String name, SabotContext context, FileSystemPlugin fsPlugin){
    this(name, context, new ParquetFormatConfig(), fsPlugin);
    this.fsPlugin = fsPlugin;
  }

  public ParquetFormatPlugin(String name, SabotContext context, ParquetFormatConfig formatConfig, FileSystemPlugin fsPlugin){
    super(context, fsPlugin);
    this.context = context;
    this.config = formatConfig;
    this.formatMatcher = new ParquetFormatMatcher(this, config);
    this.name = name == null ? DEFAULT_NAME : name;
    this.fsPlugin = fsPlugin;
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
  public AbstractWriter getWriter(PhysicalOperator child, String location, FileSystemPlugin plugin, WriterOptions options, OpProps props) throws IOException {
    return new ParquetWriter(props, child, location, options, plugin, this);
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
    return new ParquetGroupScanUtils(userName, selection, plugin, this, selection.getSelectionRoot(), columns, schema,
      globalDictionaryColumns, null, context.getOptionManager());
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystemWrapper fs, FileStatus status) throws ExecutionSetupException {
    try {
      return new PreviewReader(context, fs, status);
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  /**
   * A parquet reader that combines individual row groups into a single stream for preview purposes.
   */
  private class PreviewReader implements RecordReader {

    private final OperatorContext context;
    private final FileSystemWrapper fs;
    private final FileStatus status;
    private final ParquetMetadata footer;
    private final ParquetReaderUtility.DateCorruptionStatus dateStatus;
    private final SchemaDerivationHelper schemaHelper;
    private final InputStreamProvider streamProvider;

    private int currentIndex = -1;
    private OutputMutator output;
    private RecordReader current;

    public PreviewReader(
        OperatorContext context,
        FileSystemWrapper fs,
        FileStatus status
        ) throws IOException {
      super();
      this.context = context;
      this.fs = fs;
      this.status = status;
      this.streamProvider = new SingleStreamProvider(fs, status.getPath(), status.getLen(), null, false);
      this.footer = this.streamProvider.getFooter();
      boolean autoCorrectCorruptDates = context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR) &&
        getConfig().autoCorrectCorruptDates;
      this.dateStatus = ParquetReaderUtility.detectCorruptDates(footer, GroupScan.ALL_COLUMNS, autoCorrectCorruptDates);
      this.schemaHelper = SchemaDerivationHelper.builder()
          .readInt96AsTimeStamp(context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR))
          .dateCorruptionStatus(dateStatus)
          .build();
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(current, streamProvider);
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
      this.output = output;
      nextReader();
    }

    private void nextReader() {
      AutoCloseables.closeNoChecked(current);
      current = null;

      currentIndex++;

      /* Files with N rowgroups have a ParquetRowiseReader created for every rowgroup.
         Empty files (with block size is 0) must have a single ParquetRowiseReader created
         to get the columns in the files to generate schema, otherwise we cannot get schema
         from empty files in preview.
       */
      if (currentIndex >= footer.getBlocks().size() && currentIndex > 0) {
        return;
      }

      current = new ParquetRowiseReader(
          context,
          footer,
          currentIndex,
          status.getPath().toString(),
          GroupScan.ALL_COLUMNS,
          fs,
          schemaHelper,
          streamProvider
          );
      try {
        current.setup(output);
      } catch (ExecutionSetupException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
      current.allocate(vectorMap);
    }

    @Override
    public int next() {
      if(current == null) {
        return 0;
      }

      int records = current.next();
      while(records == 0) {
        nextReader();
        if(current == null) {
          return 0;
        }
        records = current.next();
      }

      return records;
    }
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

  @Override
  public SabotContext getContext() {
    return context;
  }

  @Override
  public FormatMatcher getMatcher() {
    return formatMatcher;
  }


  //       ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(footer, columns, autoCorrectCorruptDates);

  private static class ParquetFormatMatcher extends BasicFormatMatcher {

    private final ParquetFormatConfig formatConfig;

    public ParquetFormatMatcher(ParquetFormatPlugin plugin, ParquetFormatConfig formatConfig) {
      super(plugin, PATTERNS, MAGIC_STRINGS);
      this.formatConfig = formatConfig;
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
        root = GlobalDictionaryBuilder.getDictionaryVersionedRootPath(fs, root, version);
        for (Field field : batchSchema.getFields()) {
          final Path dictionaryFilePath = GlobalDictionaryBuilder.getDictionaryFile(fs, root, field.getName());
          if (dictionaryFilePath != null) {
            columns.add(field.getName());
          }
        }
        if (!columns.isEmpty()) {
          return DictionaryEncodedColumns.newBuilder()
              .setVersion(version)
              .setRootPath(root.toString())
              .addAllColumns(columns)
              .build();
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
  public FileDatasetHandle getDatasetAccessor(DatasetType type, PreviousDatasetInfo previousInfo, FileSystemWrapper fs,
      FileSelection fileSelection, FileSystemPlugin fsPlugin, NamespaceKey tableSchemaPath, FileUpdateKey updateKey,
      int maxLeafColumns) {
    return new ParquetFormatDatasetAccessor(type, fs, fileSelection, fsPlugin, tableSchemaPath, updateKey, this, previousInfo, maxLeafColumns);
  }
}
