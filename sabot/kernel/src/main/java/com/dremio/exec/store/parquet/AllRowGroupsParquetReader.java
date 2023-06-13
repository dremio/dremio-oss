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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

/**
 * A Parquet reader implementation which reads all row groups in a Parquet file.  This can be used in situations
 * where it is not possible to process the file in splits, such as with Iceberg delete files.
 */
public class AllRowGroupsParquetReader implements RecordReader {

  private final OperatorContext context;
  private final Path path;
  private final List<String> dataset;
  private final FileSystem fs;
  private final InputStreamProviderFactory inputStreamProviderFactory;
  private final ParquetReaderFactory parquetReaderFactory;
  private final BatchSchema schema;
  private final ParquetScanProjectedColumns projectedColumns;
  private final ParquetFilters filters;
  private final ParquetReaderOptions parquetReaderOptions;
  private final Queue<InputStreamProvider> inputStreamProviderPrefetchQueue = new ArrayDeque<>();

  private FileAttributes fileAttributes;
  private MutableParquetMetadata footer;
  private int currentRowGroupIndex;
  private int prefetchRowGroupIndex;
  private InputStreamProvider currentInputStreamProvider;
  private RecordReader currentReader;
  private OutputMutator outputMutator;

  public AllRowGroupsParquetReader(
    OperatorContext context,
    Path path,
    List<String> dataset,
    FileSystem fs,
    InputStreamProviderFactory inputStreamProviderFactory,
    ParquetReaderFactory parquetReaderFactory,
    BatchSchema schema,
    ParquetScanProjectedColumns projectedColumns,
    ParquetFilters filters,
    ParquetReaderOptions parquetReaderOptions) {

    this.context = context;
    this.path = fs.supportsPathsWithScheme() ? path : Path.of(Path.getContainerSpecificRelativePath(path));
    this.dataset = dataset;
    this.inputStreamProviderFactory = inputStreamProviderFactory;
    this.parquetReaderFactory = parquetReaderFactory;
    this.fs = fs;
    this.schema = schema;
    this.projectedColumns = projectedColumns;
    this.filters = filters;
    this.parquetReaderOptions = parquetReaderOptions;
    this.currentRowGroupIndex = -1;
    this.prefetchRowGroupIndex = 0;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    outputMutator = output;
    initFirstInputStreamProvider();
    prefetchInputStreamProviders();
    advanceToNextRowGroup();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    Preconditions.checkNotNull(currentReader);
    currentReader.allocate(vectorMap);
  }

  @Override
  public int next() {
    int records;
    while ((records = currentReader.next()) == 0) {
      if (!advanceToNextRowGroup()) {
        break;
      }
    }

    return records;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(inputStreamProviderPrefetchQueue);
    AutoCloseables.close(currentInputStreamProvider, currentReader, filters);
  }

  @Override
  public String getFilePath() {
    return path.toString();
  }

  private boolean advanceToNextRowGroup() {
    Preconditions.checkNotNull(footer);
    Preconditions.checkNotNull(fileAttributes);

    if (currentRowGroupIndex >= 0) {
      footer.removeRowGroupInformation(currentRowGroupIndex);
    }
    currentRowGroupIndex++;
    if (currentRowGroupIndex >= footer.getBlocks().size()) {
      return false;
    }

    advanceToNextInputStreamProvider();

    ParquetProtobuf.ParquetDatasetSplitScanXAttr readEntry = ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
      .setPath(fileAttributes.getPath().toString())
      .setStart(0)
      .setLength(fileAttributes.size())
      .setFileLength(fileAttributes.size())
      .setLastModificationTime(fileAttributes.lastModifiedTime().toMillis())
      .setRowGroupIndex(currentRowGroupIndex)
      .build();

    SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
      .noSchemaLearning(schema)
      .readInt96AsTimeStamp(parquetReaderOptions.isReadInt96AsTimestampEnabled())
      .dateCorruptionStatus(ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION)
      .mapDataTypeEnabled(context.getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
      .build();

    UnifiedParquetReader reader = new UnifiedParquetReader(
      context,
      parquetReaderFactory,
      schema,
      projectedColumns,
      null,
      filters,
      parquetReaderFactory.newFilterCreator(context, ParquetReaderFactory.ManagedSchemaType.ICEBERG, null,
        context.getAllocator()),
      ParquetDictionaryConvertor.DEFAULT,
      readEntry,
      fs,
      footer,
      null,
      schemaHelper,
      parquetReaderOptions.isVectorizationEnabled(),
      parquetReaderOptions.isDetailedTracingEnabled(),
      false,
      currentInputStreamProvider,
      new ArrayList<>(),
      false);
    reader.setIgnoreSchemaLearning(true);

    RecordReader last = currentReader;
    currentReader = reader;
    // currentReader now owns currentInputStreamProvider
    currentInputStreamProvider = null;
    AutoCloseables.close(RuntimeException.class, last);

    try {
      currentReader.setup(outputMutator);
    } catch (ExecutionSetupException ex) {
      throw new RuntimeException(ex);
    }

    return true;
  }

  private void initFirstInputStreamProvider() {
    if (currentInputStreamProvider == null) {
      currentInputStreamProvider = createInputStreamProvider(0);
    }
    if (footer == null) {
      try {
        footer = currentInputStreamProvider.getFooter();
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }
  }

  private void advanceToNextInputStreamProvider() {
    // provider for first row group is created in initFirstInputStreamProvider
    if (currentRowGroupIndex == 0) {
      return;
    }

    if (currentRowGroupIndex > 0) {
      if (!inputStreamProviderPrefetchQueue.isEmpty()) {
        currentInputStreamProvider = inputStreamProviderPrefetchQueue.remove();
        prefetchInputStreamProviders();
      } else {
        currentInputStreamProvider = createInputStreamProvider(currentRowGroupIndex);
      }
    }
  }

  private void prefetchInputStreamProviders() {
    Preconditions.checkNotNull(footer);

    if (parquetReaderOptions.isPrefetchingEnabled()) {
      // the first row group provider is already created, no need to prefetch
      if (prefetchRowGroupIndex == 0) {
        prefetchRowGroupIndex++;
      }

      int rowGroupCount = footer.getBlocks().size();
      while (prefetchRowGroupIndex < rowGroupCount &&
        inputStreamProviderPrefetchQueue.size() < parquetReaderOptions.getNumSplitsToPrefetch()) {
        inputStreamProviderPrefetchQueue.add(createInputStreamProvider(prefetchRowGroupIndex++));
      }
    }
  }

  private InputStreamProvider createInputStreamProvider(int rowGroupIndex) {
    try {
      if (fileAttributes == null) {
        fileAttributes = fs.getFileAttributes(path);
      }

      // footer and currentInputStreamProvider will be null if we're creating the provider for the first row group
      return inputStreamProviderFactory.create(
        fs,
        context,
        path,
        fileAttributes.size(),
        fileAttributes.size(),
        projectedColumns,
        footer,
        currentInputStreamProvider,
        f -> rowGroupIndex,
        false,
        dataset,
        fileAttributes.lastModifiedTime().toMillis(),
        false,
        true, filters, parquetReaderFactory.newFilterCreator(context, ParquetReaderFactory.ManagedSchemaType.ICEBERG, null, context.getAllocator()));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
}
