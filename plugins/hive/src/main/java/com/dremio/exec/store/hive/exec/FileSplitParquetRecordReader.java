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
package com.dremio.exec.store.hive.exec;

import static com.dremio.exec.store.parquet.ParquetOperatorCreator.PREFETCH_READER;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.AsyncStreamConf;
import com.dremio.exec.store.dfs.EmptySplitReaderCreator;
import com.dremio.exec.store.dfs.PrefetchingIterator;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.hive.ContextClassLoaderSwapper;
import com.dremio.exec.store.hive.BaseHiveStoragePlugin;
import com.dremio.exec.store.hive.HiveAsyncStreamConf;
import com.dremio.exec.store.hive.exec.dfs.DremioHadoopFileSystemWrapper;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.ManagedSchema;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.exec.util.BatchSchemaField;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A {@link RecordReader} implementation that takes a {@link FileSplit} and
 * wraps one or more {@link UnifiedParquetReader}s (one for each row groups in {@link FileSplit})
 * Will throw error if fields being read have incompatible types in file and hive table
 */
public class FileSplitParquetRecordReader implements RecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSplitParquetRecordReader.class);
  private final OperatorContext oContext;
  private final BatchSchema tableSchema;
  private final List<SchemaPath> columnsToRead;
  private final List<ParquetFilterCondition> conditions;
  private final FileSplit fileSplit;
  private final JobConf jobConf;
  final Collection<List<String>> referencedTables;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final BatchSchema outputSchema;
  private final ParquetReaderFactory readerFactory;
  private final UserGroupInformation readerUgi;
  private final BaseHiveStoragePlugin hiveStoragePlugin;
  private final InputStreamProviderFactory inputStreamProviderFactory;
  private InputStreamProvider inputStreamProviderOfFirstRowGroup;
  private final List<Integer> rowGroupNums;
  private ParquetMetadata footer;
  private FileSystem fs;
  private long fileLength;
  private boolean readFullFile;
  private org.apache.hadoop.fs.Path filePath;
  private final Function<ParquetMetadata, Integer> rowGroupIndexProvider;
  private final Consumer<ParquetMetadata> populateRowGroupNums;
  private FileAttributes fileAttributes;
  private boolean isAsyncEnabled;

  private RecordReader currentReader;
  private final ManagedSchema managedSchema;
  private Iterator<RecordReader> innerReadersIter;

  private FileSplitParquetRecordReader nextFileSplitReader;

  public FileSplitParquetRecordReader(
    final BaseHiveStoragePlugin hiveStoragePlugin,
    final OperatorContext oContext,
    final ParquetReaderFactory readerFactory,
    final BatchSchema tableSchema,
    final List<SchemaPath> columnsToRead,
    final List<ParquetFilterCondition> conditions,
    final FileSplit fileSplit,
    final JobConf jobConf,
    final Collection<List<String>> referencedTables,
    final boolean vectorize,
    final BatchSchema outputSchema,
    final boolean enableDetailedTracing,
    final UserGroupInformation readerUgi,
    final ManagedSchema managedSchema
  ) {
    this.hiveStoragePlugin = hiveStoragePlugin;
    this.oContext = oContext;
    this.tableSchema = tableSchema;
    this.columnsToRead = columnsToRead;
    this.conditions = conditions;
    this.fileSplit = fileSplit;
    filePath = new org.apache.hadoop.fs.Path(fileSplit.getPath().toUri());
    this.jobConf = jobConf;
    this.referencedTables = referencedTables;
    this.readerFactory = readerFactory;
    this.vectorize = vectorize;
    this.enableDetailedTracing = enableDetailedTracing;
    this.outputSchema = outputSchema;
    this.readerUgi = readerUgi;
    this.managedSchema = managedSchema;
    this.inputStreamProviderFactory = oContext.getConfig()
      .getInstance(InputStreamProviderFactory.KEY, InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT);
    rowGroupNums = new ArrayList<>();
    isAsyncEnabled = true;

    populateRowGroupNums = (f) -> {
      try {
        if (rowGroupNums.isEmpty()) { // make sure rowGroupNums is populated only once
          rowGroupNums.addAll(ParquetReaderUtility.getRowGroupNumbersFromFileSplit(fileSplit.getStart(), fileSplit.getLength(), f));
        }
      } catch (IOException e) {
        throw UserException.ioExceptionError(e)
          .buildSilently();
      }
    };

    rowGroupIndexProvider = (f) -> {
      populateRowGroupNums.accept(f);
      if (rowGroupNums.isEmpty()) {
        return -1;
      }
      return rowGroupNums.get(0);
    };

  }

  public void setNextFileSplitReader(FileSplitParquetRecordReader nextFileSplitReader) {
    this.nextFileSplitReader = nextFileSplitReader;
  }

  public List<ParquetFilterCondition> getFilterConditions() {
    return conditions;
  }

  public void createInputStreamProvider(Path lastPath, ParquetMetadata lastFooter) {
    if(inputStreamProviderOfFirstRowGroup != null) {
      return;
    }

    Path currentPath = Path.of(filePath.toUri());
    URI uri = currentPath.toURI();
    AsyncStreamConf cacheAndAsyncConf = HiveAsyncStreamConf.from(uri.getScheme(), jobConf,  oContext.getOptions());
    isAsyncEnabled = cacheAndAsyncConf.isAsyncEnabled();
    if (cacheAndAsyncConf.isAsyncEnabled()) {
      try {
        uri = AsyncReaderUtils.injectDremioConfigForAsyncRead(uri, jobConf);
        // update tracker variables to use the new URI.
        currentPath = Path.of(uri);
        filePath = new org.apache.hadoop.fs.Path(uri);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    ParquetMetadata knownFooter =  currentPath.equals(lastPath) ? lastFooter : null;

    try (ContextClassLoaderSwapper ccls = ContextClassLoaderSwapper.newInstance()) {
      org.apache.hadoop.fs.Path finalPath  = new org.apache.hadoop.fs.Path(uri);

      final PrivilegedExceptionAction<FileSystem> getFsAction =
        () -> hiveStoragePlugin.createFS(new DremioHadoopFileSystemWrapper(finalPath, jobConf, oContext.getStats(), cacheAndAsyncConf.isAsyncEnabled()),
          oContext, cacheAndAsyncConf);

      fs = readerUgi.doAs(getFsAction);

      fileAttributes = fs.getFileAttributes(currentPath);
      fileLength = fileAttributes.size();
      readFullFile = fileLength < oContext.getOptions()
        .getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD) &&
        ((float) columnsToRead.size()) / outputSchema.getFieldCount() > oContext.getOptions()
          .getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);
      logger.debug("file={}, readFullFile={},length={},threshold={},columns={},totalColumns={},ratio={},req ratio={}",
        filePath,
        readFullFile,
        fileLength,
        oContext.getOptions()
          .getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD),
        columnsToRead.size(),
        outputSchema.getFieldCount(),
        ((float) columnsToRead.size()) / outputSchema.getFieldCount(),
        oContext.getOptions()
          .getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO));

      final List<String> dataset = referencedTables == null || referencedTables.isEmpty() ? null : referencedTables.iterator()
        .next();
      inputStreamProviderOfFirstRowGroup = inputStreamProviderFactory.create(fs,
        oContext,
        currentPath,
        fileLength,
        fileSplit.getLength(), // max row group size possible
        ParquetScanProjectedColumns.fromSchemaPaths(columnsToRead),
        knownFooter,
        rowGroupIndexProvider,
        (a, b) -> {},
        readFullFile,
        dataset,
        fileAttributes.lastModifiedTime().toMillis()
      );
    } catch (Exception e) {
      // Close input stream provider in case of errors
      if (inputStreamProviderOfFirstRowGroup != null) {
        try {
          inputStreamProviderOfFirstRowGroup.close();
        } catch (Exception ignore) {
        }
      }
      if (e instanceof FileNotFoundException) {
        // the outer try-catch handles this.
        throw UserException.invalidMetadataError(e)
          .addContext("Parquet file not found")
          .addContext("File", fileSplit.getPath())
          .setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(referencedTables)))
          .build(logger); // better to have these messages in logs
      } else {
        throw UserException.resourceError(e).buildSilently();
      }
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    Preconditions.checkArgument(output instanceof SampleMutator, "Unexpected output mutator");
    try (ContextClassLoaderSwapper ccls = ContextClassLoaderSwapper.newInstance()) {

      // no-op except for the first split
      this.createInputStreamProvider(null, null);
      footer = inputStreamProviderOfFirstRowGroup.getFooter();

      // populate rowGroupNums in non-async case since default InputStreamProviderFactory doesn't use rowGroupIndexProvider
      // should be a no-op for async case
      populateRowGroupNums.accept(footer);

      final boolean autoCorrectCorruptDates = oContext.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);
      final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
        .readInt96AsTimeStamp(true)
        .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, columnsToRead, autoCorrectCorruptDates))
        .noSchemaLearning(outputSchema)
        .allowMixedDecimals(true)
        .limitListItems(true)
        .build();

      final Set<String> columnsToReadSet = columnsToRead.stream()
        .map(col -> col.getRootSegment()
          .getNameSegment()
          .getPath()
          .toLowerCase())
        .collect(Collectors.toSet());
      footer.getFileMetaData()
        .getSchema()
        .getFields()
        .stream()
        .filter(field -> columnsToReadSet.contains(field.getName()
          .toLowerCase()))
        .map(field -> ParquetTypeHelper.toField(field, schemaHelper))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(field -> output.addField(field, TypeHelper.getValueVectorClass(field)));

      ((SampleMutator) output).getContainer().buildSchema();
      output.getAndResetSchemaChanged();
      checkFieldTypesCompatibleWithHiveTable(output, tableSchema);

      oContext.getStats()
        .addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, rowGroupNums.size());

      RowGroupReaderCreatorFactory rowGroupReaderCreatorFactory = new RowGroupReaderCreatorFactory(output, schemaHelper);

      List<SplitReaderCreator> innerReaderCreators = Lists.newArrayList();

      if (rowGroupNums.isEmpty()) {
        innerReaderCreators.add(rowGroupReaderCreatorFactory.createEmptyReaderCreator(Path.of(filePath.toUri()), inputStreamProviderOfFirstRowGroup));

        oContext.getStats()
            .addLongStat(ScanOperator.Metric.NUM_HIVE_FILE_SPLITS_WITH_NO_ROWGROUPS, 1);

      } else {
        InputStreamProvider is = inputStreamProviderOfFirstRowGroup;
        for (int rowGroupNum : rowGroupNums) {
          ParquetDatasetSplitScanXAttr split = ParquetDatasetSplitScanXAttr.newBuilder()
            .setRowGroupIndex(rowGroupNum)
            .setPath(filePath.toString())
            .setStart(0L)
            .setLength(fileSplit.getLength()) // max row group size possible
            .setLastModificationTime(fileAttributes.lastModifiedTime()
              .toMillis())
            .build();
          innerReaderCreators.add(rowGroupReaderCreatorFactory.create(split, is));
          is = null; // inputStreamProvider is known only for first row group
        }

        SplitReaderCreator next = null;
        // set forward links
        for (int i = rowGroupNums.size() - 1; i > -1; i--) {
          SplitReaderCreator cur = innerReaderCreators.get(i);
          cur.setNext(next);
          next = cur;
        }

        if (rowGroupNums.size() > oContext.getStats().getLongStat(ScanOperator.Metric.MAX_ROW_GROUPS_IN_HIVE_FILE_SPLITS)) {
          oContext.getStats()
              .setLongStat(ScanOperator.Metric.MAX_ROW_GROUPS_IN_HIVE_FILE_SPLITS, rowGroupNums.size());
        }

      }

      innerReadersIter = new PrefetchingIterator<>(innerReaderCreators);

      currentReader = innerReadersIter.hasNext() ? innerReadersIter.next() : null;
    } catch (IOException e) {
      if (inputStreamProviderOfFirstRowGroup != null) {
        try {
          inputStreamProviderOfFirstRowGroup.close();
        } catch (Exception ignore) {
        }
      }
      throw new ExecutionSetupException("Failure during setup", e);
    } finally {
      fileAttributes = null;
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    if (currentReader == null) {
      return;
    }
    currentReader.allocate(vectorMap);
  }

  @Override
  public int next() {
    if (currentReader == null) {
      return 0;
    }

    while (currentReader != null) {
      int recordCount = currentReader.next();
      if (recordCount != 0) {
        return recordCount;
      }
      currentReader = innerReadersIter.hasNext() ? innerReadersIter.next() : null;
    }
    return 0;
  }

  @Override
  public void close() throws Exception {
    if ((conditions != null) && !conditions.isEmpty()) {
      if (conditions.get(0).isModifiedForPushdown()) {
        this.oContext.getStats().addLongStat(ScanOperator.Metric.NUM_FILTERS_MODIFIED, 1);
      }
    }

    try {
      // innerReadersIter will be null until this.setUp() is called
      // if innerReadersIter is null (which means this reader is closed before setUp())
      //    1. sync - inputStreamProviderOfFirstRowGroup will be null
      //    2. async - inputStreamProviderOfFirstRowGroup is initialized as part of async-prefetch, we need to close it here
      // if innerReadersIter is not null, closing innerReadersIter will close inputStreamProviderOfFirstRowGroup
      AutoCloseables.close(this.innerReadersIter == null ? inputStreamProviderOfFirstRowGroup : null,
              // null check on innerReadersIter not needed since 'instanceof' returns false if first arg is null
              innerReadersIter instanceof AutoCloseable ? (AutoCloseable) innerReadersIter : null);
    } finally {
      clearLocalFields();
    }
  }

  private void clearLocalFields() {
    fs = null;
    footer = null;
    innerReadersIter = null;
  }

  private boolean areFieldsCompatible(Field tableField, Field fileField) {
    Preconditions.checkArgument(fileField != null, "Invalid argument");

    // accept if there is no corresponding field in table
    if (tableField == null) {
      return true;
    }

    // field names have to be same
    if (!tableField.getName().equalsIgnoreCase(fileField.getName())) {
      return false;
    }

    // check the field type
    TypeProtos.MinorType fieldTypeInTable = CompleteType.fromField(tableField).toMinorType();
    TypeProtos.MinorType fieldTypeInFile = CompleteType.fromField(fileField).toMinorType();
    boolean compatible = TypeCastRules.isHiveCompatibleTypeChange(fieldTypeInFile, fieldTypeInTable);

    // if not compatible return
    if (!compatible) {
      return compatible;
    }

    // make sure all child types also are compatible
    Preconditions.checkState(fileField.getChildren() != null, "Invalid state");
    if (!fileField.getChildren().isEmpty()) {
      Preconditions.checkState(tableField.getChildren() != null, "Invalid state");
      Map<String, Field> tableFieldChildren = tableField.getChildren().stream().collect(
        Collectors.toMap(f ->  f.getName().toLowerCase(), f -> f)
      );

      for(Field child: fileField.getChildren()) {
        compatible = areFieldsCompatible(
          tableFieldChildren.getOrDefault(child.getName().toLowerCase(), null),
          child);

        // if any child is not compatible return
        if (!compatible) {
          return false;
        }
      }
    }

    // top level field and all its children are compatible
    return compatible;
  }

  private void checkFieldTypesCompatibleWithHiveTable(OutputMutator readerOutputMutator, BatchSchema tableSchema) {
    for (ValueVector fieldVector : readerOutputMutator.getVectors()) {
      Field fieldInFileSchema = fieldVector.getField();
      Optional<Field> fieldInTable = tableSchema.findFieldIgnoreCase(fieldInFileSchema.getName());

      if (!fieldInTable.isPresent()) {
        throw UserException.validationError()
          .message("Field [%s] not found in table schema %s", fieldInFileSchema.getName(),
            tableSchema.getFields())
          .buildSilently();
      }

      boolean compatible = areFieldsCompatible(fieldInTable.get(), fieldInFileSchema);
      if (!compatible) {
        BatchSchemaField batchSchemaFieldInTable = BatchSchemaField.fromField(fieldInTable.get());
        BatchSchemaField batchSchemaFieldInFile = BatchSchemaField.fromField(fieldInFileSchema);
        throw UserException.schemaChangeError().message("Field [%s] has incompatible types in file and table." +
            " Type in fileschema: [%s], type in tableschema: [%s]", fieldInFileSchema.getName(), batchSchemaFieldInFile, batchSchemaFieldInTable).buildSilently();
      }
    }
  }

  private class RowGroupReaderCreatorFactory {

    private final OutputMutator output;
    private final boolean prefetchReader;
    private final SchemaDerivationHelper schemaHelper;

    public HiveParquetRowGroupReaderCreator create(ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr,
                                                   InputStreamProvider inputStreamProvider) {
      return new HiveParquetRowGroupReaderCreator(splitXAttr, inputStreamProvider);
    }

    public EmptySplitReaderCreator createEmptyReaderCreator(Path path, InputStreamProvider inputStreamProvider) {
      return new EmptySplitReaderCreator(path, inputStreamProvider);
    }

    public RowGroupReaderCreatorFactory(OutputMutator output, SchemaDerivationHelper schemaHelper) {
      this.output = output;
      this.schemaHelper = schemaHelper;
      this.prefetchReader = oContext.getOptions().getOption(PREFETCH_READER) && isAsyncEnabled;
    }

    public class HiveParquetRowGroupReaderCreator extends SplitReaderCreator {

      private UnifiedParquetReader innerReader; // member variable so it can be closed

      private final BiConsumer<Path, ParquetMetadata> depletionListener = (path, footer) -> {
        if (!prefetchReader) {
          return;
        }
        if (next != null) {
          next.createInputStreamProvider(path, footer);
        } else { // last rowgroup initiates next filesplit reads
          if (nextFileSplitReader != null) {
            nextFileSplitReader.createInputStreamProvider(path, footer);
          }
        }
      };

      private HiveParquetRowGroupReaderCreator(ParquetDatasetSplitScanXAttr splitXAttr) {
        Preconditions.checkNotNull(splitXAttr, "Split cannot be null");
        this.splitXAttr = splitXAttr;
        this.path = Path.of(splitXAttr.getPath());
        this.tablePath = Lists.newArrayList(referencedTables);
        if (!fs.supportsPath(path)) {
          throw UserException.invalidMetadataError()
            .addContext(String.format("%s: Invalid FS for file '%s'", fs.getScheme(), path))
            .addContext("File", path)
            .setAdditionalExceptionContext(
              new InvalidMetadataErrorContext(
                ImmutableList.copyOf(tablePath)))
            .buildSilently();
        }
      }

      private HiveParquetRowGroupReaderCreator(ParquetDatasetSplitScanXAttr splitXAttr, InputStreamProvider inputStreamProviderOfFirstRG) {
        this(splitXAttr);
        this.inputStreamProvider = inputStreamProviderOfFirstRG;
      }

      @Override
      public RecordReader createRecordReader() {
        Preconditions.checkNotNull(inputStreamProvider); // make sure inputStreamProvider is created first
        this.getFooter(); // make sure all read-futures are complete; should be no-op since PrefetchingIterator already ensures this
        depletionListener.accept(path, footer);
        try {
          innerReader = new UnifiedParquetReader(
            oContext,
            readerFactory,
            tableSchema,
            ParquetScanProjectedColumns.fromSchemaPaths(columnsToRead),
            null,
            conditions,
            readerFactory.newFilterCreator(ParquetReaderFactory.ManagedSchemaType.HIVE, managedSchema),
            readerFactory.newDictionaryConvertor(ParquetReaderFactory.ManagedSchemaType.HIVE, managedSchema),
            splitXAttr,
            fs,
            footer,
            null,
            schemaHelper,
            vectorize,
            enableDetailedTracing,
            true,
            inputStreamProvider
          );
          innerReader.setIgnoreSchemaLearning(true);

          final PrivilegedExceptionAction<Void> readerSetupAction = () -> {
            innerReader.setup(output);
            return null;
          };
          try {
            readerUgi.doAs(readerSetupAction);
          } catch (Exception e) {
            throw new RuntimeException("Failure during execution setup", e);
          }
          return innerReader;
        } finally {
          splitXAttr = null;
          inputStreamProvider = null;
        }
      }

      @Override
      public void createInputStreamProvider(Path lastPath, ParquetMetadata lastFooter) {
        if (inputStreamProvider != null) {
          return;
        }
        handleEx(() -> {
          final List<String> dataset = tablePath == null || tablePath.isEmpty() ? null : tablePath.iterator().next();
          inputStreamProvider = inputStreamProviderFactory.create(fs, oContext, path, fileLength, splitXAttr.getLength(),
            ParquetScanProjectedColumns.fromSchemaPaths(columnsToRead), footer, (f) -> splitXAttr.getRowGroupIndex(),
            (a, b) -> {}, // prefetching happens in this.createRecordReader()
            readFullFile, dataset, splitXAttr.getLastModificationTime());
          return null;
        });
      }

      @Override
      public void close() throws Exception {
        AutoCloseables.close(innerReader, inputStreamProvider);
      }
    }
  }
}
