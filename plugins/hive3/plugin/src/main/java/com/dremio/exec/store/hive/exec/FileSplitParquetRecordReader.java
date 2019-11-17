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

import java.io.FileNotFoundException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.hive.ContextClassLoaderSwapper;
import com.dremio.exec.store.hive.exec.dfs.DremioHadoopFileSystemWrapper;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.SingleStreamProvider;
import com.dremio.exec.store.parquet.StreamPerColumnProvider;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A {@link RecordReader} implementation that takes a {@link FileSplit} and
 * wraps one or more {@link UnifiedParquetReader}s (one for each row groups in {@link FileSplit})
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

  private ParquetMetadata footer;
  private List<UnifiedParquetReader> innerReaders;
  private Iterator<UnifiedParquetReader> innerReadersIter;
  private UnifiedParquetReader currentReader;

  public FileSplitParquetRecordReader(
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
    final UserGroupInformation readerUgi
  ) {
    this.oContext = oContext;
    this.tableSchema = tableSchema;
    this.columnsToRead = columnsToRead;
    this.conditions = conditions;
    this.fileSplit = fileSplit;
    this.jobConf = jobConf;
    this.referencedTables = referencedTables;
    this.readerFactory = readerFactory;
    this.vectorize = vectorize;
    this.enableDetailedTracing = enableDetailedTracing;
    this.outputSchema = outputSchema;
    this.readerUgi = readerUgi;
  }


  private InputStreamProvider getInputStreamProvider(boolean useSingleStream, Path path,
                                                     DremioHadoopFileSystemWrapper fs, long fileLength, boolean readFullFile) {
    final long maxFooterLen = oContext.getOptions().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
    return (useSingleStream || readFullFile) ? new SingleStreamProvider(fs, path, fileLength, maxFooterLen, readFullFile, oContext) :
      new StreamPerColumnProvider(fs, path, fileLength,  maxFooterLen, oContext.getStats());
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try(ContextClassLoaderSwapper ccls = ContextClassLoaderSwapper.newInstance()) {
      try {
        boolean useSingleStream =
          // option is set for single stream
          oContext.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM) ||
            // number of columns is above threshold
            columnsToRead.size() >= oContext.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD) ||
            // split size is below multi stream size limit and the limit is enabled
            (oContext.getOptions().getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT_ENABLE) &&
              fileSplit.getLength() < oContext.getOptions().getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT));

        final org.apache.hadoop.fs.Path finalPath = new org.apache.hadoop.fs.Path(fileSplit.getPath().toUri());
        final Path dremioPath = Path.of(finalPath.toUri());
        final String pathString = finalPath.toUri().getPath();
        final DremioHadoopFileSystemWrapper fs;
        final long fileLength;
        final boolean readFullFile;
        InputStreamProvider inputStreamProvider = null;

        try {
          // TODO: DX-16001 - make async configurable for Hive.
          final PrivilegedExceptionAction<DremioHadoopFileSystemWrapper> getFsAction =
            () -> new DremioHadoopFileSystemWrapper(finalPath, jobConf, oContext.getStats());

          fs = readerUgi.doAs(getFsAction);
          fileLength = fs.getFileAttributes(dremioPath).size();
          readFullFile = fileLength < oContext.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD) &&
            ((float) columnsToRead.size()) / outputSchema.getFieldCount() > oContext.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);
          logger.debug("readFullFile={},length={},threshold={},columns={},totalColumns={},ratio={},req ratio={}",
            readFullFile,
            fileLength,
            oContext.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD),
            columnsToRead.size(),
            outputSchema.getFieldCount(),
            ((float) columnsToRead.size()) / outputSchema.getFieldCount(),
            oContext.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO));

          inputStreamProvider = getInputStreamProvider(useSingleStream, dremioPath, fs, fileLength, readFullFile);
          footer = inputStreamProvider.getFooter();
        } catch (Exception e) {
          // Close input stream provider in case of errors
          if (inputStreamProvider != null) {
            try {
              inputStreamProvider.close();
            } catch (Exception ignore) {
            }
          }
          if (e instanceof FileNotFoundException) {
            // the outer try-catch handles this.
            throw (FileNotFoundException) e;
          } else {
            throw new ExecutionSetupException(
              String.format("Failed to create FileSystem: %s", e.getMessage()), e);
          }
        }

        final List<Integer> rowGroupNums = ParquetReaderUtility.getRowGroupNumbersFromFileSplit(fileSplit.getStart(), fileSplit.getLength(), footer);
        oContext.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, rowGroupNums.size());
        innerReaders = Lists.newArrayList();
        if (rowGroupNums.isEmpty()) {
          try {
            inputStreamProvider.close();
          } catch (Exception ignore) {
          }
        }

        for (int rowGroupNum : rowGroupNums) {
          ParquetDatasetSplitScanXAttr split = ParquetDatasetSplitScanXAttr.newBuilder()
            .setRowGroupIndex(rowGroupNum)
            .setPath(pathString)
            .setStart(0l)
            .setLength(Integer.MAX_VALUE)
            .build();

          // Reuse the stream used for reading footer to read the first row group.
          if (innerReaders.size() > 0) {
            inputStreamProvider = getInputStreamProvider(useSingleStream, dremioPath, fs, fileLength, readFullFile);
          }
          final boolean autoCorrectCorruptDates = oContext.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);
          final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
            .readInt96AsTimeStamp(true)
            .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, columnsToRead, autoCorrectCorruptDates))
            .noSchemaLearning(outputSchema)
            .build();

          final UnifiedParquetReader innerReader = new UnifiedParquetReader(
            oContext,
            readerFactory,
            tableSchema,
            columnsToRead,
            null,
            conditions,
            split,
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
          readerUgi.doAs(readerSetupAction);

          innerReaders.add(innerReader);
        }
        innerReadersIter = innerReaders.iterator();
      } catch (FileNotFoundException e) {
        throw UserException.invalidMetadataError(e)
          .addContext("Parquet file not found")
          .addContext("File", fileSplit.getPath())
          .setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(referencedTables)))
          .build(logger);
      } catch (Exception e) {
        throw new ExecutionSetupException("Failure during setup", e);
      }

      currentReader = innerReadersIter.hasNext() ? innerReadersIter.next() : null;
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

    while(currentReader != null) {
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
    AutoCloseables.close(innerReaders);
  }
}
