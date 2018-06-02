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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetReaderUtility.DateCorruptionStatus;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;
import com.google.common.collect.Lists;

/**
 * A {@link RecordReader} implementation that takes a {@link FileSplit} and
 * wraps one or more {@link UnifiedParquetReader}s (one for each row groups in {@link FileSplit})
 */
public class FileSplitParquetRecordReader implements RecordReader {

  private final OperatorContext oContext;
  private final List<SchemaPath> columnsToRead;
  private final List<SchemaPath> groupScanColumns;
  private final List<FilterCondition> conditions;
  private final FileSplit fileSplit;
  private final ParquetMetadata footer;
  private final JobConf jobConf;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final BatchSchema outputSchema;
  private final ParquetReaderFactory readerFactory;

  private List<UnifiedParquetReader> innerReaders;
  private Iterator<UnifiedParquetReader> innerReadersIter;
  private UnifiedParquetReader currentReader;

  public FileSplitParquetRecordReader(
      final OperatorContext oContext,
      final ParquetReaderFactory readerFactory,
      final List<SchemaPath> columnsToRead,
      final List<SchemaPath> groupScanColumns,
      final List<FilterCondition> conditions,
      final FileSplit fileSplit,
      final ParquetMetadata footer,
      final JobConf jobConf,
      final boolean vectorize,
      final BatchSchema outputSchema,
      final boolean enableDetailedTracing
  ) {
    this.oContext = oContext;
    this.columnsToRead = columnsToRead;
    this.groupScanColumns = groupScanColumns;
    this.conditions = conditions;
    this.fileSplit = fileSplit;
    this.footer = footer;
    this.jobConf = jobConf;
    this.readerFactory = readerFactory;
    this.vectorize = vectorize;
    this.enableDetailedTracing = enableDetailedTracing;
    this.outputSchema = outputSchema;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      final Path finalPath = fileSplit.getPath();

      final FileSystem fs;
      try {
        fs = FileSystemWrapper.get(finalPath, jobConf, oContext.getStats());
      } catch(IOException e) {
        throw new ExecutionSetupException(String.format("Failed to create FileSystem: %s", e.getMessage()), e);
      }

      final List<Integer> rowGroupNums = getRowGroupNumbersFromFileSplit(fileSplit, footer);
      oContext.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, rowGroupNums.size());

      innerReaders = Lists.newArrayList();
      for (int rowGroupNum : rowGroupNums) {
        ParquetDatasetSplitXAttr split = new ParquetDatasetSplitXAttr();
        split.setRowGroupIndex(rowGroupNum);
        split.setPath(Path.getPathWithoutSchemeAndAuthority(finalPath).toString());
        split.setStart(0l);
        split.setLength((long) Integer.MAX_VALUE);

        final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
            .readInt96AsTimeStamp(true)
            .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, groupScanColumns, true))
            .noSchemaLearning(outputSchema)
            .build();

        final UnifiedParquetReader innerReader = new UnifiedParquetReader(
            oContext,
            readerFactory,
            columnsToRead,
            null,
            conditions,
            split,
            fs,
            footer,
            null,
            CodecFactory.createDirectCodecFactory(jobConf, new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
            schemaHelper,
            vectorize,
            enableDetailedTracing
        );

        innerReader.setup(output);

        innerReaders.add(innerReader);
      }
      innerReadersIter = innerReaders.iterator();
    } catch (IOException e) {
      throw new ExecutionSetupException("Failure during setup", e);
    }

    currentReader = innerReadersIter.hasNext() ? innerReadersIter.next() : null;
  }

  @Override
  public SchemaChangeMutator getSchemaChangeMutator() {
    return SchemaChangeMutator.DEFAULT;
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

  /**
   * Get the list of row group numbers for given file input split. Logic used here is same as how Hive's parquet input
   * format finds the row group numbers for input split.
   */
  private static List<Integer> getRowGroupNumbersFromFileSplit(final FileSplit split,
      final ParquetMetadata footer) throws IOException {
    final List<BlockMetaData> blocks = footer.getBlocks();

    final long splitStart = split.getStart();
    final long splitLength = split.getLength();

    final List<Integer> rowGroupNums = Lists.newArrayList();

    int i = 0;
    for (final BlockMetaData block : blocks) {
      final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
      if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
        rowGroupNums.add(i);
      }
      i++;
    }

    return rowGroupNums;
  }
}
