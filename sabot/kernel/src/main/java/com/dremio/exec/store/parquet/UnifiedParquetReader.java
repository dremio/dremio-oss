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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.columnreaders.DeprecatedParquetVectorizedReader;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitScanXAttr;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

public class UnifiedParquetReader implements RecordReader {

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnifiedParquetReader.class);

  private final OperatorContext context;
  private final ParquetMetadata footer;
  private final ParquetDatasetSplitScanXAttr readEntry;
  private final SchemaDerivationHelper schemaHelper;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final BatchSchema tableSchema;
  private final List<SchemaPath> realFields;
  private final FileSystem fs;
  private final GlobalDictionaries dictionaries;
  private final CodecFactory codecFactory;
  private final ParquetReaderFactory readerFactory;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap;
  private final List<ParquetFilterCondition> filterConditions;

  private List<RecordReader> delegates = new ArrayList<>();
  private final List<SchemaPath> nonVectorizableReaderColumns = new ArrayList<>();
  private final List<SchemaPath> vectorizableReaderColumns = new ArrayList<>();
  private final Map<String, ValueVector> vectorizedMap = new HashMap<>();
  private final Map<String, ValueVector> nonVectorizedMap = new HashMap<>();
  private InputStreamProvider inputStreamProvider;

  public UnifiedParquetReader(
      OperatorContext context,
      ParquetReaderFactory readerFactory,
      BatchSchema tableSchema,
      List<SchemaPath> realFields,
      Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap,
      List<ParquetFilterCondition> filterConditions,
      ParquetDatasetSplitScanXAttr readEntry,
      FileSystem fs,
      ParquetMetadata footer,
      GlobalDictionaries dictionaries,
      CodecFactory codecFactory,
      SchemaDerivationHelper schemaHelper,
      boolean vectorize,
      boolean enableDetailedTracing,
      InputStreamProvider inputStreamProvider) {
    super();
    this.context = context;
    this.readerFactory = readerFactory;
    this.globalDictionaryFieldInfoMap = globalDictionaryFieldInfoMap;
    this.filterConditions = filterConditions;
    this.fs = fs;
    this.footer = footer;
    this.readEntry = readEntry;
    this.vectorize = vectorize;
    this.tableSchema = tableSchema;
    this.realFields = realFields;
    this.dictionaries = dictionaries;
    this.codecFactory = codecFactory;
    this.enableDetailedTracing = enableDetailedTracing;
    this.inputStreamProvider = inputStreamProvider;
    this.schemaHelper = schemaHelper;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    computeLocality(footer);

    splitColumns(footer, vectorizableReaderColumns, nonVectorizableReaderColumns);



    final ExecutionPath execPath = getExecutionPath();
    delegates = execPath.getReaders(this);

    Preconditions.checkArgument(!delegates.isEmpty(), "There should be at least one delegated RecordReader");
    for (RecordReader delegateReader : delegates) {
      delegateReader.setup(output);
    }

    for (SchemaPath path : vectorizableReaderColumns) {
      String name = path.getRootSegment().getNameSegment().getPath();
      vectorizedMap.put(name, output.getVector(name));
    }
    for (SchemaPath path : nonVectorizableReaderColumns) {
      String name = path.getRootSegment().getNameSegment().getPath();
      nonVectorizedMap.put(name, output.getVector(name));
    }

    context.getStats().setLongStat(Metric.PARQUET_EXEC_PATH, execPath.ordinal());
    context.getStats().setLongStat(Metric.NUM_VECTORIZED_COLUMNS, vectorizableReaderColumns.size());
    context.getStats().setLongStat(Metric.NUM_NON_VECTORIZED_COLUMNS, nonVectorizableReaderColumns.size());
    context.getStats().setLongStat(Metric.FILTER_EXISTS, filterConditions != null && filterConditions.size() > 0 ? 1 : 0);
  }

  // No reason to use delegates since Parquet always uses the default schema change mutator.
  @Override
  public SchemaChangeMutator getSchemaChangeMutator() {
    return SchemaChangeMutator.DEFAULT;
  }

  private void computeLocality(ParquetMetadata footer) throws ExecutionSetupException {
    try {
      BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());

      BlockLocation[] blockLocations = fs.getFileBlockLocations(new Path(readEntry.getPath()), block.getStartingPos(), block.getCompressedSize());

      String localHost = InetAddress.getLocalHost().getCanonicalHostName();

      List<Range<Long>> intersectingRanges = new ArrayList<>();

      Range<Long> rowGroupRange = Range.openClosed(block.getStartingPos(), block.getStartingPos() + block.getCompressedSize());

      for (BlockLocation loc : blockLocations) {
        for (String host : loc.getHosts()) {
          if (host.equals(localHost)) {
            intersectingRanges.add(Range.closedOpen(loc.getOffset(), loc.getOffset() + loc.getLength()).intersection(rowGroupRange));
          }
        }
      }

      long totalIntersect = 0;
      for (Range<Long> range : intersectingRanges) {
        totalIntersect += (range.upperEndpoint() - range.lowerEndpoint());
      }
      if (totalIntersect < block.getCompressedSize()) {
        context.getStats().addLongStat(Metric.NUM_REMOTE_READERS, 1);
      } else {
        context.getStats().addLongStat(Metric.NUM_REMOTE_READERS, 0);
      }
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private RecordReader addFilterIfNecessary(RecordReader delegate) {
    if (filterConditions == null || filterConditions.isEmpty()) {
      return delegate;
    }

    Preconditions.checkState(filterConditions.size() == 1, "we only support a single filterCondition per rowGroupScan for now");
    return new CopyingFilteringReader(delegate, context, filterConditions.get(0).getExpr());
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for(ValueVector v : vectorMap.values()){
      if(v instanceof FixedWidthVector){
        ((FixedWidthVector) v).allocateNew(context.getTargetBatchSize());
      } else {
        v.allocateNew();
      }
    }
  }

  @Override
  public int next() {
    // need to make sure number of rows in batch is the same for all the readers
    int count = -1;
    for (RecordReader recordReader : delegates) {
      int n = recordReader.next();
      if (count == -1) {
        count = n;
      } else {
        if (count != n) {
          throw new IllegalStateException(String.format("Inconsistent row count. Reader %s returned %d while " +
            "previous reader returned %d", recordReader.toString(), n, count));
        }
      }
    }
    return count;
  }

  @Override
  public void close() throws Exception {
    for (RecordReader delegateReader : delegates) {
      AutoCloseables.close(delegateReader);
    }
    AutoCloseables.close(inputStreamProvider);
  }

  private void splitColumns(final ParquetMetadata footer,
                            List<SchemaPath> vectorizableReaderColumns,
                            List<SchemaPath> nonVectorizableReaderColumns) {
    final BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
    final Map<String, ColumnChunkMetaData> fields = new HashMap<>();
    final List<Type> nonVectorizableTypes = new ArrayList<>();
    final List<Type> vectorizableTypes = new ArrayList<>();

    for (ColumnChunkMetaData c : block.getColumns()) {
      if (!readerFactory.isSupported(c)) {
        // we'll skip columns we can't read.
         continue;
      }

      fields.put(c.getPath().iterator().next(), c);
    }

    MessageType schema = footer.getFileMetaData().getSchema();
    for (Type parquetField : schema.getFields()) {
      if (fields.containsKey(parquetField.getName()) &&
        (parquetField.asPrimitiveType().getOriginalType() != OriginalType.DECIMAL
        && parquetField.isPrimitive()
          && (parquetField.asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT96 ||
          schemaHelper.readInt96AsTimeStamp()))) {
        vectorizableTypes.add(parquetField);
      } else {
        nonVectorizableTypes.add(parquetField);
      }
    }

    paths: for (SchemaPath path : getResolvedColumns(block.getColumns())) {
      String name = path.getRootSegment().getNameSegment().getPath();
      for (Type type : vectorizableTypes) {
        if (type.getName().equals(name)) {
          vectorizableReaderColumns.add(path);
          continue paths;
        } else if (type.getName().equalsIgnoreCase(name)) {
          // get path from metadata
          vectorizableReaderColumns.add(SchemaPath.getSimplePath(type.getName()));
          continue paths;
        }
      }
      for (Type type : nonVectorizableTypes) {
        if (type.getName().equals(name)) {
          nonVectorizableReaderColumns.add(path);
          break;
        } else if (type.getName().equalsIgnoreCase(name)) {
          nonVectorizableReaderColumns.add(SchemaPath.getSimplePath(type.getName()));
          break;
        }
      }
    }
  }

  private Collection<SchemaPath> getResolvedColumns(List<ColumnChunkMetaData> metadata){
    if(!ColumnUtils.isStarQuery(realFields)){
      // Return all selected columns + any additional columns that are not present in the table schema (for schema
      // learning purpose)
      List<SchemaPath> columnsToRead = Lists.newArrayList(realFields);
      Set<String> columnsTableDef = Sets.newHashSet();
      Set<String> columnsTableDefLowercase = Sets.newHashSet();
      tableSchema.forEach(f -> columnsTableDef.add(f.getName()));
      tableSchema.forEach(f -> columnsTableDefLowercase.add(f.getName().toLowerCase()));
      for (ColumnChunkMetaData c : metadata) {
        final String columnInParquetFile = c.getPath().iterator().next();
        // Column names in parquet are case sensitive, in Dremio they are case insensitive
        // First try to find the column with exact case. If not found try the case insensitive comparision.
        if (!columnsTableDef.contains(columnInParquetFile) &&
            !columnsTableDefLowercase.contains(columnInParquetFile.toLowerCase())) {
          columnsToRead.add(SchemaPath.getSimplePath(columnInParquetFile));
        }
      }

      return columnsToRead;
    }

    List<SchemaPath> paths = new ArrayList<>();
    for(ColumnChunkMetaData c : metadata){
      paths.add(SchemaPath.getSimplePath(c.getPath().iterator().next()));
    }

    return paths;
  }

  private boolean determineFilterConditions(List<SchemaPath> vectorizableColumns,
                                            List<SchemaPath> nonVectorizableColumns) {
    if (filterConditions == null || filterConditions.isEmpty()) {
      return true;
    }
    return isConditionSet(vectorizableColumns, nonVectorizableColumns);
  }

  private boolean isConditionSet(List<SchemaPath> vectorizableColumns, List<SchemaPath> nonVectorizableColumns) {
    if (filterConditions == null || filterConditions.isEmpty()) {
      return false;
    }
    Preconditions.checkState(filterConditions.size() == 1, "we only support a single filterCondition per rowGroupScan for now");

    for (SchemaPath schema : vectorizableColumns) {
      if (filterConditions.get(0).getPath().equals(schema)) {
        return true;
      }
    }
    for (SchemaPath schema : nonVectorizableColumns) {
      if (filterConditions.get(0).getPath().equals(schema)) {
        return false;
      }
    }
    return true;
  }

  public ParquetMetadata getFooter() {
    return footer;
  }

  /**
   * Simple enum to handle different code paths
   * with RowWise, DeprecatedVectorized and HybridVectorized filters
   */
  private enum ExecutionPath {
    DEPRECATED_VECTORIZED {
      @Override
      public List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) throws ExecutionSetupException {
        List<RecordReader> returnList = new ArrayList<>();
          returnList.add(unifiedReader.addFilterIfNecessary(
            new DeprecatedParquetVectorizedReader(
              unifiedReader.context,
              unifiedReader.readEntry.getPath(), unifiedReader.readEntry.getRowGroupIndex(), unifiedReader.fs,
              CodecFactory.createDirectCodecFactory(
                unifiedReader.fs.getConf(),
                new ParquetDirectByteBufferAllocator(unifiedReader.context.getAllocator()), 0),
              unifiedReader.getFooter(),
              unifiedReader.realFields,
              unifiedReader.schemaHelper,
              unifiedReader.globalDictionaryFieldInfoMap,
              unifiedReader.dictionaries
            )
          ));
        return returnList;
      }
    },
    ROWWISE {
      @Override
      public List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) {
        List<RecordReader> returnList = new ArrayList<>();
        returnList.add(unifiedReader.addFilterIfNecessary(
          new ParquetRowiseReader(
            unifiedReader.context,
            unifiedReader.getFooter(),
            unifiedReader.readEntry.getRowGroupIndex(),
            unifiedReader.readEntry.getPath(),
            unifiedReader.realFields,
            unifiedReader.fs,
            unifiedReader.schemaHelper,
            unifiedReader.inputStreamProvider
          )
        ));
        return returnList;
      }
    },
    VECTORIZED {
      @Override
      public List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) {
        boolean isVectorizableFilterOn = unifiedReader.isConditionSet(unifiedReader.vectorizableReaderColumns,
          unifiedReader.nonVectorizableReaderColumns);
        final SimpleIntVector deltas;
        if (isVectorizableFilterOn) {
          deltas = new SimpleIntVector("deltas", unifiedReader.context.getAllocator());
        } else {
          deltas = null;
        }

        List<RecordReader> returnList = new ArrayList<>();
        if (!unifiedReader.vectorizableReaderColumns.isEmpty() || unifiedReader.nonVectorizableReaderColumns.isEmpty()) {
          returnList.add(
              unifiedReader.readerFactory.newReader(
                  unifiedReader.context,
                  unifiedReader.vectorizableReaderColumns,
                  unifiedReader.readEntry.getPath(),
                  unifiedReader.codecFactory,
                  unifiedReader.filterConditions,
                  unifiedReader.enableDetailedTracing,
                  unifiedReader.getFooter(),
                  unifiedReader.readEntry.getRowGroupIndex(),
                  deltas,
                  unifiedReader.schemaHelper,
                  unifiedReader.inputStreamProvider
              )
          );
        }
        if (!unifiedReader.nonVectorizableReaderColumns.isEmpty()) {
          returnList.add(
            new ParquetRowiseReader(
              unifiedReader.context,
              unifiedReader.getFooter(),
              unifiedReader.readEntry.getRowGroupIndex(),
              unifiedReader.readEntry.getPath(),
              unifiedReader.nonVectorizableReaderColumns,
              unifiedReader.fs,
              unifiedReader.schemaHelper,
              deltas,
              unifiedReader.inputStreamProvider
            )
          );
        }
        return returnList;
      }
    },

    SKIPALL {
      @Override
      public List<RecordReader> getReaders(final UnifiedParquetReader unifiedReader) throws ExecutionSetupException {
        final ParquetMetadata footer = unifiedReader.getFooter();
        final List<BlockMetaData> blocks = footer.getBlocks();
        final int rowGroupIdx = unifiedReader.readEntry.getRowGroupIndex();
        if (blocks.size() <= rowGroupIdx) {
          throw new IllegalArgumentException(
              String.format("Invalid rowgroup index in read entry. Given '%d', Max '%d'", rowGroupIdx, blocks.size())
          );
        }

        final long rowCount = blocks.get(rowGroupIdx).getRowCount();

        final RecordReader reader = new AbstractRecordReader(unifiedReader.context, Collections.<SchemaPath>emptyList()) {
          private long remainingRowCount = rowCount;

          @Override
          public void setup(OutputMutator output) throws ExecutionSetupException {

          }

          @Override
          public int next() {
            if (numRowsPerBatch > remainingRowCount) {
              int toReturn = (int) remainingRowCount;
              remainingRowCount = 0;
              return toReturn;
            }

            remainingRowCount -= numRowsPerBatch;
            return (int)numRowsPerBatch;
          }

          @Override
          public void close() throws Exception {

          }
        };
        return Collections.singletonList(reader);
      }
    };

    /**
     * To produce list of the RecordReaders for each enum entry
     * @param unifiedReader
     * @return list of RecordReaders
     * @throws ExecutionSetupException
     */
    public abstract List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) throws ExecutionSetupException;
  }

  private ExecutionPath getExecutionPath() {
    if ((globalDictionaryFieldInfoMap != null && !globalDictionaryFieldInfoMap.isEmpty())) {
      return ExecutionPath.DEPRECATED_VECTORIZED;
    }
    if (!determineFilterConditions(vectorizableReaderColumns, nonVectorizableReaderColumns) || !vectorize) {
      return ExecutionPath.ROWWISE;
    }

    if (vectorizableReaderColumns.isEmpty() && nonVectorizableReaderColumns.isEmpty()) {
      return ExecutionPath.SKIPALL;
    }
    return ExecutionPath.VECTORIZED;
  }

  public static ParquetReaderFactory getReaderFactory(SabotConfig config){
    return config.getInstance("dremio.plugins.parquet.factory", ParquetReaderFactory.class, ParquetReaderFactory.NONE);
  }
}
