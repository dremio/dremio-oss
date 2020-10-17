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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.parquet.columnreaders.DeprecatedParquetVectorizedReader;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

public class UnifiedParquetReader implements RecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnifiedParquetReader.class);
  private final OperatorContext context;
  private final MutableParquetMetadata footer;
  private final ParquetDatasetSplitScanXAttr readEntry;
  private final SchemaDerivationHelper schemaHelper;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final BatchSchema tableSchema;
  private ParquetScanProjectedColumns projectedColumns;
  private ParquetColumnResolver columnResolver;
  private final FileSystem fs;
  private final GlobalDictionaries dictionaries;
  private final CompressionCodecFactory codecFactory;
  private final ParquetReaderFactory readerFactory;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap;
  private final List<ParquetFilterCondition>  filterConditions;
  private final ParquetFilterCreator filterCreator;
  private final ParquetDictionaryConvertor dictionaryConvertor;
  private final boolean supportsColocatedReads;

  private List<RecordReader> delegates = new ArrayList<>();
  private final List<SchemaPath> nonVectorizableReaderColumns = new ArrayList<>();
  private final List<SchemaPath> vectorizableReaderColumns = new ArrayList<>();
  private InputStreamProvider inputStreamProvider;
  private boolean ignoreSchemaLearning;
  private List<RuntimeFilter> runtimeFilters;

  private OutputMutator outputMutator;
  private ArrowBuf validityBuf;
  private final int maxValidityBufSize;

  public UnifiedParquetReader(
      OperatorContext context,
      ParquetReaderFactory readerFactory,
      BatchSchema tableSchema,
      ParquetScanProjectedColumns projectedColumns,
      Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap,
      List<ParquetFilterCondition> filterConditions,
      ParquetFilterCreator filterCreator,
      ParquetDictionaryConvertor dictionaryConvertor,
      ParquetDatasetSplitScanXAttr readEntry,
      FileSystem fs,
      MutableParquetMetadata footer,
      GlobalDictionaries dictionaries,
      SchemaDerivationHelper schemaHelper,
      boolean vectorize,
      boolean enableDetailedTracing,
      boolean supportsColocatedReads,
      InputStreamProvider inputStreamProvider,
      List<RuntimeFilter> runtimeFilters) {
    super();
    this.context = context;
    this.readerFactory = readerFactory;
    this.globalDictionaryFieldInfoMap = globalDictionaryFieldInfoMap;
    this.filterConditions = filterConditions;
    this.filterCreator = filterCreator;
    this.dictionaryConvertor = dictionaryConvertor;
    this.fs = fs;
    this.footer = footer;
    this.readEntry = readEntry;
    this.vectorize = vectorize;
    this.tableSchema = tableSchema;
    this.projectedColumns = projectedColumns;
    this.columnResolver = null;
    this.dictionaries = dictionaries;
    this.codecFactory = CodecFactory.createDirectCodecFactory(new Configuration(), new ParquetDirectByteBufferAllocator(context.getAllocator()), 0);
    this.enableDetailedTracing = enableDetailedTracing;
    this.inputStreamProvider = inputStreamProvider;
    this.schemaHelper = schemaHelper;
    this.supportsColocatedReads = supportsColocatedReads;
    this.ignoreSchemaLearning = false;
    this.runtimeFilters = runtimeFilters == null ? new ArrayList<>() : runtimeFilters;
    this.maxValidityBufSize = BitVectorHelper.getValidityBufferSize(context.getTargetBatchSize());
    }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;

    if (supportsColocatedReads && context.getOptions().getOption(ExecConstants.SCAN_COMPUTE_LOCALITY)) {
      computeLocality(footer);
    }

    this.columnResolver = this.projectedColumns.getColumnResolver(
      footer.getFileMetaData().getSchema());
    splitColumns(footer, vectorizableReaderColumns, nonVectorizableReaderColumns);

    dropIncompatibleRuntimeFilters(output);
    Set<String> filterColumns = runtimeFilters.stream()
      .flatMap(rf -> rf.getNonPartitionColumnFilters().stream())
      .flatMap(ccf -> ccf.getColumnsList().stream())
      .map(String::toLowerCase).collect(Collectors.toSet());
    if (filterConditions != null) {
      filterColumns.addAll(filterConditions.stream().map(pfc -> pfc.getPath().getAsNamePart().getName().toLowerCase()).collect(Collectors.toSet()));
    }

    // init validity buf only if filters are on multiple columns
    if (filterColumns.size() > 1) {
      this.validityBuf = context.getAllocator().buffer(maxValidityBufSize);
    }

    final ExecutionPath execPath = getExecutionPath();
    delegates = execPath.getReaders(this);

    Preconditions.checkArgument(!delegates.isEmpty(), "There should be at least one delegated RecordReader");
    for (RecordReader delegateReader : delegates) {
      delegateReader.setup(output);
    }

    context.getStats().setLongStat(Metric.PARQUET_EXEC_PATH, execPath.ordinal());
    context.getStats().setLongStat(Metric.NUM_VECTORIZED_COLUMNS, vectorizableReaderColumns.size());
    context.getStats().setLongStat(Metric.NUM_NON_VECTORIZED_COLUMNS, nonVectorizableReaderColumns.size());
    context.getStats().setLongStat(Metric.FILTER_EXISTS, filterConditions != null && filterConditions.size() > 0 ? 1 : 0);

    boolean enableColumnTrim = context.getOptions().getOption(ExecConstants.TRIM_COLUMNS_FROM_ROW_GROUP);
    if (output.getSchemaChanged() || !enableColumnTrim) {
      return;
    }

    // remove information about columns that we dont care about
    Set<String> parquetColumnNamesToRetain = getParquetColumnNamesToRetain();
    if (parquetColumnNamesToRetain != null) {
      long numColumnsTrimmed = footer.removeUnneededColumns(parquetColumnNamesToRetain);
      context.getStats().addLongStat(Metric.NUM_COLUMNS_TRIMMED, numColumnsTrimmed);
    }
  }

  @VisibleForTesting
  void dropIncompatibleRuntimeFilters(final OutputMutator output) {
    try {
      final List<RuntimeFilter> voidFilters = new ArrayList<>(runtimeFilters.size());
      for (RuntimeFilter runtimeFilter : runtimeFilters) {
        final List<CompositeColumnFilter> retainedColFilters = new ArrayList<>();
        for (CompositeColumnFilter colFilter : runtimeFilter.getNonPartitionColumnFilters()) {
          final String colName = colFilter.getColumnsList().get(0).toLowerCase();
          final ArrowType scanFieldType = output.getVector(colName).getField().getType();
          final Types.MinorType scanFieldMinorType = Types.getMinorTypeForArrowType(scanFieldType);
          if (!colFilter.getValueList().getFieldType().equals(scanFieldMinorType)) {
            logger.warn("Dropping runtime filter on {} because scan type {} is incompatible with filter type {}",
                    colName, scanFieldMinorType, colFilter.getValueList().getFieldType());
            context.getStats().addLongStat(Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);
            colFilter.close();
          } else {
            retainedColFilters.add(colFilter);
          }
        }
        runtimeFilter.getNonPartitionColumnFilters().retainAll(retainedColFilters);
        if (runtimeFilter.getPartitionColumnFilter() == null && runtimeFilter.getNonPartitionColumnFilters().isEmpty()) {
          voidFilters.add(runtimeFilter);
        }
      }
      runtimeFilters.removeAll(voidFilters);
    } catch (Exception e) {
      logger.warn("Error while evaluating runtime filter drop candidates.", e);
    }
  }

  @VisibleForTesting // used only in tests
  List<RuntimeFilter> getRuntimeFilters() {
    return this.runtimeFilters;
  }

  public void setIgnoreSchemaLearning(boolean ignoreSchemaLearning) {
    this.ignoreSchemaLearning = ignoreSchemaLearning;
  }

  private void computeLocality(MutableParquetMetadata footer) throws ExecutionSetupException {
    try {
      BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
      Preconditions.checkArgument(block != null, "Parquet footer does not contain information about row group");

      Iterable<FileBlockLocation> blockLocations = fs.getFileBlockLocations(Path.of(readEntry.getPath()), block.getStartingPos(), block.getCompressedSize());

      String localHost = InetAddress.getLocalHost().getCanonicalHostName();

      List<Range<Long>> intersectingRanges = new ArrayList<>();

      Range<Long> rowGroupRange = Range.openClosed(block.getStartingPos(), block.getStartingPos() + block.getCompressedSize());

      for (FileBlockLocation loc : blockLocations) {
        for (String host : loc.getHosts()) {
          if (host.equals(localHost)) {
            intersectingRanges.add(Range.closedOpen(loc.getOffset(), loc.getOffset() + loc.getSize()).intersection(rowGroupRange));
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

    if (filterCreator.filterMayChange()) {
      filterConditions.get(0).setFilterModifiedForPushdown(true);
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
    // at most one filter
    if (validityBuf == null) {
      return readEnsuringReadersReturnSameNumberOfRecords();
    }

    int totalRecords = -1; // including invalid records

    // read till a non-empty batch is found
    while (totalRecords == -1 || (totalRecords > 0 && totalRecords == BitVectorHelper.getNullCount(validityBuf, totalRecords))) {
      validityBuf.setOne(0, maxValidityBufSize);
      totalRecords = readEnsuringReadersReturnSameNumberOfRecords();
    }
    // remove invalid rows if present
    if (totalRecords > 0 && BitVectorHelper.getNullCount(validityBuf, totalRecords) != 0) {
      totalRecords = removeInvalidRows(totalRecords);
    }

    return totalRecords;
  }

  /**
   * Read from each reader and make sure number of rows in batch is the same for all the readers
   * @return
   */
  private int readEnsuringReadersReturnSameNumberOfRecords() {
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

  /**
   *
   * @param records
   * @return
   */
  private int removeInvalidRows(int records) {
    // TODO(DX-25408): avoid copying in certain cases
    int copyTo = 0;
    for (int i = 0; i < records; i++) {
      if (BitVectorHelper.get(validityBuf, i) == 1) {
        Preconditions.checkArgument(copyTo <= i, "Copying from lower index to higher index will lead to data corruption");
        if (i != copyTo) {
          for (ValueVector vv : outputMutator.getVectors()) {
            vv.copyFrom(i, copyTo, vv); // copies validity and value
          }
        }
        copyTo++;
      }
    }
    for (ValueVector vv : outputMutator.getVectors()) {
      vv.setValueCount(copyTo);
    }
    return copyTo;
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    List<SchemaPath> columnsToBoost = Lists.newArrayList();
    for(RecordReader recordReader : delegates) {
      List<SchemaPath> tmp = recordReader.getColumnsToBoost();
      if (tmp != null) {
        columnsToBoost.addAll(tmp);
      }
    }

    return columnsToBoost;
  }

  @Override
  public void close() throws Exception {
    if (context.getOptions().getOption(ExecConstants.TRIM_ROWGROUPS_FROM_FOOTER)) {
      footer.removeRowGroupInformation(readEntry.getRowGroupIndex());
      context.getStats().addLongStat(Metric.NUM_ROW_GROUPS_TRIMMED, 1);
    }
    try {
      List<AutoCloseable> closeables = new ArrayList<>();
      closeables.addAll(delegates);
      closeables.add(inputStreamProvider);
      closeables.add(validityBuf);
      AutoCloseables.close(closeables);
    } finally {
      delegates = null;
      inputStreamProvider = null;
      validityBuf = null;
    }
  }

  private void splitColumns(final MutableParquetMetadata footer,
                            List<SchemaPath> vectorizableReaderColumns,
                            List<SchemaPath> nonVectorizableReaderColumns) {
    final BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
    Preconditions.checkArgument(block != null, "Parquet footer does not contain information about row group");
    final Map<String, ColumnChunkMetaData> fieldsWithEncodingsSupportedByVectorizedReader = new HashMap<>();
    final List<Type> nonVectorizableTypes = new ArrayList<>();
    final List<Type> vectorizableTypes = new ArrayList<>();

    for (ColumnChunkMetaData c : block.getColumns()) {
      if (!readerFactory.isSupported(c)) {
        // we'll skip columns we can't read.
         continue;
      }

      fieldsWithEncodingsSupportedByVectorizedReader.put(c.getPath().iterator().next(), c);
    }

    MessageType schema = footer.getFileMetaData().getSchema();
    for (Type parquetField : schema.getFields()) {
      if (this.ignoreSchemaLearning) {
        // check if parquet field is in projected columns, if not, then ignore it
        boolean ignoreThisField = true;
        for (SchemaPath projectedPath : this.columnResolver.getBatchSchemaProjectedColumns()) {
          String name = projectedPath.getRootSegment().getNameSegment().getPath();
          String parquetColumnName = this.columnResolver.getParquetColumnName(name);
          if (parquetColumnName != null && parquetField.getName().equalsIgnoreCase(parquetColumnName)) {
            ignoreThisField = false;
            break;
          }
        }
        if (ignoreThisField) {
          continue;
        }
      }
      if (fieldsWithEncodingsSupportedByVectorizedReader.containsKey(parquetField.getName()) &&
          isParquetFieldVectorizable(fieldsWithEncodingsSupportedByVectorizedReader, parquetField)) {
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
        if (type.getName().equalsIgnoreCase(name)) {
          // path will be given to parquet rowise reader as input, which should be able to
          // map path to correct parquet column descriptor by doing case insensitive mapping
          nonVectorizableReaderColumns.add(path);
          break;
        }
      }
    }
  }

  private boolean isParquetFieldVectorizable(Map<String, ColumnChunkMetaData> fields, Type parquetField) {
    return (parquetField.isPrimitive() && isNotInt96(parquetField) &&
            checkIfDecimalIsVectorizable(parquetField, fields.get(parquetField.getName())));
  }

  private boolean isNotInt96(Type parquetField) {
    return parquetField.asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT96 ||
    schemaHelper.readInt96AsTimeStamp();
  }

  private boolean checkIfDecimalIsVectorizable(Type parquetField, ColumnChunkMetaData metadata) {
    if (parquetField.asPrimitiveType().getOriginalType() != OriginalType.DECIMAL) {
      return true;
    }

    return context.getOptions().getOption(PlannerSettings.ENABLE_VECTORIZED_PARQUET_DECIMAL);
  }

  // Returns top-level column names in Parquet file converted to lower case
  private Set<String> getParquetColumnNamesToRetain() {
    if(ColumnUtils.isStarQuery(this.columnResolver.getBatchSchemaProjectedColumns())){
      return null;
    }

    return this.columnResolver.getProjectedParquetColumns()
      .stream()
      .map((s) -> s.getRootSegment().getNameSegment().getPath().toLowerCase())
      .collect(Collectors.toSet());
  }

  private Collection<SchemaPath> getResolvedColumns(List<ColumnChunkMetaData> metadata){
    if(!ColumnUtils.isStarQuery(this.columnResolver.getBatchSchemaProjectedColumns())){
      // Return all selected columns + any additional columns that are not present in the table schema (for schema
      // learning purpose)
      List<SchemaPath> columnsToRead = Lists.newArrayList(this.columnResolver.getProjectedParquetColumns());
      Set<String> columnsTableDef = Sets.newHashSet();
      Set<String> columnsTableDefLowercase = Sets.newHashSet();
      tableSchema.forEach(f -> columnsTableDef.add(f.getName()));
      tableSchema.forEach(f -> columnsTableDefLowercase.add(f.getName().toLowerCase()));
      for (ColumnChunkMetaData c : metadata) {
        final String columnInParquetFile = c.getPath().iterator().next();
        // Column names in parquet are case sensitive, in Dremio they are case insensitive
        // First try to find the column with exact case. If not found try the case insensitive comparision.
        String batchSchemaColumnName = this.columnResolver.getBatchSchemaColumnName(columnInParquetFile);
        if (batchSchemaColumnName != null &&
            !columnsTableDef.contains(batchSchemaColumnName) &&
            !columnsTableDefLowercase.contains(batchSchemaColumnName.toLowerCase())) {
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

  private MutableParquetMetadata getFooter() {
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
              unifiedReader.codecFactory,
              unifiedReader.getFooter(),
              unifiedReader.projectedColumns,
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
            unifiedReader.projectedColumns,
            unifiedReader.fs,
            unifiedReader.schemaHelper,
            unifiedReader.inputStreamProvider,
            unifiedReader.codecFactory
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
        if (isVectorizableFilterOn || unifiedReader.isNonPartitionColFilterPresent()) {
          deltas = new SimpleIntVector("deltas", unifiedReader.context.getAllocator());
        } else {
          deltas = null;
        }

        List<RecordReader> returnList = new ArrayList<>();
        if (!unifiedReader.vectorizableReaderColumns.isEmpty() || unifiedReader.nonVectorizableReaderColumns.isEmpty()) {
          returnList.add(
              unifiedReader.readerFactory.newReader(
                  unifiedReader.context,
                  unifiedReader.projectedColumns.cloneForSchemaPaths(
                    unifiedReader.columnResolver.getBatchSchemaColumns(unifiedReader.vectorizableReaderColumns)),
                  unifiedReader.readEntry.getPath(),
                  unifiedReader.codecFactory,
                  unifiedReader.filterConditions,
                  unifiedReader.filterCreator,
                  unifiedReader.dictionaryConvertor,
                  unifiedReader.enableDetailedTracing,
                  unifiedReader.getFooter(),
                  unifiedReader.readEntry.getRowGroupIndex(),
                  deltas,
                  unifiedReader.schemaHelper,
                  unifiedReader.inputStreamProvider,
                  unifiedReader.runtimeFilters,
                  unifiedReader.validityBuf)
          );
        }
        if (!unifiedReader.nonVectorizableReaderColumns.isEmpty()) {
          returnList.add(
            new ParquetRowiseReader(
              unifiedReader.context,
              unifiedReader.getFooter(),
              unifiedReader.readEntry.getRowGroupIndex(),
              unifiedReader.readEntry.getPath(),
              unifiedReader.projectedColumns.cloneForSchemaPaths(
                unifiedReader.columnResolver.getBatchSchemaColumns(unifiedReader.nonVectorizableReaderColumns)
              ),
              unifiedReader.fs,
              unifiedReader.schemaHelper,
              deltas,
              unifiedReader.inputStreamProvider,
              unifiedReader.codecFactory
            )
          );
        }
        return returnList;
      }
    },
    SKIP_ALL {
      @Override
      public List<RecordReader> getReaders(final UnifiedParquetReader unifiedReader) {
        final RecordReader reader = new AbstractRecordReader(unifiedReader.context, Collections.emptyList()) {
          @Override
          public void setup(OutputMutator output) {
          }

          @Override
          public int next() {
            return 0;
          }

          @Override
          public void close() {
          }
        };
        return Collections.singletonList(reader);
      }
    },
    INCLUDE_ALL {
      @Override
      public List<RecordReader> getReaders(final UnifiedParquetReader unifiedReader) throws ExecutionSetupException {
        final MutableParquetMetadata footer = unifiedReader.getFooter();
        final List<BlockMetaData> blocks = footer.getBlocks();
        final int rowGroupIdx = unifiedReader.readEntry.getRowGroupIndex();
        if (blocks.size() <= rowGroupIdx) {
          throw new IllegalArgumentException(
              String.format("Invalid rowgroup index in read entry. Given '%d', Max '%d'", rowGroupIdx, blocks.size())
          );
        }

        BlockMetaData block = blocks.get(rowGroupIdx);
        Preconditions.checkArgument(block != null, "Parquet footer does not contain information about row group");
        final long rowCount = block.getRowCount();

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

  private boolean isNonPartitionColFilterPresent() {
    return this.runtimeFilters.stream().flatMap(f -> f.getNonPartitionColumnFilters().stream()).findAny().isPresent();
  }

  private ExecutionPath getExecutionPath() {
    if ((globalDictionaryFieldInfoMap != null && !globalDictionaryFieldInfoMap.isEmpty())) {
      return ExecutionPath.DEPRECATED_VECTORIZED;
    }
    if (!determineFilterConditions(vectorizableReaderColumns, nonVectorizableReaderColumns) || !vectorize) {
      return ExecutionPath.ROWWISE;
    }

    if (vectorizableReaderColumns.isEmpty() && nonVectorizableReaderColumns.isEmpty()) {
      return filterCanContainNull() ? ExecutionPath.INCLUDE_ALL : ExecutionPath.SKIP_ALL;
    }
    return ExecutionPath.VECTORIZED;
  }

  private boolean filterCanContainNull() {
    return CollectionUtils.isEmpty(filterConditions)
            && runtimeFilters
            .stream()
            .flatMap(r -> r.getNonPartitionColumnFilters().stream())
            .map(CompositeColumnFilter::getValueList)
            .filter(Objects::nonNull)
            .allMatch(ValueListFilter::isContainsNull);
  }

  public static ParquetReaderFactory getReaderFactory(SabotConfig config){
    return config.getInstance("dremio.plugins.parquet.factory", ParquetReaderFactory.class, ParquetReaderFactory.NONE);
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (runtimeFilter != null && !runtimeFilters.contains(runtimeFilter)) {
      this.runtimeFilters.add(runtimeFilter);
      this.delegates.forEach(d -> d.addRuntimeFilter(runtimeFilter));
    }
  }
}
