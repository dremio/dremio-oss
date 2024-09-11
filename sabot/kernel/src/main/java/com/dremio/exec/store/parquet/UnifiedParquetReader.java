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

import com.dremio.common.AutoCloseables;
import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFilter;
import com.dremio.exec.store.parquet2.LogicalListL1Converter;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.exec.util.BitSetHelper;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.copier.CopierFactory;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class UnifiedParquetReader implements RecordReader {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(UnifiedParquetReader.class);
  private final OperatorContext context;
  private final MutableParquetMetadata footer;
  private final ParquetDatasetSplitScanXAttr readEntry;
  private final SchemaDerivationHelper schemaHelper;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final boolean isConvertedIcebergDataset;
  private final BatchSchema tableSchema;
  private ParquetScanProjectedColumns projectedColumns;
  private ParquetColumnResolver columnResolver;
  private final FileSystem fs;
  private final CompressionCodecFactory codecFactory;
  private final ParquetReaderFactory readerFactory;
  private final ParquetFilters filters;
  private final ParquetFilterCreator filterCreator;
  private final ParquetDictionaryConvertor dictionaryConvertor;
  private final boolean supportsColocatedReads;
  private final boolean useCopiersToRemoveInvalidRows;

  private List<RecordReader> delegates = new ArrayList<>();
  private final List<SchemaPath> nonVectorizableReaderColumns = new ArrayList<>();
  private final List<SchemaPath> vectorizableReaderColumns = new ArrayList<>();
  private InputStreamProvider inputStreamProvider;
  private boolean ignoreSchemaLearning;
  private List<RuntimeFilter> runtimeFilters;

  private OutputMutator outputMutator;
  private ArrowBuf validityBuf;
  private final int maxValidityBufSize;
  private List<FieldBufferCopier> copiers;
  private ArrowBuf sv2;

  public UnifiedParquetReader(
      OperatorContext context,
      ParquetReaderFactory readerFactory,
      BatchSchema tableSchema,
      ParquetScanProjectedColumns projectedColumns,
      ParquetFilters filters,
      ParquetFilterCreator filterCreator,
      ParquetDictionaryConvertor dictionaryConvertor,
      ParquetDatasetSplitScanXAttr readEntry,
      FileSystem fs,
      MutableParquetMetadata footer,
      SchemaDerivationHelper schemaHelper,
      boolean vectorize,
      boolean enableDetailedTracing,
      boolean supportsColocatedReads,
      InputStreamProvider inputStreamProvider,
      List<RuntimeFilter> runtimeFilters,
      boolean isConvertedIcebergDataset) {
    super();
    this.context = context;
    this.readerFactory = readerFactory;
    this.filters = filters;
    this.filterCreator = filterCreator;
    this.dictionaryConvertor = dictionaryConvertor;
    this.fs = fs;
    this.footer = footer;
    this.readEntry = readEntry;
    this.vectorize = vectorize;
    this.tableSchema = tableSchema;
    this.projectedColumns = projectedColumns;
    this.columnResolver = null;
    this.codecFactory =
        CodecFactory.createDirectCodecFactory(
            new Configuration(), new ParquetDirectByteBufferAllocator(context.getAllocator()), 0);
    this.enableDetailedTracing = enableDetailedTracing;
    this.inputStreamProvider = inputStreamProvider;
    this.schemaHelper = schemaHelper;
    this.supportsColocatedReads = supportsColocatedReads;
    this.ignoreSchemaLearning = false;
    this.runtimeFilters =
        runtimeFilters == null
            ? new ArrayList<>()
            : runtimeFilters.stream()
                .map(RuntimeFilter::getInstanceWithNewNonPartitionColFiltersList)
                .collect(Collectors.toList());
    this.maxValidityBufSize = BitVectorHelper.getValidityBufferSize(context.getTargetBatchSize());
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
    this.useCopiersToRemoveInvalidRows =
        context.getOptions().getOption(ExecConstants.USE_COPIER_IN_PARQUET_READER)
            && context.getTargetBatchSize() <= Short.MAX_VALUE;
  }

  public UnifiedParquetReader(
      OperatorContext context,
      ParquetReaderFactory readerFactory,
      BatchSchema tableSchema,
      ParquetScanProjectedColumns projectedColumns,
      ParquetFilters filters,
      ParquetFilterCreator filterCreator,
      ParquetDictionaryConvertor dictionaryConvertor,
      ParquetDatasetSplitScanXAttr readEntry,
      FileSystem fs,
      MutableParquetMetadata footer,
      SchemaDerivationHelper schemaHelper,
      boolean vectorize,
      boolean enableDetailedTracing,
      boolean supportsColocatedReads,
      InputStreamProvider inputStreamProvider,
      List<RuntimeFilter> runtimeFilters) {
    this(
        context,
        readerFactory,
        tableSchema,
        projectedColumns,
        filters,
        filterCreator,
        dictionaryConvertor,
        readEntry,
        fs,
        footer,
        schemaHelper,
        vectorize,
        enableDetailedTracing,
        supportsColocatedReads,
        inputStreamProvider,
        runtimeFilters,
        true);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;

    if (supportsColocatedReads
        && context.getOptions().getOption(ExecConstants.SCAN_COMPUTE_LOCALITY)) {
      computeLocality(footer);
    }

    this.columnResolver =
        this.projectedColumns.getColumnResolver(footer.getFileMetaData().getSchema());
    splitColumns(footer, vectorizableReaderColumns, nonVectorizableReaderColumns);

    Set<String> filterColumns =
        runtimeFilters.stream()
            .flatMap(rf -> rf.getNonPartitionColumnFilters().stream())
            .flatMap(ccf -> ccf.getColumnsList().stream())
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    if (filters.hasPushdownFilters()) {
      filterColumns.addAll(
          filters.getPushdownFilters().stream()
              .map(pfc -> pfc.getPath().toDotString().toLowerCase())
              .collect(Collectors.toSet()));
    }

    // If we have multiple filters, filtering beyond the 1st is done via the validityBuf, with
    // invalid rows
    // removed in a second pass.  The copiers are initialized after we process any delegate readers
    // so that any
    // columns they add are included in the copies - e.g. path/rownum columns for DML.
    boolean requiresInvalidRowRemoval =
        filterColumns.size() > 1
            || (filterColumns.size() == 1 && filters.hasPositionalDeleteFilter())
            || filters.hasEqualityDeleteFilter();
    if (requiresInvalidRowRemoval) {
      this.validityBuf = context.getAllocator().buffer(maxValidityBufSize);
    }

    final ExecutionPath execPath = getExecutionPath();
    delegates = execPath.getReaders(this);

    Preconditions.checkArgument(
        !delegates.isEmpty(), "There should be at least one delegated RecordReader");
    for (RecordReader delegateReader : delegates) {
      delegateReader.setup(output);
    }

    // init copiers only if filters are on multiple columns or a single filter along with positional
    // deletes
    if (requiresInvalidRowRemoval && useCopiersToRemoveInvalidRows) {
      List<FieldVector> outputFieldVectors = new ArrayList<>();
      outputMutator
          .getVectors()
          .forEach(valueVector -> outputFieldVectors.add((FieldVector) valueVector));
      copiers =
          CopierFactory.getInstance(context.getConfig(), context.getOptions())
              .getTwoByteCopiers(outputFieldVectors, outputFieldVectors, false);
      sv2 =
          context
              .getAllocator()
              .buffer(context.getTargetBatchSize() * SelectionVector2.RECORD_SIZE);
    }

    // init the equality delete filter if one is present
    if (filters.hasEqualityDeleteFilter()) {
      filters.getEqualityDeleteFilter().setup(outputMutator, validityBuf);
    }

    setMetricValue(Metric.PARQUET_EXEC_PATH, Long.valueOf(execPath.ordinal()));
    setMetricValue(Metric.NUM_VECTORIZED_COLUMNS, Long.valueOf(vectorizableReaderColumns.size()));
    setMetricValue(
        Metric.NUM_NON_VECTORIZED_COLUMNS, Long.valueOf(nonVectorizableReaderColumns.size()));
    setMetricValue(Metric.FILTER_EXISTS, filters.getPushdownFilters().size() > 0 ? 1L : 0L);

    boolean enableColumnTrim =
        context.getOptions().getOption(ExecConstants.TRIM_COLUMNS_FROM_ROW_GROUP);
    if (output.getSchemaChanged() || !enableColumnTrim) {
      return;
    }

    // remove information about columns that we dont care about
    Set<String> parquetColumnNamesToRetain = getParquetColumnNamesToRetain();
    if (parquetColumnNamesToRetain != null) {
      long numColumnsTrimmed = footer.removeUnneededColumns(parquetColumnNamesToRetain);
      addMetricValue(Metric.NUM_COLUMNS_TRIMMED, numColumnsTrimmed);
    }
  }

  public void setIgnoreSchemaLearning(boolean ignoreSchemaLearning) {
    this.ignoreSchemaLearning = ignoreSchemaLearning;
  }

  private void computeLocality(MutableParquetMetadata footer) throws ExecutionSetupException {
    try {
      BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
      Preconditions.checkArgument(
          block != null,
          "Parquet file '%s' footer does not have information about row group %s",
          this.readEntry.getPath(),
          readEntry.getRowGroupIndex());

      Iterable<FileBlockLocation> blockLocations =
          fs.getFileBlockLocations(
              Path.of(readEntry.getPath()), block.getStartingPos(), block.getCompressedSize());

      String localHost = InetAddress.getLocalHost().getCanonicalHostName();

      List<Range<Long>> intersectingRanges = new ArrayList<>();

      Range<Long> rowGroupRange =
          Range.openClosed(
              block.getStartingPos(), block.getStartingPos() + block.getCompressedSize());

      for (FileBlockLocation loc : blockLocations) {
        for (String host : loc.getHosts()) {
          if (host.equals(localHost)) {
            intersectingRanges.add(
                Range.closedOpen(loc.getOffset(), loc.getOffset() + loc.getSize())
                    .intersection(rowGroupRange));
          }
        }
      }

      long totalIntersect = 0;
      for (Range<Long> range : intersectingRanges) {
        totalIntersect += (range.upperEndpoint() - range.lowerEndpoint());
      }
      if (totalIntersect < block.getCompressedSize()) {
        addMetricValue(Metric.NUM_REMOTE_READERS, 1L);

      } else {
        addMetricValue(Metric.NUM_REMOTE_READERS, 0L);
      }

    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private void setMetricValue(Metric metric, Long longValue) {
    if (context.getStats() != null) {
      context.getStats().setLongStat(metric, longValue);
    }
  }

  private void addMetricValue(Metric metric, Long longValue) {
    if (context.getStats() != null) {
      context.getStats().addLongStat(metric, longValue);
    }
  }

  private RecordReader addFilterIfNecessary(RecordReader delegate) {
    if (!filters.hasPushdownFilters()) {
      return delegate;
    }

    if (filterCreator.filterMayChange()) {
      filters.getPushdownFilters().stream()
          .filter(c -> c.getFilter().exact())
          .forEach(c -> c.setFilterModifiedForPushdown(true));
      return delegate;
    }

    final List<LogicalExpression> logicalExpressions =
        filters.getPushdownFilters().stream()
            .filter(f -> f.getFilter().exact())
            .map(c -> c.getExpr())
            .collect(Collectors.toList());
    if (logicalExpressions.isEmpty()) {
      return delegate;
    }

    final LogicalExpression filterExpr =
        logicalExpressions.size() == 1
            ? logicalExpressions.get(0)
            : FunctionCallFactory.createBooleanOperator("and", logicalExpressions);
    return new CopyingFilteringReader(delegate, context, filterExpr);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (ValueVector v : vectorMap.values()) {
      if (v instanceof FixedWidthVector) {
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
    while (totalRecords == -1
        || (totalRecords > 0
            && totalRecords == BitVectorHelper.getNullCount(validityBuf, totalRecords))) {
      validityBuf.setOne(0, maxValidityBufSize);
      totalRecords = readEnsuringReadersReturnSameNumberOfRecords();
    }

    // if there is an equality delete filter, call it to filter out records that match its delete
    // conditions
    if (totalRecords > 0 && filters.hasEqualityDeleteFilter()) {
      filters.getEqualityDeleteFilter().filter(totalRecords);
    }

    // remove invalid rows if present
    if (totalRecords > 0 && BitVectorHelper.getNullCount(validityBuf, totalRecords) != 0) {
      totalRecords =
          useCopiersToRemoveInvalidRows
              ? removeInvalidRowsWithCopiers(totalRecords)
              : removeInvalidRows(totalRecords);
    }

    return totalRecords;
  }

  /**
   * Read from each reader and make sure number of rows in batch is the same for all the readers
   *
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
          throw new IllegalStateException(
              String.format(
                  "Inconsistent row count. Reader %s returned %d while "
                      + "previous reader returned %d",
                  recordReader.toString(), n, count));
        }
      }
    }
    return count;
  }

  /**
   * @param records
   * @return
   */
  private int removeInvalidRows(int records) {
    // TODO(DX-25408): avoid copying in certain cases
    int copyTo = 0;
    for (int i = 0; i < records; i++) {
      if (BitVectorHelper.get(validityBuf, i) == 1) {
        Preconditions.checkArgument(
            copyTo <= i, "Copying from lower index to higher index will lead to data corruption");
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

  /**
   * @param records
   * @return
   */
  private int removeInvalidRowsWithCopiers(int records) {
    // TODO(DX-25408): avoid copying in certain cases
    // This method is called only if there is atleast one invalid row

    // Create sv2 containing valid indices starting after the first invalid index
    int validRowsCount = 0;
    int firstInvalidIndex = -1;
    int sv2Count = 0;

    // Find the first invalid index, and store valid indices after firstInvalidIndex till the end of
    // the byte containing the firstInvalidIndex
    int i = 0;
    while (i < records) {
      byte validityByte = validityBuf.getByte(i >>> 3);
      // Get the list of invalid bit positions in this byte
      List<Integer> inValidBitPositions = BitSetHelper.getUnsetBitPositions(validityByte);
      if (inValidBitPositions.isEmpty()) {
        // All the bits in this validityByte are set, skip this byte
        validRowsCount += 8;
        i += 8;
      } else {
        // firstInvalidIndex is in this validityByte
        firstInvalidIndex = i + inValidBitPositions.get(0);
        validRowsCount += inValidBitPositions.get(0);
        // Copy valid bits in this byte after the firstInvalidIndex
        List<Integer> validBitPositions = BitSetHelper.getSetBitPositions(validityByte);
        for (int validIdx : validBitPositions) {
          int finalValidIdx = i + validIdx;
          if (finalValidIdx > firstInvalidIndex && finalValidIdx < records) {
            sv2.setShort(sv2Count++ * SelectionVector2.RECORD_SIZE, finalValidIdx);
            validRowsCount++;
          }
        }
        i += 8;
        break;
      }
    }

    // Iterate after the byte containing the firstInValidIndex till the last full byte
    for (; i + 8 <= records; i += 8) {
      byte validityByte = validityBuf.getByte(i >>> 3);
      // Store valid indices in sv2 vector
      List<Integer> validBitPositions = BitSetHelper.getSetBitPositions(validityByte);
      for (int validIdx : validBitPositions) {
        int finalValidIdx = i + validIdx;
        sv2.setShort(sv2Count++ * SelectionVector2.RECORD_SIZE, finalValidIdx);
        validRowsCount++;
      }
    }

    //  When records is not a multiple of 8, iterate the last validity byte
    if (i < records) {
      byte validityByte = validityBuf.getByte(i >>> 3);
      // Store valid indices in sv2 vector
      List<Integer> validBitPositions = BitSetHelper.getSetBitPositions(validityByte);
      for (int validIdx : validBitPositions) {
        int finalValidIdx = i + validIdx;
        if (finalValidIdx < records) {
          sv2.setShort(sv2Count++ * SelectionVector2.RECORD_SIZE, finalValidIdx);
          validRowsCount++;
        }
      }
    }

    // Copy starting from the first invalid index
    for (FieldBufferCopier copier : copiers) {
      copier.copy(sv2.memoryAddress(), sv2Count, new FieldBufferCopier.Cursor(firstInvalidIndex));
    }
    for (ValueVector vv : outputMutator.getVectors()) {
      vv.setValueCount(validRowsCount);
    }

    return validRowsCount;
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    List<SchemaPath> columnsToBoost = Lists.newArrayList();
    for (RecordReader recordReader : delegates) {
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

      addMetricValue(Metric.NUM_ROW_GROUPS_TRIMMED, 1L);
    }
    try {
      List<AutoCloseable> closeables = new ArrayList<>();
      closeables.addAll(delegates);
      closeables.add(inputStreamProvider);
      closeables.add(validityBuf);
      closeables.add(sv2);
      closeables.add(filters);
      closeables.add(codecFactory::release);
      AutoCloseables.close(closeables);
    } finally {
      delegates = null;
      inputStreamProvider = null;
      validityBuf = null;
    }
  }

  private void splitColumns(
      final MutableParquetMetadata footer,
      List<SchemaPath> vectorizableReaderColumns,
      List<SchemaPath> nonVectorizableReaderColumns) {
    boolean isArrowSchemaPresent =
        DremioArrowSchema.isArrowSchemaPresent(footer.getFileMetaData().getKeyValueMetaData());
    final BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
    Preconditions.checkArgument(
        block != null,
        "Parquet file '%s' footer does not have information about row group %s",
        this.readEntry.getPath(),
        readEntry.getRowGroupIndex());
    final Map<String, ColumnChunkMetaData> fieldsWithEncodingsSupportedByVectorizedReader =
        new HashMap<>();
    final List<Type> nonVectorizableTypes = new ArrayList<>();
    final List<Type> vectorizableTypes = new ArrayList<>();
    Set<String> fieldsWithPartialOrNoEncodingsSupportedByVectorizedReader = new HashSet<>();

    for (ColumnChunkMetaData c : block.getColumns()) {
      String field = c.getPath().iterator().next();
      if (!readerFactory.isSupported(c, context)) {
        // we'll skip columns we can't read.
        fieldsWithPartialOrNoEncodingsSupportedByVectorizedReader.add(field);
        fieldsWithEncodingsSupportedByVectorizedReader.remove(field);
        continue;
      }
      if (!fieldsWithPartialOrNoEncodingsSupportedByVectorizedReader.contains(field)) {
        fieldsWithEncodingsSupportedByVectorizedReader.put(field, c);
      }
    }

    MessageType schema = footer.getFileMetaData().getSchema();
    Set<String> projectedParquetColumnRootSegments = new TreeSet(String.CASE_INSENSITIVE_ORDER);
    projectedParquetColumnRootSegments.addAll(
        Sets.newHashSet(
            this.columnResolver.getProjectedParquetColumns().stream()
                .map(schemaPath -> schemaPath.getRootSegment().getNameSegment().getPath())
                .collect(Collectors.toSet())));
    for (Type parquetField : schema.getFields()) {
      if (this.ignoreSchemaLearning) {
        // check if parquet field is in projected columns, if not, then ignore it
        if (!projectedParquetColumnRootSegments.contains(parquetField.getName())) {
          continue;
        }
      }
      if (fieldsWithEncodingsSupportedByVectorizedReader.containsKey(parquetField.getName())
          && isParquetFieldVectorizable(
              fieldsWithEncodingsSupportedByVectorizedReader, parquetField, isArrowSchemaPresent)) {
        vectorizableTypes.add(parquetField);
      } else {
        nonVectorizableTypes.add(parquetField);
      }
    }

    AdditionalColumnResolver additionalColumnResolver =
        new AdditionalColumnResolver(tableSchema, columnResolver);
    paths:
    for (SchemaPath path : additionalColumnResolver.resolveColumns(block.getColumns())) {
      String name = path.getRootSegment().getNameSegment().getPath();
      for (Type type : vectorizableTypes) {
        if (type.getName().equalsIgnoreCase(name)) {
          vectorizableReaderColumns.add(path);
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

  private boolean isParquetFieldVectorizable(
      Map<String, ColumnChunkMetaData> fields, Type parquetField, boolean isArrowSchemaPresent) {
    return ((parquetField.isPrimitive()
            && isNotInt96(parquetField)
            && checkIfDecimalIsVectorizable(parquetField, fields.get(parquetField.getName())))
        || (context.getOptions().getOption(ExecConstants.ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS)
            && isComplexFieldVectorizable(parquetField, isArrowSchemaPresent)));
  }

  private boolean unionHasComplexChild(UnionVector unionVector) {
    // check if all fields of union are primitive
    for (FieldVector fieldVector : unionVector.getChildrenFromFields()) {
      if (fieldVector.getField().getType().isComplex()) {
        return true;
      }
    }
    return false;
  }

  private Type getParquetChildField(Type parquetField, String childName) {
    try {
      return parquetField.asGroupType().getType(childName);
    } catch (InvalidRecordException ire) {
      return null;
    }
  }

  private boolean isTypeVectorizable(
      ValueVector vector,
      boolean isArrowSchemaPresent,
      Type parquetField,
      boolean isParentTypeMap) {
    if (vector == null || !vector.getField().getType().isComplex()) {
      // vector doesn't exist or it is of a primitive type
      return true;
    }
    if (vector instanceof StructVector) {
      isParentTypeMap = (parquetField.getOriginalType() == OriginalType.MAP);
      // if vector is struct, then all of its children must be vectorizable
      for (FieldVector fieldVector : ((StructVector) vector).getChildrenFromFields()) {
        Type parquetChildField = getParquetChildField(parquetField, fieldVector.getName());
        if (parquetChildField == null) {
          // there is no parquet field corresponding to table field. ignore the field.
          continue;
        }
        if (!isTypeVectorizable(
            fieldVector, isArrowSchemaPresent, parquetChildField, isParentTypeMap)) {
          return false;
        }
      }
      return true;
    } else if (vector instanceof MapVector) {
      return isTypeVectorizable(
          ((MapVector) vector).getDataVector(),
          isArrowSchemaPresent,
          parquetField.asGroupType().getType(0),
          false);
    } else if (vector instanceof ListVector) {
      if (parquetField.getOriginalType() != OriginalType.LIST && !isParentTypeMap) {
        // if we get a listvector for non list parquet element, use rowise reader
        return false;
      }
      Type parquetChildField;
      if (isParentTypeMap) {
        parquetChildField = parquetField;
      } else {
        if (!LogicalListL1Converter.isSupportedSchema(parquetField.asGroupType())) {
          return false;
        }
        parquetChildField = parquetField.asGroupType().getType(0).asGroupType().getType(0);
      }
      // if vector is list, then its child element must be vectorizable
      return isTypeVectorizable(
          ((ListVector) vector).getDataVector(), isArrowSchemaPresent, parquetChildField, false);
    } else if (vector instanceof UnionVector) {
      return !isArrowSchemaPresent && !unionHasComplexChild((UnionVector) vector);
    } else {
      throw new UnsupportedOperationException("Unsupported vector " + vector.getField().getType());
    }
  }

  private boolean isComplexFieldVectorizable(Type parquetField, boolean isArrowSchemaPresent) {
    String columnName = this.columnResolver.getBatchSchemaColumnName(parquetField.getName());
    return isTypeVectorizable(
        outputMutator.getVector(columnName), isArrowSchemaPresent, parquetField, false);
  }

  private boolean isNotInt96(Type parquetField) {
    return parquetField.asPrimitiveType().getPrimitiveTypeName()
            != PrimitiveType.PrimitiveTypeName.INT96
        || schemaHelper.readInt96AsTimeStamp();
  }

  private boolean checkIfDecimalIsVectorizable(Type parquetField, ColumnChunkMetaData metadata) {
    if (parquetField.asPrimitiveType().getOriginalType() != OriginalType.DECIMAL) {
      return true;
    }

    return context.getOptions().getOption(PlannerSettings.ENABLE_VECTORIZED_PARQUET_DECIMAL);
  }

  // Returns top-level column names in Parquet file converted to lower case
  private Set<String> getParquetColumnNamesToRetain() {
    if (ColumnUtils.isStarQuery(this.columnResolver.getBatchSchemaProjectedColumns())) {
      return null;
    }

    return this.columnResolver.getProjectedParquetColumns().stream()
        .map((s) -> s.getRootSegment().getNameSegment().getPath().toLowerCase())
        .collect(Collectors.toSet());
  }

  private boolean determineFilterConditions(List<SchemaPath> nonVectorizableColumns) {
    if (!filters.hasPushdownFilters()) {
      return true;
    }
    return isConditionSet(nonVectorizableColumns);
  }

  private boolean isConditionSet(List<SchemaPath> nonVectorizableColumns) {
    if (!filters.hasPushdownFilters()) {
      return false;
    }

    final Predicate<SchemaPath> isFiltered =
        schema -> filters.getPushdownFilters().stream().anyMatch(f -> f.getPath().equals(schema));
    return nonVectorizableColumns.stream().noneMatch(isFiltered);
  }

  private MutableParquetMetadata getFooter() {
    return footer;
  }

  /**
   * Simple enum to handle different code paths with RowWise, DeprecatedVectorized and
   * HybridVectorized filters
   */
  private enum ExecutionPath {
    ROWWISE {
      @Override
      public List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) {

        List<RecordReader> returnList = new ArrayList<>();
        int rowGroupIndex = unifiedReader.readEntry.getRowGroupIndex();
        SimpleIntVector deltas = null;
        if (unifiedReader.filters.hasPositionalDeleteFilter()) {
          deltas = new SimpleIntVector("deltas", unifiedReader.context.getAllocator());
          returnList.add(
              new RowwisePositionalDeleteFilteringReader(
                  unifiedReader.context,
                  deltas,
                  unifiedReader.footer.getAccumulatedRowCount(rowGroupIndex),
                  unifiedReader.footer.getEndRowPos(rowGroupIndex),
                  unifiedReader.filters.getPositionalDeleteFilter()));
        }

        RecordReader reader =
            new ParquetRowiseReader(
                unifiedReader.context,
                unifiedReader.getFooter(),
                rowGroupIndex,
                unifiedReader.readEntry.getPath(),
                unifiedReader.projectedColumns,
                unifiedReader.fs,
                unifiedReader.schemaHelper,
                deltas,
                unifiedReader.inputStreamProvider,
                unifiedReader.codecFactory,
                false,
                unifiedReader.tableSchema);

        returnList.add(unifiedReader.addFilterIfNecessary(getWrappedReader(reader, unifiedReader)));
        return returnList;
      }
    },
    VECTORIZED {
      @Override
      public List<RecordReader> getReaders(UnifiedParquetReader unifiedReader) {
        boolean isVectorizableFilterOn =
            unifiedReader.isConditionSet(unifiedReader.nonVectorizableReaderColumns);
        final SimpleIntVector deltas;
        if (isVectorizableFilterOn
            || unifiedReader.isVectorizableNonPartitionColFilterPresent()
            || unifiedReader.filters.hasPositionalDeleteFilter()) {
          deltas = new SimpleIntVector("deltas", unifiedReader.context.getAllocator());
        } else {
          deltas = null;
        }

        List<RecordReader> returnList = new ArrayList<>();
        if (!unifiedReader.vectorizableReaderColumns.isEmpty()
            || unifiedReader.nonVectorizableReaderColumns.isEmpty()
            || unifiedReader.filters.hasPositionalDeleteFilter()) {
          RecordReader reader =
              unifiedReader.readerFactory.newReader(
                  unifiedReader.context,
                  unifiedReader.projectedColumns.cloneForSchemaPaths(
                      unifiedReader.columnResolver.getBatchSchemaColumns(
                          unifiedReader.vectorizableReaderColumns),
                      unifiedReader.isConvertedIcebergDataset),
                  unifiedReader.readEntry.getPath(),
                  unifiedReader.codecFactory,
                  unifiedReader.filters,
                  unifiedReader.filterCreator,
                  unifiedReader.dictionaryConvertor,
                  unifiedReader.enableDetailedTracing,
                  unifiedReader.getFooter(),
                  unifiedReader.readEntry.getRowGroupIndex(),
                  deltas,
                  unifiedReader.schemaHelper,
                  unifiedReader.inputStreamProvider,
                  unifiedReader.runtimeFilters,
                  unifiedReader.validityBuf,
                  unifiedReader.tableSchema,
                  unifiedReader.ignoreSchemaLearning);

          returnList.add(getWrappedReader(reader, unifiedReader));
        }
        if (!unifiedReader.nonVectorizableReaderColumns.isEmpty()) {
          RecordReader reader =
              new ParquetRowiseReader(
                  unifiedReader.context,
                  unifiedReader.getFooter(),
                  unifiedReader.readEntry.getRowGroupIndex(),
                  unifiedReader.readEntry.getPath(),
                  unifiedReader.projectedColumns.cloneForSchemaPaths(
                      unifiedReader.columnResolver.getBatchSchemaColumns(
                          unifiedReader.nonVectorizableReaderColumns),
                      unifiedReader.isConvertedIcebergDataset),
                  unifiedReader.fs,
                  unifiedReader.schemaHelper,
                  deltas,
                  unifiedReader.inputStreamProvider,
                  unifiedReader.codecFactory,
                  unifiedReader.vectorizableReaderColumns.isEmpty()
                      ? unifiedReader.tableSchema
                      : null);
          returnList.add(
              unifiedReader.vectorizableReaderColumns.isEmpty()
                  ? getWrappedReader(reader, unifiedReader)
                  : reader);
        }
        return returnList;
      }
    },
    SKIP_ALL {
      @Override
      public List<RecordReader> getReaders(final UnifiedParquetReader unifiedReader) {
        final RecordReader reader =
            new AbstractRecordReader(unifiedReader.context, Collections.emptyList()) {
              @Override
              public void setup(OutputMutator output) {}

              @Override
              public int next() {
                return 0;
              }

              @Override
              public void close() {}
            };
        return Collections.singletonList(reader);
      }
    },
    INCLUDE_ALL {
      @Override
      public List<RecordReader> getReaders(final UnifiedParquetReader unifiedReader)
          throws ExecutionSetupException {
        final MutableParquetMetadata footer = unifiedReader.getFooter();
        final List<BlockMetaData> blocks = footer.getBlocks();
        final int rowGroupIdx = unifiedReader.readEntry.getRowGroupIndex();
        if (blocks.size() <= rowGroupIdx) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid rowgroup index in read entry. Given '%d', Max '%d'",
                  rowGroupIdx, blocks.size()));
        }

        BlockMetaData block = blocks.get(rowGroupIdx);
        Preconditions.checkArgument(
            block != null,
            "Parquet file '%s' footer does not have information about row group %s",
            unifiedReader.readEntry.getPath(),
            unifiedReader.readEntry.getRowGroupIndex());
        final long rowCount = block.getRowCount();
        final long accumulatedRowCount = footer.getAccumulatedRowCount(rowGroupIdx);
        logger.debug("Row group {}, accumulated row count {}", rowGroupIdx, accumulatedRowCount);
        final BatchSchema tableSchema = unifiedReader.tableSchema;
        final RecordReader reader =
            new AbstractRecordReader(unifiedReader.context, Collections.<SchemaPath>emptyList()) {
              private long remainingRowCount = rowCount;
              private BigIntAutoIncrementer rowIndexGenerator =
                  tableSchema.findFieldIgnoreCase(ColumnUtils.ROW_INDEX_COLUMN_NAME).isPresent()
                      ? new BigIntAutoIncrementer(
                          ColumnUtils.ROW_INDEX_COLUMN_NAME, context.getTargetBatchSize(), null)
                      : null;

              @Override
              public void setup(OutputMutator output) throws ExecutionSetupException {
                if (rowIndexGenerator != null) {
                  rowIndexGenerator.setRowIndexBase(accumulatedRowCount);
                  rowIndexGenerator.setup(output);
                }
              }

              @Override
              public int next() {
                if (numRowsPerBatch > remainingRowCount) {
                  int toReturn = (int) remainingRowCount;
                  remainingRowCount = 0;
                  populateRowIndex(toReturn);
                  return toReturn;
                }

                remainingRowCount -= numRowsPerBatch;
                populateRowIndex(numRowsPerBatch);
                return numRowsPerBatch;
              }

              private void populateRowIndex(int count) {
                if (rowIndexGenerator != null) {
                  rowIndexGenerator.populate(count);
                }
              }

              @Override
              public void close() throws Exception {}
            };
        return Collections.singletonList(getWrappedReader(reader, unifiedReader));
      }
    };

    private static RecordReader getWrappedReader(
        RecordReader reader, UnifiedParquetReader unifiedReader) {
      Preconditions.checkNotNull(
          unifiedReader.readEntry.getOriginalPath(),
          "the original split file path cannot be null. ");

      // Add "fileName" system column, if the "tableSchema" explicitly requires this column.
      return unifiedReader
              .tableSchema
              .findFieldIgnoreCase(ColumnUtils.FILE_PATH_COLUMN_NAME)
              .isPresent()
          ? new AdditionalColumnsRecordReader(
              unifiedReader.context,
              reader,
              Arrays.asList(
                  new ConstantColumnPopulators.VarCharNameValuePair(
                      ColumnUtils.FILE_PATH_COLUMN_NAME,
                      unifiedReader.readEntry.getOriginalPath())),
              unifiedReader.context.getAllocator())
          : reader;
    }

    /**
     * To produce list of the RecordReaders for each enum entry
     *
     * @param unifiedReader
     * @return list of RecordReaders
     * @throws ExecutionSetupException
     */
    public abstract List<RecordReader> getReaders(UnifiedParquetReader unifiedReader)
        throws ExecutionSetupException;
  }

  private boolean isVectorizableNonPartitionColFilterPresent() {
    return this.runtimeFilters.stream()
        .flatMap(f -> f.getNonPartitionColumnFilters().stream())
        .flatMap(f -> f.getColumnsList().stream())
        .map(SchemaPath::getSimplePath)
        .anyMatch(vectorizableReaderColumns::contains);
  }

  private ExecutionPath getExecutionPath() {
    if (!vectorize || !determineFilterConditions(nonVectorizableReaderColumns)) {
      return ExecutionPath.ROWWISE;
    }

    if (vectorizableReaderColumns.isEmpty()
        && nonVectorizableReaderColumns.isEmpty()
        && !filters.hasPositionalDeleteFilter()) {
      return filterCanContainNull() ? ExecutionPath.INCLUDE_ALL : ExecutionPath.SKIP_ALL;
    }

    return ExecutionPath.VECTORIZED;
  }

  private boolean filterCanContainNull() {
    return !filters.hasPushdownFilters()
        && runtimeFilters.stream()
            .flatMap(r -> r.getNonPartitionColumnFilters().stream())
            .map(CompositeColumnFilter::getValueList)
            .filter(Objects::nonNull)
            .allMatch(ValueListFilter::isContainsNull);
  }

  public static ParquetReaderFactory getReaderFactory(SabotConfig config) {
    return config.getInstance(
        "dremio.plugins.parquet.factory", ParquetReaderFactory.class, ParquetReaderFactory.NONE);
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (runtimeFilter != null && !runtimeFilters.contains(runtimeFilter)) {
      RuntimeFilter filterWithNewNonPartColFilterList =
          RuntimeFilter.getInstanceWithNewNonPartitionColFiltersList(runtimeFilter);
      this.runtimeFilters.add(filterWithNewNonPartColFilterList);
      this.delegates.forEach(d -> d.addRuntimeFilter(filterWithNewNonPartColFilterList));
    }
  }

  /**
   * A simple RecordReader which applies positional deletes from a PositionalDeleteFilter to a delta
   * vector. This is intended to be used as a reader executed prior to ParquetRowwiseReader in the
   * ROWWISE execution path.
   */
  private static class RowwisePositionalDeleteFilteringReader extends AbstractParquetReader {

    private final long startRowPos;
    private final long endRowPos;
    private final PositionalDeleteFilter positionalDeleteFilter;

    public RowwisePositionalDeleteFilteringReader(
        OperatorContext context,
        SimpleIntVector deltas,
        long startRowPos,
        long endRowPos,
        PositionalDeleteFilter positionalDeleteFilter) {
      super(context, Collections.emptyList(), deltas);
      this.startRowPos = startRowPos;
      this.endRowPos = endRowPos;
      this.positionalDeleteFilter = Preconditions.checkNotNull(positionalDeleteFilter);
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
      deltas.allocateNew(numRowsPerBatch);
      positionalDeleteFilter.seek(startRowPos);
    }

    @Override
    public int next() {
      return positionalDeleteFilter.applyToDeltas(endRowPos, numRowsPerBatch, deltas);
    }

    @Override
    public void close() throws Exception {
      deltas.close();
    }
  }
}
