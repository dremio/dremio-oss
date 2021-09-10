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
package com.dremio.exec.store.parquet.columnreaders;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.common.util.MajorTypeHelper.getFieldForNameAndMajorType;
import static com.dremio.exec.store.parquet.columnreaders.ColumnReaderFactory.createFixedColumnReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.RepeatedValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;

import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetReaderStats;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.Streams;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DeprecatedParquetVectorizedReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DeprecatedParquetVectorizedReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final char DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH = 32*1024;

  private int bitWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private List<ColumnReader<?>> columnStatuses;
  private FileSystem fileSystem;
  Path fsPath;
  private VarLenBinaryReader varLengthReader;
  private MutableParquetMetadata footer;
  private BlockMetaData rowGroupMetadata;
  // This is a parallel list to the columns list above, it is used to determine the subset of the project
  // pushdown columns that do not appear in this file
  private boolean[] columnsFound;
  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<IntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead;

  private final CompressionCodecFactory codecFactory;
  final int rowGroupIndex;
  long totalRecordsRead;
  SeekableInputStream singleInputStream;

  private ParquetScanProjectedColumns projectedColumns;
  private final SchemaDerivationHelper schemaHelper;
  private final GlobalDictionaries globalDictionaries;
  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns;

  public DeprecatedParquetVectorizedReader(
    OperatorContext operatorContext,
    String path,
    int rowGroupIndex,
    FileSystem fs,
    CompressionCodecFactory codecFactory,
    MutableParquetMetadata footer,
    ParquetScanProjectedColumns projectedColumns,
    SchemaDerivationHelper schemHelper,
    Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns,
    GlobalDictionaries globalDictionaries) throws ExecutionSetupException {
    super(operatorContext, projectedColumns.getBatchSchemaProjectedColumns());
    this.fsPath = Path.of(path);
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.footer = footer;
    this.schemaHelper = schemHelper;
    this.globalDictionaryColumns = globalDictionaryColumns == null? Collections.<String, GlobalDictionaryFieldInfo>emptyMap() : globalDictionaryColumns;
    this.globalDictionaries = globalDictionaries;
    this.singleInputStream = null;
    this.projectedColumns = projectedColumns;
  }

  /**
   * Flag indicating if the old non-standard data format appears
   * in this file, see DRILL-4203.
   *
   * @return true if the dates are corrupted and need to be corrected
   */
  public ParquetReaderUtility.DateCorruptionStatus getDateCorruptionStatus() {
    return schemaHelper.getDateCorruptionStatus();
  }

  public boolean readInt96AsTimeStamp() {
    return schemaHelper.readInt96AsTimeStamp();
  }

  public CompressionCodecFactory getCodecFactory() {
    return codecFactory;
  }

  public Path getFsPath() {
    return fsPath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public int getBitWidthAllFixedFields() {
    return bitWidthAllFixedFields;
  }

  public long getBatchSize() { // in nodes
    return numBytesPerBatch * 8;
  }

  public List<ColumnReader<?>> getColumnStatuses() {
    return columnStatuses;
  }

  public VarLenBinaryReader getVarLengthReader() {
    return varLengthReader;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in pageDataByteArray of the type
   */
  public static int getTypeLengthInBits(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 64;
      case INT32:   return 32;
      case BOOLEAN: return 1;
      case FLOAT:   return 32;
      case DOUBLE:  return 64;
      case INT96:   return 96;
      // binary and fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

  private boolean fieldSelected(Field field) {
    // TODO - not sure if this is how we want to represent this
    // for now it makes the existing tests pass, simply selecting
    // all available data if no columns are provided
    if (isStarQuery()) {
      return true;
    }

    int i = 0;
    for (SchemaPath expr : getColumns()) {
      if ( field.getName().equalsIgnoreCase(expr.getAsUnescapedPath())) {
        columnsFound[i] = true;
        return true;
      }
      i++;
    }
    return false;
  }

  public OperatorContext getOperatorContext() {
    return context;
  }

  /**
   * Returns data type length for a given {@see ColumnDescriptor} and it's corresponding
   * {@see SchemaElement}. Neither is enough information alone as the max
   * repetition level (indicating if it is an array type) is in the ColumnDescriptor and
   * the length of a fixed width field is stored at the schema level.
   *
   * @return the length if fixed width, else -1
   */
  private int getDataTypeLength(ColumnDescriptor column, SchemaElement se) {
    if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      if (column.getMaxRepetitionLevel() > 0) {
        return -1;
      }
      if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        return se.getType_length() * 8;
      } else {
        return getTypeLengthInBits(column.getType());
      }
    } else {
      return -1;
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    if (!isStarQuery()) {
      columnsFound = new boolean[getColumns().size()];
      nullFilledVectors = new ArrayList<>();
    }
    columnStatuses = new ArrayList<>();
//    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    Schema arrowSchema = parseArrowSchema();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    int columnsToScan = 0;
    mockRecordsRead = 0;
    rowGroupMetadata = footer.getBlocks().get(rowGroupIndex);
    Preconditions.checkArgument(rowGroupMetadata != null, "Parquet footer does not contain information about row group");

    Field field;
//    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
    FileMetaData fileMetaData;

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, rowGroupMetadata.getRowCount(),
        fsPath.toURI().getPath());
    totalRecordsRead = 0;

    boolean useSingleStream = context.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM);
    if (useSingleStream || columns.size()  >= context.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD)) {
      try {
        singleInputStream = Streams.wrap(fileSystem.open(fsPath));
      } catch (IOException ioe) {
        throw new ExecutionSetupException("Error opening or reading metadata for parquet file at location: "
          + fsPath.getName(), ioe);
      }
    }

    // TODO - figure out how to deal with this better once we add nested reading, note also look where this map is used below
    // store a map from column name to converted types if they are non-null
    Map<String, SchemaElement> schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer.getFileMetaData(), rowGroupMetadata);

    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      SchemaElement se = schemaElements.get(column.getPath()[0]);
      final SchemaPath schemaPath = toSchemaPath(column.getPath());
      String fieldName = schemaPath.getAsUnescapedPath();
      final MajorType mt;
      final int dataTypeLength;
      if (globalDictionaryColumns.containsKey(fieldName)) {
        mt = MajorType.newBuilder().setMinorType(MinorType.INT).setMode(getDataMode(column)).build();
        dataTypeLength = getTypeLengthInBits(PrimitiveType.PrimitiveTypeName.INT32);
      } else {
        mt = ParquetToMinorTypeConverter.toMajorType(
          column.getType(),
          se.getType_length(),
          getDataMode(column),
          se,
          context.getOptions(),
          arrowSchema == null ? null : arrowSchema.findField(fieldName),
          schemaHelper.readInt96AsTimeStamp()
        );
        dataTypeLength = getDataTypeLength(column, se);
      }
      field = getFieldForNameAndMajorType(fieldName, mt);
      if ( ! fieldSelected(field)) {
        continue;
      }
      columnsToScan++;
      if (dataTypeLength == -1) {
        allFieldsFixedLength = false;
      } else {
        bitWidthAllFixedFields += dataTypeLength;
      }
    }
//    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    int rowsPerBatch = 0;
    if (columnsToScan != 0 && allFieldsFixedLength) {
      rowsPerBatch = (int) Math.min(Math.min(numBytesPerBatch / bitWidthAllFixedFields,
          rowGroupMetadata.getColumns().get(0).getValueCount()), 65535);
    }
    else {
      rowsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
    }
    this.numRowsPerBatch = Math.min(this.numRowsPerBatch, rowsPerBatch);

    try {
      ValueVector vector;
      SchemaElement schemaElement;
      final ArrayList<VarLengthColumn<?>> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength;
      // the column chunk meta-data is not guaranteed to be in the same order as the columns in the schema
      // a map is constructed for fast access to the correct columnChunkMetadata to correspond
      // to an element in the schema
      Map<String, Integer> columnChunkMetadataPositionsInList = new HashMap<>();

      int colChunkIndex = 0;
      for (ColumnChunkMetaData colChunk : rowGroupMetadata.getColumns()) {
        columnChunkMetadataPositionsInList.put(Arrays.toString(colChunk.getPath().toArray()), colChunkIndex);
        colChunkIndex++;
      }
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = rowGroupMetadata.getColumns().get(columnChunkMetadataPositionsInList.get(Arrays.toString(column.getPath())));
        schemaElement = schemaElements.get(column.getPath()[0]);
        Field childArrowField = arrowSchema == null ? null : arrowSchema.findField(schemaElement.getName());
        if(childArrowField != null){
          field = childArrowField;
        } else {
          MajorType type = ParquetToMinorTypeConverter.toMajorType(
            column.getType(),
            schemaElement.getType_length(),
            getDataMode(column),
            schemaElement,
            context.getOptions(),
            childArrowField,
            schemaHelper.readInt96AsTimeStamp()
          );
          field = getFieldForNameAndMajorType(toFieldName(column.getPath()), type);
        }

        // the field was not requested to be read
        if ( !fieldSelected(field)) {
          continue;
        }

        final CompleteType type = CompleteType.fromField(field);
        final boolean dictionaryEncoded = globalDictionaryColumns.containsKey(toFieldName(column.getPath()));
        if (dictionaryEncoded) {
          vector = output.addField(field, IntVector.class);
          columnStatuses.add(new RawDictionaryReader(this, (int) numRowsPerBatch, column, columnChunkMetaData, true,
            (IntVector)vector, schemaElement, globalDictionaries));
        } else {
          fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
          vector = output.addField(field, type.getValueVectorClass());
          if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
            if (column.getMaxRepetitionLevel() > 0) {
              final RepeatedValueVector repeatedVector = (RepeatedValueVector) vector;
              ColumnReader<?> dataReader = ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength,
                column, columnChunkMetaData, (int) numRowsPerBatch,
                repeatedVector.getDataVector(), schemaElement, type);
              varLengthColumns.add(new FixedWidthRepeatedReader(this, dataReader,
                getTypeLengthInBits(column.getType()), -1, column, columnChunkMetaData, false, repeatedVector, schemaElement));
            } else {
              columnStatuses.add(createFixedColumnReader(
                this,
                fieldFixedLength,
                column,
                columnChunkMetaData,
                (int) numRowsPerBatch,
                vector,
                schemaElement,
                type));
            }
          } else {
            // create a reader and add it to the appropriate list
            varLengthColumns.add(ColumnReaderFactory.getReader(this, -1, column, columnChunkMetaData, false, vector, schemaElement));
          }
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns);

      if (!isStarQuery()) {
        List<SchemaPath> projectedColumns = Lists.newArrayList(getColumns());
        SchemaPath col;
        for (int i = 0; i < columnsFound.length; i++) {
          col = projectedColumns.get(i);
          assert col!=null;
          if ( ! columnsFound[i] && !col.equals(STAR_COLUMN)) {
            nullFilledVectors.add((IntVector)output.addField(new Field(col.getAsUnescapedPath(), true,
                    getArrowMinorType(MinorType.INT).getType(), null),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(getArrowMinorType(TypeProtos.MinorType.INT))));

          }
        }
      }
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  private Schema parseArrowSchema() throws ExecutionSetupException {
    try {
      Schema arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
      return arrowSchema;
    } catch (IOException e) {
      logger.warn("Invalid Arrow Schema", e);
    }
    return null;
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    throw new RuntimeException(message, e);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, (int) numRowsPerBatch, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  SeekableInputStream getSingleStream() {
    return singleInputStream;
  }

  private String toFieldName(String[] paths) {
    return SchemaPath.getCompoundPath(paths).getAsUnescapedPath();
  }

  private SchemaPath toSchemaPath(String[] paths) {
    return SchemaPath.getCompoundPath(paths);
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (final ColumnReader<?> column : columnStatuses) {
      column.valuesReadInCurrentPass = 0;
    }
    for (final VarLengthColumn<?> r : varLengthReader.columns) {
      r.valuesReadInCurrentPass = 0;
    }
  }

 public void readAllFixedFields(long recordsToRead) throws IOException {

   for (ColumnReader<?> crs : columnStatuses) {
     crs.processPages(recordsToRead);
   }
 }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    try {
      ColumnReader<?> firstColumnStatus;
      if (columnStatuses.size() > 0) {
        firstColumnStatus = columnStatuses.iterator().next();
      }
      else{
        if (varLengthReader.columns.size() > 0) {
          firstColumnStatus = varLengthReader.columns.iterator().next();
        }
        else{
          firstColumnStatus = null;
        }
      }
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (firstColumnStatus == null) {
        if (mockRecordsRead == rowGroupMetadata.getRowCount()) {
          return 0;
        }
        recordsToRead = Math.min(numRowsPerBatch, rowGroupMetadata.getRowCount() - mockRecordsRead);
        for (final ValueVector vv : nullFilledVectors ) {
          vv.setValueCount( (int) recordsToRead);
        }
        mockRecordsRead += recordsToRead;
        totalRecordsRead += recordsToRead;
        return (int) recordsToRead;
      }

      if (allFieldsFixedLength) {
        recordsToRead = firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead;
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
      }
      recordsToRead = Math.min(recordsToRead, numRowsPerBatch);

      if (allFieldsFixedLength) {
        readAllFixedFields(recordsToRead);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead);
      }

      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (final ValueVector vv : nullFilledVectors ) {
          vv.setValueCount(firstColumnStatus.getRecordsReadInCurrentPass());
        }
      }

//      logger.debug("So far read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, hadoopPath.toUri().getPath());
      totalRecordsRead += firstColumnStatus.getRecordsReadInCurrentPass();
      return firstColumnStatus.getRecordsReadInCurrentPass();
    } catch (Exception e) {
      handleAndRaise("\nHadoop path: " + fsPath.toURI().getPath() +
        "\nTotal records read: " + totalRecordsRead +
        "\nMock records read: " + mockRecordsRead +
        "\nRecords to read: " + recordsToRead +
        "\nRow group index: " + rowGroupIndex +
        "\nRecords in row group: " + rowGroupMetadata.getRowCount(), e);
    }

    // this is never reached
    return 0;
  }

  @Override
  public void close() throws Exception {
    logger.debug("Read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, fsPath.toURI().getPath());
    // enable this for debugging when it is know that a whole file will be read
    // limit kills upstream operators once it has enough records, so this assert will fail
//    assert totalRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount();
    if (columnStatuses != null) {
      for (final ColumnReader<?> column : columnStatuses) {
        column.clear();
      }
      columnStatuses.clear();
      columnStatuses = null;
    }

    codecFactory.release();

    if (varLengthReader != null) {
      for (final VarLengthColumn r : varLengthReader.columns) {
        r.clear();
      }
      varLengthReader.columns.clear();
      varLengthReader = null;
    }

    if (singleInputStream != null) {
      singleInputStream.close();
    }

    if(parquetReaderStats != null) {
      if (logger.isTraceEnabled()) {
        logger.trace("ParquetTrace,Summary,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
          fsPath,
          parquetReaderStats.numDictPageHeaders,
          parquetReaderStats.numPageHeaders,
          parquetReaderStats.numDictPageLoads,
          parquetReaderStats.numPageLoads,
          parquetReaderStats.numDictPagesDecompressed,
          parquetReaderStats.numPagesDecompressed,
          parquetReaderStats.totalDictPageHeaderBytes,
          parquetReaderStats.totalPageHeaderBytes,
          parquetReaderStats.totalDictPageReadBytes,
          parquetReaderStats.totalPageReadBytes,
          parquetReaderStats.totalDictDecompressedBytes,
          parquetReaderStats.totalDecompressedBytes,
          parquetReaderStats.timeDictPageHeaders,
          parquetReaderStats.timePageHeaders,
          parquetReaderStats.timeDictPageLoads,
          parquetReaderStats.timePageLoads,
          parquetReaderStats.timeDictPagesDecompressed,
          parquetReaderStats.timePagesDecompressed);
      }
      parquetReaderStats=null;
    }
  }


  @Override
  public boolean supportsSkipAllQuery(){
    return true;
  }

}
