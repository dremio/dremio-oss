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
package com.dremio.exec.store.parquet2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections.CollectionUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ColumnChunkIncReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.AbstractParquetReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.ParquetColumnResolver;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ParquetRowiseReader extends AbstractParquetReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRowiseReader.class);
  private final int rowGroupIndex;
  private final String path;
  private final InputStreamProvider inputStreamProvider;
  private final CompressionCodecFactory codec;

  // same as the DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH in DeprecatedParquetVectorizedReader

  private ParquetScanProjectedColumns projectedColumns;
  private ParquetMetadata footer;
  private MessageType schema;
  private FileSystem fileSystem;
  private VectorContainerWriter writer;
  private ColumnChunkIncReadStore pageReadStore;
  private RecordReader<Void> recordReader;
  private ParquetRecordMaterializer recordMaterializer;
  private long recordCount;
  private OperatorContext operatorContext;
  private boolean readEvenIfSchemaChanges;

  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<IntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead=0;
  private List<SchemaPath> columnsNotFound=null;
  boolean noColumnsFound = false; // true if none of the columns in the projection list is found in the schema

  // See DRILL-4203
  private SchemaDerivationHelper schemaHelper;
  private VectorizedBasedFilter vectorizedBasedFilter;

  public ParquetRowiseReader(OperatorContext context, ParquetMetadata footer, int rowGroupIndex, String path,
                              ParquetScanProjectedColumns projectedColumns, FileSystem fileSystem, SchemaDerivationHelper schemaHelper,
                              SimpleIntVector deltas, InputStreamProvider inputStreamProvider, CompressionCodecFactory codec,
                              boolean readEvenIfSchemaChanges) {
    super(context, projectedColumns.getBatchSchemaProjectedColumns(), deltas);
    this.footer = footer;
    this.fileSystem = fileSystem;
    this.rowGroupIndex = rowGroupIndex;
    this.path = path;
    this.schemaHelper = schemaHelper;
    this.inputStreamProvider = inputStreamProvider;
    this.codec = codec;
    this.projectedColumns = projectedColumns;
    this.readEvenIfSchemaChanges = readEvenIfSchemaChanges;
  }

  public ParquetRowiseReader(OperatorContext context, ParquetMetadata footer, int rowGroupIndex, String path,
                             ParquetScanProjectedColumns projectedColumns, FileSystem fileSystem, SchemaDerivationHelper schemaHelper,
                             SimpleIntVector deltas, InputStreamProvider inputStreamProvider, CompressionCodecFactory codec) {
    this(context, footer, rowGroupIndex, path, projectedColumns, fileSystem, schemaHelper, deltas, inputStreamProvider, codec, false);
  }

  public ParquetRowiseReader(OperatorContext context, ParquetMetadata footer, int rowGroupIndex, String path,
                             ParquetScanProjectedColumns projectedColumns, FileSystem fileSystem, SchemaDerivationHelper schemaHelper,
                             InputStreamProvider inputStreamProvider, CompressionCodecFactory codec) {
    this(context, footer, rowGroupIndex, path, projectedColumns, fileSystem, schemaHelper, null, inputStreamProvider, codec);
  }

  public ParquetRowiseReader(OperatorContext context, ParquetMetadata footer, int rowGroupIndex, String path,
                             ParquetScanProjectedColumns projectedColumns, FileSystem fileSystem, SchemaDerivationHelper schemaHelper,
                             InputStreamProvider inputStreamProvider, CompressionCodecFactory codec, boolean readEvenIfSchemaChanges) {
    this(context, footer, rowGroupIndex, path, projectedColumns, fileSystem, schemaHelper, null, inputStreamProvider, codec, readEvenIfSchemaChanges);
  }

  public static SchemaPath convertColumnDescriptor(ParquetColumnResolver columnResolver, final MessageType schema, final ColumnDescriptor columnDescriptor) {
    List<String> path = columnResolver.convertColumnDescriptor(schema, columnDescriptor);
    String[] schemaColDesc = new String[path.size()];
    path.toArray(schemaColDesc);
    return SchemaPath.getCompoundPath(schemaColDesc);
  }

  private static MessageType getProjection(MessageType schema,
                                          Collection<SchemaPath> columns,
                                          ParquetColumnResolver columnResolver,
                                          List<SchemaPath> columnsNotFound) {
    MessageType projection = null;

    String messageName = schema.getName();
    List<ColumnDescriptor> schemaColumns = schema.getColumns();
    // parquet type.union() seems to lose ConvertedType info when merging two columns that are the same type. This can
    // happen when selecting two elements from an array. So to work around this, we use set of SchemaPath to avoid duplicates
    // and then merge the types at the end
    Set<Integer> selectedSchemaPaths = Sets.newLinkedHashSet();

    // get a list of modified columns which have the array elements removed from the schema path since parquet schema doesn't include array elements
    List<SchemaPath> modifiedColumns = Lists.newLinkedList();
    for (SchemaPath path : columns) {
      List<String> segments = Lists.newArrayList();
      PathSegment seg = path.getRootSegment();
      do {
        if (seg.isNamed()) {
          segments.add(seg.getNameSegment().getPath());
        }
      } while ((seg = seg.getChild()) != null);
      String[] pathSegments = new String[segments.size()];
      segments.toArray(pathSegments);
      SchemaPath modifiedSchemaPath = SchemaPath.getCompoundPath(pathSegments);
      modifiedColumns.add(modifiedSchemaPath);
    }

    // convert the columns in the parquet schema to a list of SchemaPath columns so that they can be compared in case insensitive manner
    // to the projection columns
    List<SchemaPath> schemaPaths = Lists.newLinkedList();
    for (ColumnDescriptor columnDescriptor : schemaColumns) {
      schemaPaths.add(convertColumnDescriptor(columnResolver, schema, columnDescriptor));
    }

    // loop through projection columns and add any columns that are missing from parquet schema to columnsNotFound list
    for (SchemaPath columnPath : modifiedColumns) {
      boolean notFound = true;
      for (int i = 0; i < schemaPaths.size(); i++) {
        if (schemaPaths.get(i).contains(columnPath)) {
          selectedSchemaPaths.add(i);
          notFound = false;
        }
      }
      if (notFound) {
        columnsNotFound.add(columnPath);
      }
    }

    // convert SchemaPaths from selectedSchemaPaths and convert to parquet type, and merge into projection schema
    for (int selected : selectedSchemaPaths) {
      Type t = getType(schemaColumns.get(selected).getPath(), 0, schema);

      if (projection == null) {
        projection = new MessageType(messageName, t);
      } else {
        projection = projection.union(new MessageType(messageName, t));
      }
    }
    return projection;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.operatorContext = context;
      schema = footer.getFileMetaData().getSchema();
      ParquetColumnResolver columnResolver = this.projectedColumns.getColumnResolver(schema);
      Schema arrowSchema;
      try {
        arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
      } catch (Exception e) {
        arrowSchema = null;
        logger.warn("Invalid Arrow Schema", e);
      }

      if (!schemaHelper.isAllowMixedDecimals()) {
        verifyDecimalTypesAreSame(output, columnResolver);
      }

      MessageType projection;

      if (isStarQuery()) {
        projection = schema;
      } else {
        columnsNotFound = new ArrayList<>();
        projection = getProjection(schema, columnResolver.getProjectedParquetColumns(), columnResolver, columnsNotFound);
        if(columnsNotFound.size()==getColumns().size()){
          noColumnsFound=true;
        }
      }

      writer = new VectorContainerWriter(output);
      if(!noColumnsFound) {
        // Discard the columns not found in the schema when create ParquetRecordMaterializer, since they have been added to output already.
        final Collection<SchemaPath> columns = columnsNotFound == null || columnsNotFound.size() == 0 ? columnResolver.getProjectedParquetColumns():
          CollectionUtils.subtract(columnResolver.getProjectedParquetColumns(), columnsNotFound);
        recordMaterializer = new ParquetRecordMaterializer(columnResolver, output, writer, projection, columns, context.getOptions(), arrowSchema, schemaHelper);
        logger.debug("Requesting schema {}", projection);
      }

      if (output.getSchemaChanged() && !readEvenIfSchemaChanges) {
        logger.info("Detected schema change. Not initializing further readers.");
        return;
      }

      boolean schemaOnly = (operatorContext == null) || (footer.getBlocks().size() == 0);

      if (!schemaOnly) {
        Path filePath = Path.of(path);

        BlockMetaData blockMetaData = footer.getBlocks().get(rowGroupIndex);

        recordCount = blockMetaData.getRowCount();

        pageReadStore = new ColumnChunkIncReadStore(recordCount,
          codec, operatorContext.getAllocator(),
          filePath, inputStreamProvider);
      }

      if (!schemaOnly && !noColumnsFound) {


        Map<ColumnPath, ColumnChunkMetaData> paths = new HashMap<>();

        for (ColumnChunkMetaData md : footer.getBlocks().get(rowGroupIndex).getColumns()) {
          paths.put(md.getPath(), md);
        }
        for (String[] path : projection.getPaths()) {
          Type type = schema.getType(path);
          if (type.isPrimitive()) {
            ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
            pageReadStore.addColumn(schema.getColumnDescription(path), md);
          }
        }

        ColumnIOFactory factory = new ColumnIOFactory(false);
        MessageColumnIO columnIO = factory.getColumnIO(projection, schema);

        if (deltas != null) {
          recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer, new UnboundRecordFilter() {
            @Override
            public RecordFilter bind(Iterable<ColumnReader> readers) {
              return vectorizedBasedFilter = new VectorizedBasedFilter(readers, deltas);
            }
          });
        } else {
          recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer);
        }
      }
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  private void verifyDecimalTypesAreSame(OutputMutator output, ParquetColumnResolver columnResolver) {
    for (ValueVector vector : output.getVectors()) {
      Field fieldInSchema = vector.getField();
      if (fieldInSchema.getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
        ArrowType.Decimal typeInTable = (ArrowType.Decimal) fieldInSchema.getType();
        Type typeInParquet = null;
        // the field in arrow schema may not be present in hive schema
        try {
          typeInParquet  = schema.getType(columnResolver.getParquetColumnName(fieldInSchema.getName()));
        } catch (InvalidRecordException e) {
        }
        if (typeInParquet == null) {
          continue;
        }
        boolean schemaMisMatch = true;
        OriginalType originalType = typeInParquet.getOriginalType();
        if (originalType.equals(OriginalType.DECIMAL) ) {
          int precision = typeInParquet
            .asPrimitiveType().getDecimalMetadata().getPrecision();
          int scale = typeInParquet.asPrimitiveType().getDecimalMetadata().getScale();
          ArrowType decimalType = new ArrowType.Decimal(precision, scale);
          if (decimalType.equals(typeInTable)) {
            schemaMisMatch = false;
          }
        }
        if (schemaMisMatch) {
          throw UserException.schemaChangeError().message("Mixed types "+ fieldInSchema.getType()
            + " , " + typeInParquet + " is not supported.")
            .build(logger);
        }
      }
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    close();
    String message = "Error in parquet reader (complex).\nMessage: " + s +
      "\nFile path: " + path + "\nParquet Metadata: " + footer;
    throw new RuntimeException(message, e);
  }

  private static Type getType(String[] pathSegments, int depth, MessageType schema) {
    Type type = schema.getType(Arrays.copyOfRange(pathSegments, 0, depth + 1));
    if (depth + 1 == pathSegments.length) {
      return type;
    } else {
      Preconditions.checkState(!type.isPrimitive());
      return new GroupType(type.getRepetition(), type.getName(), type.getOriginalType(), getType(pathSegments, depth + 1, schema));
    }
  }

  private long totalRead = 0;

  @Override
  public int next() {
    int count = 0;
    long maxRecordCount = 0;
    try {
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (noColumnsFound) {
        if (mockRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount()) {
          return 0;
        }
        long recordsToRead = 0;
        recordsToRead = Math.min(numRowsPerBatch, footer.getBlocks().get(rowGroupIndex).getRowCount() - mockRecordsRead);
        writer.setValueCount((int)recordsToRead);
        mockRecordsRead += recordsToRead;
        totalRead += recordsToRead;
        return (int) recordsToRead;
      }

      maxRecordCount = numRowsPerBatch;
      if (deltas != null) {
        maxRecordCount = deltas.getValueCount();
        vectorizedBasedFilter.reset();
      }
      while (count < maxRecordCount && totalRead < recordCount) {
        recordMaterializer.setPosition(count);
        recordReader.read();
        count++;
        totalRead++;
      }
      writer.setValueCount(count);
      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (final ValueVector vv : nullFilledVectors) {
          vv.setValueCount(count);
        }
      }
      return count;
    } catch (Throwable t) {
      throw UserException.dataWriteError(t)
          .message("Failed to read data from parquet file")
          .addContext("File path", path)
          .addContext("Rowgroup index", rowGroupIndex)
          .addContext("Delta vector present, size (if present)", deltas != null ? "yes, " + deltas.getValueCount() : "no")
          .addContext("Max no. of rows trying to read", maxRecordCount)
          .addContext("No. of rows read so far in current iteration", count)
          .addContext("No. of rows read so far in current rowgroup", totalRead)
          .addContext("Max no. rows in current rowgroup", recordCount)
          .addContext("Footer %s", footer)
          .build(logger);
    }
  }

  @Override
  public void close() {
    try {
      if (pageReadStore != null) {
        pageReadStore.close();
        pageReadStore = null;
      }
    } catch (Exception e) {
      logger.warn("Failure while closing PageReadStore", e);
    }
  }

  /**
   * Helper filter class to filter out records based on deltas vector provided
   * by VectorizedParquetReader (ParquetVectorizedReader)
   */
  private static class VectorizedBasedFilter implements RecordFilter {

    private final Iterable<ColumnReader> readers;
    private int index = -1;
    private int runningDelta = Integer.MAX_VALUE;
    private int maxIndex;
    private SimpleIntVector deltas;

    public VectorizedBasedFilter(Iterable<ColumnReader> readers, SimpleIntVector deltas) {
      this.readers = readers;
      this.deltas = deltas;
    }

    public void reset() {
      index = 0;
      maxIndex = deltas.getValueCount();
      if (maxIndex > 0) {
        runningDelta = deltas.get(0);
      }
    }

    @Override
    public boolean isMatch() {
      if (runningDelta == 0 && index++ < maxIndex) {
        if (index < maxIndex) {
          runningDelta = deltas.get(index);
        }
        return true;
      }
      runningDelta--;
      return false;
    }
  }

  @Override
  public boolean supportsSkipAllQuery(){
    return true;
  }

}
