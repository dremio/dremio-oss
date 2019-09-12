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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ValueVector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.DataReaderProperties;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.HivePluginOptions;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.hive.exec.HiveORCCopiers.ORCCopier;
import com.dremio.exec.store.hive.exec.apache.HadoopFileSystemWrapper;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;

/**
 * Use vectorized reader provided by the Hive to read ORC files. We copy one column completely at a time,
 * instead of one row at time.
 */
public class HiveORCVectorizedReader extends HiveAbstractReader {

  /**
   * For transactional orc files, the row data is stored in the struct vector at position 5
   */
  static final int TRANS_ROW_COLUMN_INDEX = 5;
  private org.apache.hadoop.hive.ql.io.orc.RecordReader hiveOrcReader;
  private ORCCopier[] copiers;
  private DremioORCRecordUtils.DefaultDataReader dataReader;

  /**
   * Hive vectorized ORC reader reads into this batch. It is a heap based structure and reused until the reader exhaust
   * records. Most of the heap structures remain constant size on heap, but the variable width structures may get
   * reallocated if there is not enough space.
   */
  private VectorizedRowBatch hiveBatch;
  // non-zero value indicates partially read batch in previous iteration.
  private int offset;

  public HiveORCVectorizedReader(final HiveTableXattr tableAttr, final SplitAndPartitionInfo split,
      final List<SchemaPath> projectedColumns, final OperatorContext context, final JobConf jobConf,
      final SerDe tableSerDe, final StructObjectInspector tableOI, final SerDe partitionSerDe,
      final StructObjectInspector partitionOI, final ScanFilter filter, final Collection<List<String>> referencedTables,
      final UserGroupInformation readerUgi) {
    super(tableAttr, split, projectedColumns, context, jobConf, tableSerDe, tableOI, partitionSerDe, partitionOI, filter,
      referencedTables, readerUgi);
  }

  private int[] getOrdinalIdsOfSelectedColumns(List< OrcProto.Type > types, List<Integer> selectedColumns, boolean isOriginal) {
    int rootColumn = isOriginal ? 0 : TRANS_ROW_COLUMN_INDEX + 1;
    int[] ids = new int[types.size()];
    OrcProto.Type root = types.get(rootColumn);

    // iterating over only direct children
    for(int i = 0; i < root.getSubtypesCount(); ++i) {
      if (selectedColumns.contains(i)) {
        // find the position of this column in the types list
        ids[i] = root.getSubtypes(i);
      }
    }

    return ids;
  }

  class SearchResult {
    public int index;
    public ObjectInspector oI;
  }

  /*
    PreOrder tree traversal
    position.index will contain preorder tree traversal index starting at root=0
  */
  private static boolean searchAllFields(final ObjectInspector rootOI,
                                         final String name,
                                         final int[] childCounts,
                                         SearchResult position
                                         ) {
    Category category = rootOI.getCategory();
    if (category == Category.STRUCT) {
      position.index++; // first child is immediately next to parent
      StructObjectInspector sOi = (StructObjectInspector) rootOI;
      for (StructField sf : sOi.getAllStructFieldRefs()) {
        // We depend on the fact that caller takes care of calling current method
        // once for each segment in the selected column path. So, we should always get
        // searched field as immediate child
        if (position.index >= childCounts.length) {
          // input schema has more columns than what reader can read
          return false;
        }
        if (sf.getFieldName().equalsIgnoreCase(name)) {
          position.oI = sf.getFieldObjectInspector();
          return true;
        } else {
          position.index += childCounts[position.index];
        }
      }
    } else if (category == Category.MAP) {
      position.index++; // first child is immediately next to parent
      if (position.index >= childCounts.length) {
        // input schema has more columns than what reader can read
        return false;
      }
      if (name.equalsIgnoreCase(HiveUtilities.MAP_KEY_FIELD_NAME)) {
        ObjectInspector kOi = ((MapObjectInspector) rootOI).getMapKeyObjectInspector();
        position.oI = kOi;
        return true;
      }
      position.index += childCounts[position.index];
      if (position.index >= childCounts.length) {
        // input schema has more columns than what reader can read
        return false;
      }
      if (name.equalsIgnoreCase(HiveUtilities.MAP_VALUE_FIELD_NAME)) {
        ObjectInspector vOi = ((MapObjectInspector) rootOI).getMapValueObjectInspector();
        position.oI = vOi;
        return true;
      }
    }
    return false;
  }

  // Takes SchemaPath and sets included bits of only fields in selected schema path
  // and all children of last segment
  // Example: if table schema is  <col1: int, col2:struct<f1:int, f2:string>, col3: string>
  // then childCounts will be [6, 1, 3, 1, 1, 1]
  // calling this method for co2.f2 will set include[2] for struct and include[4] for field f2
  private void getIncludedColumnsFromTableSchema(ObjectInspector rootOI, int rootColumn, SchemaPath selectedField, int[] childCounts, boolean[] include) {
    SearchResult searchResult = new SearchResult();
    searchResult.index = rootColumn;
    searchResult.oI = null;

    List<String> nameSegments = selectedField.getNameSegments();
    ListIterator<String> listIterator = nameSegments.listIterator();
    while (listIterator.hasNext()) {
      String name = listIterator.next();
      boolean found = searchAllFields(rootOI, name, childCounts, searchResult);
      if (found) {
        rootColumn = searchResult.index;
        rootOI = searchResult.oI;
        if (rootColumn < include.length) {
          if (listIterator.hasNext()) {
            include[rootColumn] = true;
          } else {
            int childCount = childCounts[rootColumn];
            for (int child = 0; child < childCount; ++child) {
              include[rootColumn + child] = true;
            }
          }
        }
      } else {
        break;
      }
    }
  }

  private void getIncludedColumnsFromTableSchema(ObjectInspector oi, int rootColumn, int[] childCounts, boolean[] include) {
    if (rootColumn < include.length) {
      include[rootColumn] = true;
    }
    Collection<SchemaPath> selectedColumns = getColumns();
    for (SchemaPath selectedField: selectedColumns) {
      getIncludedColumnsFromTableSchema(oi, rootColumn, selectedField, childCounts, include);
    }
  }

  /*
    For each root, populate total number of nodes in the tree starting from it
   */
  private int getChildCountsFromTableSchema(ObjectInspector rootOI, int position, int[] counts) {
    if (position >= counts.length) {
      return 0;
    }
    Category category = rootOI.getCategory();
    switch (category) {
      case PRIMITIVE:
        counts[position] = 1;
        return counts[position];
      case LIST:
        // total count is children count and 1 extra for itself
        counts[position] = getChildCountsFromTableSchema(((ListObjectInspector)rootOI).getListElementObjectInspector(),
          position + 1, counts) + 1;
        return counts[position];
      case STRUCT: {
        // total count is children count and 1 extra for itself
        int totalCount = 1;
        int childPosition = position + 1;
        StructObjectInspector sOi = (StructObjectInspector) rootOI;
        for (StructField sf : sOi.getAllStructFieldRefs()) {
          int childCount = getChildCountsFromTableSchema(sf.getFieldObjectInspector(),
            childPosition, counts);
          childPosition += childCount;
          totalCount += childCount;
        }
        counts[position] = totalCount;
        return counts[position];
      }
      case MAP: {
        // total count is children count and 1 extra for itself
        int totalCount = 1;
        int childPosition = position + 1;
        ObjectInspector kOi = ((MapObjectInspector) rootOI).getMapKeyObjectInspector();
        int childCount = getChildCountsFromTableSchema(kOi, childPosition, counts);
        childPosition += childCount;
        totalCount += childCount;
        ObjectInspector vOi = ((MapObjectInspector) rootOI).getMapValueObjectInspector();
        childCount = getChildCountsFromTableSchema(vOi, childPosition, counts);
        totalCount += childCount;
        counts[position] = totalCount;
        return counts[position];
      }
      case UNION: {
        // total count is children count and 1 extra for itself
        int totalCount = 1;
        int childPosition = position + 1;
        for (ObjectInspector fOi : ((UnionObjectInspector) rootOI).getObjectInspectors()) {
          int childCount = getChildCountsFromTableSchema(fOi, childPosition, counts);
          childPosition += childCount;
          totalCount += childCount;
        }
        counts[position] = totalCount;
        return counts[position];
      }
      default:
        throw UserException.unsupportedError()
          .message("Vectorized ORC reader is not supported for datatype: %s", category)
          .build(logger);
    }
  }

  @Override
  protected void internalInit(InputSplit inputSplit, JobConf jobConf, ValueVector[] vectors) throws IOException {
    final OrcSplit fSplit = (OrcSplit)inputSplit;
    final Path path = fSplit.getPath();

    final OrcFile.ReaderOptions opts = OrcFile.readerOptions(jobConf);

    // TODO: DX-16001 make enabling async configurable.
    final FileSystem fs = new HadoopFileSystemWrapper(jobConf, path.getFileSystem(jobConf), this.context.getStats());
    opts.filesystem(fs);
    final Reader hiveReader = OrcFile.createReader(path, opts);

    final List<OrcProto.Type> types = hiveReader.getTypes();


    final Reader.Options options = new Reader.Options();
    long offset = fSplit.getStart();
    long length = fSplit.getLength();
    options.schema(fSplit.isOriginal() ? hiveReader.getSchema() : hiveReader.getSchema().getChildren().get(TRANS_ROW_COLUMN_INDEX));
    options.range(offset, length);
    boolean[] include = new boolean[types.size()];
    int[] childCounts = new int[types.size()];

    getChildCountsFromTableSchema(finalOI, fSplit.isOriginal() ? 0 : TRANS_ROW_COLUMN_INDEX + 1, childCounts);
    getIncludedColumnsFromTableSchema(finalOI, fSplit.isOriginal() ? 0 : TRANS_ROW_COLUMN_INDEX + 1, childCounts, include);
    include[0] = true; // always include root. reader always includes, but setting it explicitly here.

    options.include(include);
    Boolean zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(jobConf);
    Boolean useDirectMemory = context.getOptions().getOption(HivePluginOptions.HIVE_ORC_READER_USE_DIRECT_MEMORY);
    dataReader = DremioORCRecordUtils.createDefaultDataReader(context.getAllocator(), DataReaderProperties.builder()
      .withBufferSize(hiveReader.getCompressionSize())
      .withCompression(hiveReader.getCompressionKind())
      .withFileSystem(fs)
      .withPath(path)
      .withTypeCount(types.size())
      .withZeroCopy(zeroCopy)
      .build(), useDirectMemory);
    options.dataReader(dataReader);

    String[] selectedColNames = getColumns().stream().map(x -> x.getAsUnescapedPath().toLowerCase()).toArray(String[]::new);

    // there is an extra level of nesting in the transactional tables
    if (!fSplit.isOriginal()) {
      selectedColNames = ArrayUtils.addAll(new String[]{"row"}, selectedColNames);
    }

    if (filter != null) {
      final HiveProxyingOrcScanFilter orcScanFilter = (HiveProxyingOrcScanFilter) filter;
      final SearchArgument sarg = HiveUtilities.decodeSearchArgumentFromBase64(orcScanFilter.getProxiedOrcScanFilter().getKryoBase64EncodedFilter());
      options.searchArgument(sarg, OrcInputFormat.getSargColumnNames(selectedColNames, types, options.getInclude(), fSplit.isOriginal()));
    }

    hiveOrcReader = hiveReader.rowsOptions(options);
    StructObjectInspector orcFileRootOI = (StructObjectInspector) hiveReader.getObjectInspector();
    if (!fSplit.isOriginal()) {
      orcFileRootOI = (StructObjectInspector)orcFileRootOI.getAllStructFieldRefs().get(TRANS_ROW_COLUMN_INDEX).getFieldObjectInspector();
    }
    hiveBatch = createVectorizedRowBatch(orcFileRootOI, fSplit.isOriginal());

    final List<Integer> projectedColOrdinals = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    final int[] ordinalIdsFromOrcFile = getOrdinalIdsOfSelectedColumns(types, projectedColOrdinals, fSplit.isOriginal());
    HiveORCCopiers.HiveColumnVectorData columnVectorData = new HiveORCCopiers.HiveColumnVectorData(include, childCounts);

    copiers = HiveORCCopiers.createCopiers(columnVectorData,
                                              projectedColOrdinals, ordinalIdsFromOrcFile,
                                              vectors, hiveBatch, fSplit.isOriginal(), this.operatorContextOptions);

    // Store the number of vectorized columns for stats/to find whether vectorized ORC reader is used or not
    context.getStats().setLongStat(Metric.NUM_VECTORIZED_COLUMNS, vectors.length);
  }

  @Override
  protected int populateData() {
    try {
      final int numRowsPerBatch = (int) this.numRowsPerBatch;

      int outputIdx = 0;

      // Consume the left over records from previous iteration
      if (offset > 0 && offset < hiveBatch.size) {
        int toRead = Math.min(hiveBatch.size - offset, numRowsPerBatch - outputIdx);
        copy(offset, toRead, outputIdx);
        outputIdx += toRead;
        offset += toRead;
      }

      while (outputIdx < numRowsPerBatch && hiveOrcReader.nextBatch(hiveBatch)) {
        offset = 0;
        int toRead = Math.min(hiveBatch.size, numRowsPerBatch - outputIdx);
        copy(offset, toRead, outputIdx);
        outputIdx += toRead;
        offset = toRead;
      }

      return outputIdx;
    } catch (Throwable t) {
      throw createExceptionWithContext("Failed to read data from ORC file", t);
    }
  }

  private void copy(final int inputIdx, final int count, final int outputIdx) {
    for (ORCCopier copier : copiers) {
      copier.copy(inputIdx, count, outputIdx);
    }
  }
  private boolean isSupportedType(Category category) {
    return (category == Category.PRIMITIVE ||
      category == Category.LIST ||
      category == Category.STRUCT ||
      category == Category.MAP ||
      category == Category.UNION);
  }

  private List<ColumnVector> getVectors(StructObjectInspector rowOI) {
    return rowOI.getAllStructFieldRefs()
      .stream()
      .map((Function<StructField, ColumnVector>) structField -> {
        Category category = structField.getFieldObjectInspector().getCategory();
        if (!isSupportedType(category)) {
          throw UserException.unsupportedError()
            .message("Vectorized ORC reader is not supported for datatype: %s", category)
            .build(logger);
        }
        return getColumnVector(structField.getFieldObjectInspector());
      })
      .collect(Collectors.toList());

  }

  private ColumnVector getColumnVector(ObjectInspector oi) {
    Category category = oi.getCategory();
    switch (category) {

      case PRIMITIVE:
        return getPrimitiveColumnVector((PrimitiveObjectInspector)oi);
      case LIST:
        return getListColumnVector((ListObjectInspector)oi);
      case STRUCT:
        return getStructColumnVector((StructObjectInspector)oi);
      case MAP:
        return getMapColumnVector((MapObjectInspector)oi);
      case UNION:
        return getUnionColumnVector((UnionObjectInspector)oi);
      default:
        throw UserException.unsupportedError()
          .message("Vectorized ORC reader is not supported for datatype: %s", category)
          .build(logger);
    }
  }
  private ColumnVector getUnionColumnVector(UnionObjectInspector uoi) {
    ArrayList<ColumnVector> vectors = new ArrayList<>();
    List<? extends ObjectInspector> members = uoi.getObjectInspectors();
    for (ObjectInspector unionField: members) {
      vectors.add(getColumnVector(unionField));
    }
    ColumnVector[] columnVectors = vectors.toArray(new ColumnVector[0]);
    return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, columnVectors);
  }


  private ColumnVector getMapColumnVector(MapObjectInspector moi) {
    ColumnVector keys = getColumnVector(moi.getMapKeyObjectInspector());
    ColumnVector values = getColumnVector(moi.getMapValueObjectInspector());
    return new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE, keys, values);
  }
  private ColumnVector getStructColumnVector(StructObjectInspector soi) {
    ArrayList<ColumnVector> vectors = new ArrayList<>();
    List<? extends StructField> members = soi.getAllStructFieldRefs();
    for (StructField structField: members) {
      vectors.add(getColumnVector(structField.getFieldObjectInspector()));
    }
    ColumnVector[] columnVectors = vectors.toArray(new ColumnVector[0]);
    return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, columnVectors);
  }
  private ColumnVector getListColumnVector(ListObjectInspector loi) {
    ColumnVector lecv = getColumnVector(loi.getListElementObjectInspector());
    return new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE, lecv);
  }
  private ColumnVector getPrimitiveColumnVector(PrimitiveObjectInspector poi) {
      switch (poi.getPrimitiveCategory()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
        return new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      case TIMESTAMP:
        return new TimestampColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      case DECIMAL:
        DecimalTypeInfo tInfo = (DecimalTypeInfo) poi.getTypeInfo();
        return new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
          tInfo.precision(), tInfo.scale()
        );
      default:
        throw UserException.unsupportedError()
          .message("Vectorized ORC reader is not supported for datatype: %s", poi.getPrimitiveCategory())
          .build(logger);
      }
  }

  /**
   * Helper method that creates {@link VectorizedRowBatch}. For each selected column an input vector is created in the
   * batch. For unselected columns the vector entry is going to be null. The order of input vectors in batch should
   * match the order the columns in ORC file.
   *
   * @param rowOI Used to find the ordinal of the selected column.
   * @return
   */
  private VectorizedRowBatch createVectorizedRowBatch(StructObjectInspector rowOI, boolean isOriginal) {
    final List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    final List<ColumnVector> vectors = getVectors(rowOI);

    final VectorizedRowBatch result = new VectorizedRowBatch(fieldRefs.size());

    ColumnVector[] vectorArray =  vectors.toArray(new ColumnVector[0]);

    if (!isOriginal) {
      vectorArray = createTransactionalVectors(vectorArray);
    }

    result.cols = vectorArray;
    result.numCols = fieldRefs.size();
    result.reset();
    return result;
  }

  private ColumnVector[] createTransactionalVectors(ColumnVector[] dataVectors) {
    ColumnVector[] transVectors = new ColumnVector[6];

    transVectors[0] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    transVectors[1] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    transVectors[2] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    transVectors[3] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    transVectors[4] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);

    transVectors[5] = new StructColumnVector(dataVectors.length, dataVectors);

    return transVectors;
  }

  @Override
  public void close() throws IOException {
    if (hiveOrcReader != null) {
      hiveOrcReader.close();
      hiveOrcReader = null;
    }

    if (dataReader != null) {
      if (dataReader.isRemoteRead()) {
        context.getStats().addLongStat(ScanOperator.Metric.NUM_REMOTE_READERS, 1);
      } else {
        context.getStats().addLongStat(ScanOperator.Metric.NUM_REMOTE_READERS, 0);
      }
      dataReader = null;
    }

    super.close();
  }

}
