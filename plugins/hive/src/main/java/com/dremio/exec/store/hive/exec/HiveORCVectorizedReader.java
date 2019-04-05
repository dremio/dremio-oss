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
import java.util.List;
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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcProto;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.hive.ORCScanFilter;
import com.dremio.exec.store.hive.exec.HiveORCCopiers.ORCCopier;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

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

  /**
   * Hive vectorized ORC reader reads into this batch. It is a heap based structure and reused until the reader exhaust
   * records. Most of the heap structures remain constant size on heap, but the variable width structures may get
   * reallocated if there is not enough space.
   */
  private VectorizedRowBatch hiveBatch;
  // non-zero value indicates partially read batch in previous iteration.
  private int offset;

  public HiveORCVectorizedReader(final HiveTableXattr tableAttr, final DatasetSplit split,
      final List<SchemaPath> projectedColumns, final OperatorContext context, final JobConf jobConf,
      final SerDe tableSerDe, final StructObjectInspector tableOI, final SerDe partitionSerDe,
      final StructObjectInspector partitionOI, final ScanFilter filter) {
    super(tableAttr, split, projectedColumns, context, jobConf, tableSerDe, tableOI, partitionSerDe, partitionOI, filter);
  }

  @Override
  protected void internalInit(InputSplit inputSplit, JobConf jobConf, ValueVector[] vectors) throws IOException {
    final OrcSplit fSplit = (OrcSplit)inputSplit;
    final Path path = fSplit.getPath();

    final OrcFile.ReaderOptions opts = OrcFile.readerOptions(jobConf);
    final FileSystem fs = FileSystemWrapper.get(path, jobConf);
    opts.filesystem(fs);
    final Reader hiveReader = OrcFile.createReader(path, opts);

    final List<OrcProto.Type> types = hiveReader.getTypes();
    final Reader.Options options = new Reader.Options();
    long offset = fSplit.getStart();
    long length = fSplit.getLength();
    options.schema(fSplit.isOriginal() ? hiveReader.getSchema() : hiveReader.getSchema().getChildren().get(TRANS_ROW_COLUMN_INDEX));
    options.range(offset, length);
    boolean[] include = OrcInputFormat.genIncludedColumns(types, jobConf, fSplit.isOriginal());
    // for transactional tables (non original), we need to handle the additional transactional metadata columns
    if (!fSplit.isOriginal()) {
      // we include the top level struct, but exclude the transactional metadata columns
      include = ArrayUtils.addAll(new boolean[]{true, false, false, false, false, false}, include);
    }
    options.include(include);
    options.zeroCopyPoolShim(new HiveORCZeroCopyShim(context.getAllocator()));

    String[] selectedColNames = getColumns().stream().map(x -> x.getAsUnescapedPath().toLowerCase()).toArray(String[]::new);

    // there is an extra level of nesting in the transactional tables
    if (!fSplit.isOriginal()) {
      selectedColNames = ArrayUtils.addAll(new String[]{"row"}, selectedColNames);
    }

    if (filter != null) {
      final ORCScanFilter orcScanFilter = (ORCScanFilter) filter;
      final SearchArgument sarg = orcScanFilter.getSarg();
      options.searchArgument(sarg, OrcInputFormat.getSargColumnNames(selectedColNames, types, options.getInclude(), fSplit.isOriginal()));
    }

    hiveOrcReader = hiveReader.rowsOptions(options);
    hiveBatch = createVectorizedRowBatch((StructObjectInspector) hiveReader.getObjectInspector(), fSplit.isOriginal());

    final List<Integer> projectedColOrdinals = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    copiers = HiveORCCopiers.createCopiers(projectedColOrdinals, vectors, hiveBatch, fSplit.isOriginal());

    // Store the number of vectorized columns for stats/to find whether vectorized ORC reader is used or not
    context.getStats().setLongStat(Metric.NUM_VECTORIZED_COLUMNS, vectors.length);
  }

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

  private List<ColumnVector> getVectors(StructObjectInspector rowOI) {
    return rowOI.getAllStructFieldRefs()
      .stream()
      .map((Function<StructField, ColumnVector>) structField -> {
        Category category = structField.getFieldObjectInspector().getCategory();
        if (category != Category.PRIMITIVE) {
          throw UserException.unsupportedError()
            .message("Vectorized ORC reader is not supported for datatype: %s", category)
            .build(logger);
        }
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) structField.getFieldObjectInspector();
        return getColumnVector(poi);
      })
      .collect(Collectors.toList());

  }

  private ColumnVector getColumnVector(PrimitiveObjectInspector poi) {
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

    super.close();
  }

}
