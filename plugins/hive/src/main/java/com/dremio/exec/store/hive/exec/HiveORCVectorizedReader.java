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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.RecordReaderUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.hive.ORCScanFilter;
import com.dremio.exec.store.hive.exec.HiveORCCopiers.ORCCopier;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

import io.netty.buffer.ArrowBuf;

/**
 * Use vectorized reader provided by the Hive to read ORC files. We copy one column completely at a time,
 * instead of one row at time.
 */
public class HiveORCVectorizedReader extends HiveAbstractReader {

  private org.apache.hadoop.hive.ql.io.orc.RecordReader hiveOrcReader;
  private ORCCopier[] copiers;

  private String[] selectedColNames;

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
    final FileSplit fSplit = (FileSplit)inputSplit;
    final Path path = fSplit.getPath();

    final OrcFile.ReaderOptions opts = OrcFile.readerOptions(jobConf);
    final Reader hiveReader = OrcFile.createReader(path, opts);

    final List<OrcProto.Type> types = hiveReader.getTypes();
    final Reader.Options options = new Reader.Options();
    long offset = fSplit.getStart();
    long length = fSplit.getLength();
    options.range(offset, length);
    options.include(OrcInputFormat.genIncludedColumns(types, jobConf, true));
    options.zeroCopyPoolShim(new HiveORCZeroCopyShim(context.getAllocator()));

    selectedColNames = getColumns().stream().map(x -> x.getAsUnescapedPath().toLowerCase()).toArray(String[]::new);

    if (filter != null) {
      final ORCScanFilter orcScanFilter = (ORCScanFilter) filter;
      final SearchArgument sarg = orcScanFilter.getSarg();
      options.searchArgument(sarg, OrcInputFormat.getSargColumnNames(selectedColNames, types, options.getInclude(), true));
    }

    hiveOrcReader = hiveReader.rowsOptions(options);
    hiveBatch = createVectorizedRowBatch(partitionOI);

    final List<Integer> projectedColOrdinals = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    copiers = HiveORCCopiers.createCopiers(projectedColOrdinals, vectors, hiveBatch);

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

      while (outputIdx < numRowsPerBatch && hiveOrcReader.hasNext()) {
        offset = 0;
        hiveOrcReader.nextBatch(hiveBatch);
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

  /**
   * Helper method that creates {@link VectorizedRowBatch}. For each selected column an input vector is created in the
   * batch. For unselected columns the vector entry is going to be null. The order of input vectors in batch should
   * match the order the columns in ORC file.
   *
   * @param rowOI Used to find the ordinal of the selected column.
   * @return
   */
  private VectorizedRowBatch createVectorizedRowBatch(StructObjectInspector rowOI) {
    final List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    final VectorizedRowBatch result = new VectorizedRowBatch(fieldRefs.size());
    for (int j = 0; j < fieldRefs.size(); j++) {
      final ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();
      switch (foi.getCategory()) {
        case PRIMITIVE: {
          final PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
          switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
              result.cols[j] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
              break;
            case TIMESTAMP:
              result.cols[j] = new TimestampColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
              break;
            case FLOAT:
            case DOUBLE:
              result.cols[j] = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
              break;
            case BINARY:
            case STRING:
            case CHAR:
            case VARCHAR:
              result.cols[j] = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
              break;
            case DECIMAL:
              DecimalTypeInfo tInfo = (DecimalTypeInfo) poi.getTypeInfo();
              result.cols[j] = new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                  tInfo.precision(), tInfo.scale()
              );
              break;
            default:
              throw UserException.unsupportedError()
                  .message("Vectorized ORC reader is not supported for datatype: %s", poi.getPrimitiveCategory())
                  .build(logger);
          }
          break;
        }

        default:
          throw UserException.unsupportedError()
              .message("Vectorized ORC reader is not supported for datatype: %s", foi.getCategory())
              .build(logger);
      }
    }

    result.numCols = fieldRefs.size();
    result.reset();
    return result;
  }

  @Override
  public void close() throws IOException {
    if (hiveOrcReader != null) {
      hiveOrcReader.close();
      hiveOrcReader = null;
    }

    super.close();
  }

  private static class HiveORCZeroCopyShim implements org.apache.orc.Reader.ZeroCopyPoolShim {
    private static final class ByteBufferWrapper {
      private final ByteBuffer byteBuffer;

      ByteBufferWrapper(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
      }

      @Override
      public boolean equals(Object rhs) {
        return (rhs instanceof ByteBufferWrapper) && (this.byteBuffer == ((ByteBufferWrapper) rhs).byteBuffer);
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(byteBuffer);
      }
    }

    private final Map<ByteBufferWrapper, ArrowBuf> directBufMap = new HashMap<>();
    private final BufferAllocator allocator;
    private final RecordReaderUtils.ByteBufferAllocatorPool heapAllocator;

    HiveORCZeroCopyShim(BufferAllocator allocator) {
      this.allocator = allocator;
      this.heapAllocator = new RecordReaderUtils.ByteBufferAllocatorPool();
    }

    @Override
    public void clear() {
      // Releasing any remaining direct buffers that were not released due to errors.
      for (ArrowBuf buf : directBufMap.values()) {
        buf.release();
      }
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      if (!direct) {
        return heapAllocator.getBuffer(false, length);
      }
      ArrowBuf buf = allocator.buffer(length);
      ByteBuffer retBuf = buf.nioBuffer(0, length);
      directBufMap.put(new ByteBufferWrapper(retBuf), buf);
      return retBuf;
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      if (!buffer.isDirect()) {
        heapAllocator.putBuffer(buffer);
        return;
      }
      ArrowBuf buf = directBufMap.remove(new ByteBufferWrapper(buffer));
      if (buf != null) {
        buf.release();
      }
    }
  }
}
