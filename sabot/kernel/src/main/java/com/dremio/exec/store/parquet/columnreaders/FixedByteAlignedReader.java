/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.math.BigDecimal;

import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableVectorDefinitionSetter;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.joda.time.DateTimeConstants;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.parquet.ParquetReaderUtility;

import io.netty.buffer.ArrowBuf;


class FixedByteAlignedReader<V extends ValueVector> extends ColumnReader<V> {

  protected ArrowBuf bytebuf;


  FixedByteAlignedReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                         boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  // this method is called by its superclass during a read loop
  @Override
  protected void readField(long recordsToReadInThisPass) {

    recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

    readStartInBytes = pageReader.readPosInBytes;
    readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
    readLength = (int) Math.ceil(readLengthInBits / 8.0);

    bytebuf = pageReader.pageData;
    // vectorData is assigned by the superclass read loop method
    writeData();
    for (int i = 0; i < recordsToReadInThisPass; i++) {
      ((NullableVectorDefinitionSetter) valueVec.getMutator()).setIndexDefined(i);
    }
  }

  protected void writeData() {
    vectorData.writeBytes(bytebuf, (int) readStartInBytes, (int) readLength);
  }

  public static class FixedBinaryReader extends FixedByteAlignedReader<VariableWidthVector> {
    // TODO - replace this with fixed binary type in Dremio
    VariableWidthVector castedVector;

    FixedBinaryReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                      VariableWidthVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, true, v, schemaElement);
      castedVector = v;
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {
      // we can use the standard read method to transfer the data
      super.readField(recordsToReadInThisPass);
      // TODO - replace this with fixed binary type in Dremio
      // now we need to write the lengths of each value
      int byteLength = dataTypeLengthInBits / 8;
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        castedVector.getMutator().setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
      }
    }

  }

  public static abstract class ConvertedReader<V extends ValueVector> extends FixedByteAlignedReader<V> {

    protected int dataTypeLengthInBytes;

    ConvertedReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    public void writeData() {
      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsReadInThisIteration; i++) {
        addNext((int)readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    /**
     * Reads from bytebuf, converts, and writes to buffer
     * @param start the index in bytes to start reading from
     * @param index the index of the ValueVector
     */
    abstract void addNext(int start, int index);
  }

  public static class DateReader extends ConvertedReader<NullableDateMilliVector> {

    private final NullableDateMilliVector.Mutator mutator;

    DateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, NullableDateMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      mutator = v.getMutator();
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue =  pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      mutator.set(index, intValue * (long) DateTimeConstants.MILLIS_PER_DAY);
    }
  }

  /**
   * Old versions of Drill were writing a non-standard format for date. See DRILL-4203
   */
  public static class CorruptDateReader extends ConvertedReader<NullableDateMilliVector> {

    private final NullableDateMilliVector.Mutator mutator;

    CorruptDateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                      boolean fixedLength, NullableDateMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      mutator = v.getMutator();
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue = pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      mutator.set(index, (intValue - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY);
    }

  }

  /**
   * Old versions of Drill were writing a non-standard format for date. See DRILL-4203
   * <p/>
   * For files that lack enough metadata to determine if the dates are corrupt, we must just
   * correct values when they look corrupt during this low level read.
   */
  public static class CorruptionDetectingDateReader extends ConvertedReader<NullableDateMilliVector> {

    private final NullableDateMilliVector.Mutator mutator;

    CorruptionDetectingDateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                                  boolean fixedLength, NullableDateMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      mutator = v.getMutator();
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue = pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      if (intValue > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
        mutator.set(index, (intValue - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY);
      } else {
        mutator.set(index, intValue * (long) DateTimeConstants.MILLIS_PER_DAY);
      }
    }

  }

  public static class DecimalReader extends ConvertedReader<NullableDecimalVector> {

    DecimalReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                  boolean fixedLength, NullableDecimalVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromArrowBuf(bytebuf, start, schemaElement.getScale());
      DecimalUtility.writeBigDecimalToArrowBuf(intermediate, valueVec.getBuffer(), index);
      valueVec.getMutator().setIndexDefined(index);
    }
  }

}
