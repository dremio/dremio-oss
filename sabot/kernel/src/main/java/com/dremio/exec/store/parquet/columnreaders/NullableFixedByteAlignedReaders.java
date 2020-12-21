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

import static com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.getDateTimeValueFromBinary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.joda.time.DateTimeConstants;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.parquet.ParquetReaderUtility;

public class NullableFixedByteAlignedReaders {

  static class NullableFixedByteAlignedReader<V extends ValueVector> extends NullableColumnReader<V> {
    protected ArrowBuf bytebuf;

    NullableFixedByteAlignedReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.bytebuf = pageReader.pageData;

      // fill in data.
      vectorData.setBytes(vectorData.writerIndex(), bytebuf, (int) readStartInBytes, (int)
        readLength);
      vectorData.writerIndex(vectorData.writerIndex() + (int)readLength);
    }
  }

  /**
   * Class for reading the fixed length byte array type in parquet. Currently Dremio does not have
   * a fixed length binary type, so this is read into a varbinary with the same size recorded for
   * each value.
   */
  static class NullableFixedBinaryReader extends NullableFixedByteAlignedReader<VarBinaryVector> {
    NullableFixedBinaryReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                              ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.bytebuf = pageReader.pageData;
      if (usingDictionary) {
        Binary currDictValToWrite;
        for (int i = 0; i < recordsReadInThisIteration; i++){
          currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
          ByteBuffer buf = currDictValToWrite.toByteBuffer();
          valueVec.setSafe(valuesReadInCurrentPass + i, buf, buf.position(),
              currDictValToWrite.length());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        long writerIndex = valueVec.getDataBuffer().writerIndex();
        valueVec.getDataBuffer().setInt(0, LargeMemoryUtil.checkedCastToInt(writerIndex + readLength));
      } else {
        super.readField(recordsToReadInThisPass);
        // TODO - replace this with fixed binary type in Dremio
        // for now we need to write the lengths of each value
        int byteLength = dataTypeLengthInBits / 8;
        for (int i = 0; i < recordsToReadInThisPass; i++) {
          valueVec.setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
        }
      }
    }
  }

  /**
   * Class for reading parquet fixed binary type INT96, which is used for storing hive,
   * impala timestamp values with nanoseconds precision (12 bytes). So it reads such values as a Dremio timestamp (8 bytes).
   */
  static class NullableFixedBinaryAsTimeStampReader extends NullableFixedByteAlignedReader<TimeStampMilliVector> {
    /**
     * The width of each element of the TimeStampMilliVector is 8 byte(s).
     */
    private static final int TIMESTAMP_LENGTH_IN_BITS = 64;

    NullableFixedBinaryAsTimeStampReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                              ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeStampMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {
      this.bytebuf = pageReader.pageData;
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          Binary binaryTimeStampValue = pageReader.dictionaryValueReader.readBytes();
          valueVec.setSafe(valuesReadInCurrentPass + i, getDateTimeValueFromBinary(binaryTimeStampValue));
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++) {
          Binary binaryTimeStampValue = pageReader.valueReader.readBytes();
          valueVec.setSafe(valuesReadInCurrentPass + i, getDateTimeValueFromBinary(binaryTimeStampValue));
        }
      }
      // The nanos precision is cut to millis. Therefore the length of single timestamp value is 8 bytes(s)
      // instead of 12 byte(s).
      dataTypeLengthInBits = TIMESTAMP_LENGTH_IN_BITS;
    }
  }

  static class NullableDictionaryIntReader extends NullableColumnReader<IntVector> {

    NullableDictionaryIntReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, IntVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryDecimal9Reader extends NullableColumnReader<DecimalVector> {

    NullableDictionaryDecimal9Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                     ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, DecimalVector v,
                                     SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.dictionaryValueReader.readInteger()));
          /* this will swap bytes as we are writing to the buffer of DecimalVector */
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i, DecimalVector.TYPE_WIDTH);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.valueReader.readInteger()));
          /* this will swap bytes as we are writing to the buffer of DecimalVector */
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i, DecimalVector.TYPE_WIDTH);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        }
      }
    }
  }

  static class NullableDictionaryTimeReader extends NullableColumnReader<TimeMilliVector> {

    NullableDictionaryTimeReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                     ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeMilliVector v,
                                     SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readInteger());
        }
      }
    }
  }

  static class NullableDictionaryBigIntReader extends NullableColumnReader<BigIntVector> {

    NullableDictionaryBigIntReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, BigIntVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readLong());
        }
      }
    }
  }

  static class NullableDictionaryTimeStampReader extends NullableColumnReader<TimeStampMilliVector> {

    NullableDictionaryTimeStampReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, TimeStampMilliVector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readLong());
        }
      }
    }
  }
  static class NullableDictionaryDecimal18Reader extends NullableColumnReader<DecimalVector> {

    NullableDictionaryDecimal18Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, DecimalVector v,
                                      SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.dictionaryValueReader.readLong()));
          /* this will swap bytes as we are writing to the buffer of DecimalVector */
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i, DecimalVector.TYPE_WIDTH);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.valueReader.readLong()));
          /* this will swap bytes as we are writing to the buffer of DecimalVector */
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i, DecimalVector.TYPE_WIDTH);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        }
      }
    }
  }
  static class NullableDictionaryFloat4Reader extends NullableColumnReader<Float4Vector> {

    NullableDictionaryFloat4Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float4Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readFloat());
        }
      }
    }
  }

  static class NullableDictionaryFloat8Reader extends NullableColumnReader<Float8Vector> {

    NullableDictionaryFloat8Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Float8Vector v,
                                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      if (usingDictionary) {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
        }
      } else {
        for (int i = 0; i < recordsToReadInThisPass; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.valueReader.readDouble());
        }
      }
    }
  }

  static abstract class NullableConvertedReader<V extends ValueVector> extends NullableFixedByteAlignedReader<V> {

    protected int dataTypeLengthInBytes;

    NullableConvertedReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    protected void readField(long recordsToReadInThisPass) {

      this.bytebuf = pageReader.pageData;

      dataTypeLengthInBytes = (int) Math.ceil(dataTypeLengthInBits / 8.0);
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        addNext((int) readStartInBytes + i * dataTypeLengthInBytes, i + valuesReadInCurrentPass);
      }
    }

    abstract void addNext(int start, int index);
  }

  public static class NullableDateReader extends NullableConvertedReader<DateMilliVector> {
    NullableDateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                       boolean fixedLength, DateMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue =  pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      valueVec.set(index, intValue * (long) DateTimeConstants.MILLIS_PER_DAY);
    }

  }

  /**
   * Old versions of Drill were writing a non-standard format for date. See DRILL-4203
   */
  public static class NullableCorruptDateReader extends NullableConvertedReader<DateMilliVector> {

    NullableCorruptDateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
        boolean fixedLength, DateMilliVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue =  pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      valueVec.set(index, (intValue - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY);
    }

  }

  /**
   * Old versions of Drill were writing a non-standard format for date. See DRILL-4203
   *
   * For files that lack enough metadata to determine if the dates are corrupt, we must just
   * correct values when they look corrupt during this low level read.
   */
  public static class CorruptionDetectingNullableDateReader extends NullableConvertedReader<DateMilliVector> {

    DateMilliVector dateVector;

    CorruptionDetectingNullableDateReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize,
        ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
        boolean fixedLength, DateMilliVector v, SchemaElement schemaElement)
            throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      dateVector = v;
    }

    @Override
    void addNext(int start, int index) {
      int intValue;
      if (usingDictionary) {
        intValue =  pageReader.dictionaryValueReader.readInteger();
      } else {
        intValue = readIntLittleEndian(bytebuf, start);
      }

      if (intValue > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
        dateVector.set(index, (intValue - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY);
      } else {
        dateVector.set(index, intValue * (long) DateTimeConstants.MILLIS_PER_DAY);
      }
    }
  }

  public static class NullableDecimalReader extends NullableConvertedReader<DecimalVector> {
    NullableDecimalReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                          boolean fixedLength, DecimalVector v, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    @Override
    void addNext(int start, int index) {
      /* data read from Parquet into the bytebuf is already in BE format, no need
       * swap bytes to construct BigDecimal. only when we write BigDecimal to
       * data buffer of decimal vector, we need to swap bytes which the DecimalUtility
       * function already does.
       */
      BigDecimal intermediate = DecimalHelper.getBigDecimalFromBEArrowBuf(bytebuf, index, schemaElement.getScale());
      /* this will swap bytes as we are writing to the buffer of DecimalVector */
      DecimalUtility.writeBigDecimalToArrowBuf(intermediate, valueVec.getDataBuffer(), index, DecimalVector.TYPE_WIDTH);
      valueVec.setIndexDefined(index);
    }
  }

}

