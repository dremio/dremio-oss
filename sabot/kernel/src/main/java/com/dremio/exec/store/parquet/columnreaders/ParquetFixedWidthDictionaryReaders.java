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
import java.math.BigInteger;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;

import com.dremio.common.exceptions.ExecutionSetupException;

public class ParquetFixedWidthDictionaryReaders {

  static class DictionaryIntReader extends FixedByteAlignedReader<NullableIntVector> {
    DictionaryIntReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableIntVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryFixedBinaryReader extends FixedByteAlignedReader<NullableVarBinaryVector> {
    DictionaryFixedBinaryReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);
      readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
      readLength = (int) Math.ceil(readLengthInBits / 8.0);

      if (usingDictionary) {
        Binary currDictValToWrite = null;
        for (int i = 0; i < recordsReadInThisIteration; i++){
          currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
          valueVec.setSafe(valuesReadInCurrentPass + i, currDictValToWrite.toByteBuffer(), 0,
              currDictValToWrite.length());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        int writerIndex = valueVec.getDataBuffer().writerIndex();
        valueVec.getDataBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }

      // TODO - replace this with fixed binary type in Dremio
      // now we need to write the lengths of each value
      int byteLength = dataTypeLengthInBits / 8;
      for (int i = 0; i < recordsToReadInThisPass; i++) {
        valueVec.setValueLengthSafe(valuesReadInCurrentPass + i, byteLength);
      }
    }
  }

  static class DictionaryIntDecimalReader extends FixedByteAlignedReader<NullableDecimalVector> {
    DictionaryIntDecimalReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                               ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimalVector v,
                               SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.dictionaryValueReader.readInteger()));
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        }
      }
    }
  }

  static class DictionaryTimeReader extends FixedByteAlignedReader<NullableTimeMilliVector> {
    DictionaryTimeReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableTimeMilliVector v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readInteger());
        }
      }
    }
  }

  static class DictionaryBigIntReader extends FixedByteAlignedReader<NullableBigIntVector> {
    DictionaryBigIntReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableBigIntVector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      if (usingDictionary) {
        for (int i = 0; i < recordsReadInThisIteration; i++){
          valueVec.setSafe(valuesReadInCurrentPass + i,  pageReader.dictionaryValueReader.readLong());
        }
        // Set the write Index. The next page that gets read might be a page that does not use dictionary encoding
        // and we will go into the else condition below. The readField method of the parent class requires the
        // writer index to be set correctly.
        readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        readLength = (int) Math.ceil(readLengthInBits / 8.0);
        int writerIndex = valueVec.getDataBuffer().writerIndex();
        valueVec.getDataBuffer().setIndex(0, writerIndex + (int)readLength);
      } else {
        super.readField(recordsToReadInThisPass);
      }
    }
  }

  static class DictionaryLongDecimalReader extends FixedByteAlignedReader<NullableDecimalVector> {
    DictionaryLongDecimalReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimalVector v,
                                SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          BigDecimal bigDecimal = new BigDecimal(BigInteger.valueOf(pageReader.dictionaryValueReader.readLong()));
          DecimalUtility.writeBigDecimalToArrowBuf(bigDecimal, vectorData, valuesReadInCurrentPass + i);
          valueVec.setIndexDefined(valuesReadInCurrentPass + i);
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryTimeStampReader extends FixedByteAlignedReader<NullableTimeStampMilliVector> {
    DictionaryTimeStampReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableTimeStampMilliVector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {

      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        try {
          valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readLong());
        } catch ( Exception ex) {
          throw ex;
        }
      }
    }
  }

  static class DictionaryFloat4Reader extends FixedByteAlignedReader<NullableFloat4Vector> {
    DictionaryFloat4Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat4Vector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readFloat());
      }
    }
  }

  static class DictionaryFloat8Reader extends FixedByteAlignedReader<NullableFloat8Vector> {
    DictionaryFloat8Reader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                           ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableFloat8Vector v,
                           SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    // this method is called by its superclass during a read loop
    @Override
    protected void readField(long recordsToReadInThisPass) {
      recordsReadInThisIteration = Math.min(pageReader.currentPageCount
          - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);

      for (int i = 0; i < recordsReadInThisIteration; i++){
        valueVec.setSafe(valuesReadInCurrentPass + i, pageReader.dictionaryValueReader.readDouble());
      }
    }
  }
}
