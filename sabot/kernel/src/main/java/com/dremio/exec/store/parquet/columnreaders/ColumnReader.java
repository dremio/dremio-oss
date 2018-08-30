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
package com.dremio.exec.store.parquet.columnreaders;

import java.io.IOException;

import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.dremio.common.exceptions.ExecutionSetupException;

import io.netty.buffer.ArrowBuf;

public abstract class ColumnReader<V extends ValueVector> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnReader.class);

  final DeprecatedParquetVectorizedReader parentReader;

  // Value Vector for this column
  final V valueVec;

  ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }

  // column description from the parquet library
  final ColumnDescriptor columnDescriptor;
  // metadata of the column, from the parquet library
  final ColumnChunkMetaData columnChunkMetaData;
  // status information on the current page
  PageReader pageReader;

  final SchemaElement schemaElement;
  boolean usingDictionary;

  // quick reference to see if the field is fixed length (as this requires an instanceof)
  final boolean isFixedLength;

  // counter for the total number of values read from one or more pages
  // when a batch is filled all of these values should be the same for all of the columns
  int totalValuesRead;

  // counter for the values that have been read in this pass (a single call to the next() method)
  int valuesReadInCurrentPass;

  // length of single data value in nodes, if the length is fixed
  int dataTypeLengthInBits;
  int bytesReadInCurrentPass;

  protected ArrowBuf vectorData;
  // when reading definition levels for nullable columns, it is a one-way stream of integers
  // when reading var length data, where we don't know if all of the records will fit until we've read all of them
  // we must store the last definition level an use it in at the start of the next batch
  int currDefLevel;

  // variables for a single read pass
  long readStartInBytes = 0, readLength = 0, readLengthInBits = 0, recordsReadInThisIteration = 0;

  protected ColumnReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                         ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    this.parentReader = parentReader;
    this.columnDescriptor = descriptor;
    this.columnChunkMetaData = columnChunkMetaData;
    this.isFixedLength = fixedLength;
    this.schemaElement = schemaElement;
    this.valueVec =  v;
    this.pageReader = (parentReader.getSingleStream() != null)?
      new DeprecatedSingleStreamPageReader(this, parentReader.getSingleStream(), parentReader.getHadoopPath(), columnChunkMetaData) :
      new PageReader(this, parentReader.getFileSystem(), parentReader.getHadoopPath(), columnChunkMetaData);

    if (columnDescriptor.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      if (columnDescriptor.getType() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        dataTypeLengthInBits = columnDescriptor.getTypeLength() * 8;
      } else if (columnDescriptor.getType() == PrimitiveTypeName.INT96
        && (valueVec instanceof TimeStampMilliVector || valueVec instanceof TimeStampMilliVector)) {
        // if int 96 column is being read as a Timestamp, this truncates the time format used by Impala
        // dataTypeLengthInBits is only ever used when computing offsets into the destination vector, so it
        // needs to be set to the bit width of the resulting Arrow type, usually this matches the input length
        dataTypeLengthInBits = 64;
      } else {
        dataTypeLengthInBits = DeprecatedParquetVectorizedReader.getTypeLengthInBits(columnDescriptor.getType());
      }
    }
  }

  public int getRecordsReadInCurrentPass() {
    return valuesReadInCurrentPass;
  }

  public void processPages(long recordsToReadInThisPass) throws IOException {
    reset();
    if(recordsToReadInThisPass>0) {
      do {
        determineSize(recordsToReadInThisPass, 0);

      } while (valuesReadInCurrentPass < recordsToReadInThisPass && pageReader.hasPage());
    }
    valueVec.setValueCount(valuesReadInCurrentPass);
  }

  public void clear() {
    valueVec.clear();
    pageReader.clear();
  }

  public void readValues(long recordsToRead) {
    readField(recordsToRead);

    valuesReadInCurrentPass += recordsReadInThisIteration;
    pageReader.valuesRead += recordsReadInThisIteration;
    pageReader.readPosInBytes = readStartInBytes + readLength;
  }

  protected abstract void readField(long recordsToRead);

  /**
   * Determines the size of a single value in a variable column.
   *
   * Return value indicates if we have finished a row group and should stop reading
   *
   * @param recordsReadInCurrentPass
   * @param lengthVarFieldsInCurrentRecord
   * @return - true if we should stop reading
   * @throws IOException
   */
  public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {

    boolean doneReading = readPage();
    if (doneReading) {
      return true;
    }

    doneReading = processPageData((int) recordsReadInCurrentPass);
    if (doneReading) {
      return true;
    }

    lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

    doneReading = checkVectorCapacityReached();
    if (doneReading) {
      return true;
    }

    return false;
  }

  protected void readRecords(int recordsToRead) {
    for (int i = 0; i < recordsToRead; i++) {
      readField(i);
    }
    pageReader.valuesRead += recordsToRead;
  }

  protected boolean processPageData(int recordsToReadInThisPass) throws IOException {
    readValues(recordsToReadInThisPass);
    return true;
  }

  public void updatePosition() {}

  public void updateReadyToReadPosition() {}

  public void reset() {
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    bytesReadInCurrentPass = 0;
    vectorData = valueVec.getDataBuffer();
  }

  public int capacity() {
    return (int) (valueVec.getValueCapacity() * dataTypeLengthInBits / 8.0);
  }

  // Read a page if we need more data, returns true if we need to exit the read loop
  public boolean readPage() throws IOException {
    if (!pageReader.hasPage()
        || totalValuesReadAndReadyToReadInPage() == pageReader.currentPageCount) {
      readRecords(pageReader.valuesReadyToRead);
      if (pageReader.hasPage()) {
        totalValuesRead += pageReader.currentPageCount;
      }
      if (!pageReader.next()) {
        hitRowGroupEnd();
        return true;
      }
      postPageRead();
    }
    return false;
  }

  protected int totalValuesReadAndReadyToReadInPage() {
    return pageReader.valuesRead + pageReader.valuesReadyToRead;
  }

  protected void postPageRead() {
    pageReader.valuesReadyToRead = 0;
  }

  protected void hitRowGroupEnd() {}

  protected boolean checkVectorCapacityReached() {
    return false;
  }

  // copied out of parquet library, didn't want to deal with the uneeded throws statement they had declared
  public static int readIntLittleEndian(ArrowBuf in, int offset) {
    int ch4 = in.getByte(offset) & 0xff;
    int ch3 = in.getByte(offset + 1) & 0xff;
    int ch2 = in.getByte(offset + 2) & 0xff;
    int ch1 = in.getByte(offset + 3) & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

}
