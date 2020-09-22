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

import java.io.IOException;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;

import com.dremio.common.exceptions.ExecutionSetupException;

public abstract class VarLengthValuesColumn<V extends ValueVector> extends VarLengthColumn {

  Binary currLengthDeterminingDictVal;
  Binary currDictValToWrite;
  VariableWidthVector variableWidthVector;

  VarLengthValuesColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    variableWidthVector = (VariableWidthVector) valueVec;
    if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      usingDictionary = true;
    }
    else {
      usingDictionary = false;
    }
  }

  public abstract boolean setSafe(int index, ArrowBuf bytes, int start, int length);

  @Override
  protected void readField(long recordToRead) {
    dataTypeLengthInBits = ((BaseVariableWidthVector)variableWidthVector).getValueLength(valuesReadInCurrentPass);
    // again, I am re-purposing the unused field here, it is a length n BYTES, not nodes
    boolean success = setSafe(valuesReadInCurrentPass, pageReader.pageData,
        (int) pageReader.readPosInBytes + 4, dataTypeLengthInBits);
    assert success : String.format("setSafe() failed\n" +
        "  valuesReadInCurrentPass: %d\n" +
        "  readPosInBytes: %d\n" +
        "  dataTypeLengthInBits: %d",
      valuesReadInCurrentPass, (int) pageReader.readPosInBytes + 4, dataTypeLengthInBits);
    updatePosition();
  }

  @Override
  public void updateReadyToReadPosition() {
    pageReader.readyToReadPosInBytes += dataTypeLengthInBits + 4;
    pageReader.valuesReadyToRead++;
    currLengthDeterminingDictVal = null;
  }

  @Override
  public void updatePosition() {
    pageReader.readPosInBytes += dataTypeLengthInBits + 4;
    bytesReadInCurrentPass += dataTypeLengthInBits;
    valuesReadInCurrentPass++;
  }

  @Override
  public boolean skipReadyToReadPositionUpdate() {
    return false;
  }

  @Override
  protected boolean readAndStoreValueSizeInformation() throws IOException {
    // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
    if (usingDictionary) {
      if (currLengthDeterminingDictVal == null) {
        currLengthDeterminingDictVal = pageReader.dictionaryLengthDeterminingReader.readBytes();
      }
      currDictValToWrite = currLengthDeterminingDictVal;
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = currLengthDeterminingDictVal.length();
    } else {
      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = pageReader.pageData.getInt((int) pageReader.readyToReadPosInBytes);
    }

    // this should not fail
    ((BaseVariableWidthVector)variableWidthVector).setValueLengthSafe(valuesReadInCurrentPass + pageReader.valuesReadyToRead,
        dataTypeLengthInBits);
    return false;
  }

}
