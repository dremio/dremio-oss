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

import java.io.IOException;
import java.util.Map;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.google.common.collect.Maps;

/**
 * Read local dictionary ids and map them to global dictionary id.
 */
public class RawDictionaryReader extends NullableColumnReader<NullableIntVector> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RawDictionaryReader.class);
  private final int[] localIdToGlobalId;
  private final int dictionaryWidthBits;

  public RawDictionaryReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize,
                             ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                             boolean fixedLength, NullableIntVector valueVector, SchemaElement schemaElement,
                             GlobalDictionaries globalDictionaries) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, valueVector, schemaElement);
    // outgoing data type nodes
    dataTypeLengthInBits = DeprecatedParquetVectorizedReader.getTypeLengthInBits(PrimitiveType.PrimitiveTypeName.INT32);
    localIdToGlobalId = new int[pageReader.dictionary.getMaxId() + 1];
    final VectorContainer vectorContainer = globalDictionaries.getDictionaries().get(schemaElement.getName());
    switch (schemaElement.getType()) {
      case INT32: {
        final Map<Integer, Integer> valueLookup = Maps.newHashMap();
        final NullableIntVector intVector = vectorContainer.getValueAccessorById(NullableIntVector.class, 0).getValueVector();
        for (int i = 0; i < vectorContainer.getRecordCount(); ++i) {
          valueLookup.put(intVector.getAccessor().get(i), i);
        }
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = valueLookup.get(pageReader.dictionary.decodeToInt(i));
        }
      }
      break;

      case INT64: {
        final Map<Long, Integer> valueLookup = Maps.newHashMap();
        final NullableBigIntVector longVector = vectorContainer.getValueAccessorById(NullableBigIntVector.class, 0).getValueVector();
        for (int i = 0; i < vectorContainer.getRecordCount(); ++i) {
          valueLookup.put(longVector.getAccessor().get(i), i);
        }
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = valueLookup.get(pageReader.dictionary.decodeToLong(i));
        }
      }
      break;

      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
      case BYTE_ARRAY: {
        final Map<Binary, Integer> valueLookup = Maps.newHashMap();
        final NullableVarBinaryVector binaryVector = vectorContainer.getValueAccessorById(NullableVarBinaryVector.class, 0).getValueVector();
        for (int i = 0; i < vectorContainer.getRecordCount(); ++i) {
          valueLookup.put(Binary.fromConstantByteArray(binaryVector.getAccessor().get(i)), i);
        }
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = valueLookup.get(pageReader.dictionary.decodeToBinary(i));
        }
      }
      break;

      case FLOAT: {
        final Map<Float, Integer> valueLookup = Maps.newHashMap();
        final NullableFloat4Vector floarVector = vectorContainer.getValueAccessorById(NullableFloat4Vector.class, 0).getValueVector();
        for (int i = 0; i < vectorContainer.getRecordCount(); ++i) {
          valueLookup.put(floarVector.getAccessor().get(i), i);
        }
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = valueLookup.get(pageReader.dictionary.decodeToFloat(i));
        }
      }
      break;

      case DOUBLE: {
        final Map<Double, Integer> valueLookup = Maps.newHashMap();
        final NullableFloat8Vector doubleVector = vectorContainer.getValueAccessorById(NullableFloat8Vector.class, 0).getValueVector();
        for (int i = 0; i < vectorContainer.getRecordCount(); ++i) {
          valueLookup.put(doubleVector.getAccessor().get(i), i);
        }
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = valueLookup.get(pageReader.dictionary.decodeToDouble(i));
        }
      }
      break;

      case BOOLEAN: {
        for (int i = 0; i <= pageReader.dictionary.getMaxId(); ++i) {
          localIdToGlobalId[i] = pageReader.dictionary.decodeToBoolean(i) ? 1 : 0;
        }
      }
      break;

      default:
        break;
    }
    this.dictionaryWidthBits =  BytesUtils.getWidthFromMaxInt(pageReader.dictionary.getMaxId() - 1);
  }

  @Override
  public void processPages(long recordsToReadInThisPass) throws IOException {
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    vectorData = valueVec.getDataBuffer();

    int runLength = -1;     // number of non-null records in this pass.
    int nullRunLength = -1; // number of consecutive null records that we read.
    int currentDefinitionLevel = -1;
    int readCount = 0; // the record number we last read.
    int writeCount = 0; // the record number we last wrote to the value vector.
    // This was previously the indexInOutputVector variable
    boolean haveMoreData; // true if we have more data and have not filled the vector

    while (readCount < recordsToReadInThisPass && writeCount < valueVec.getValueCapacity()) {
      // read a page if needed
      if (!pageReader.hasPage()
        || (definitionLevelsRead >= pageReader.currentPageCount)) {
        if (!pageReader.next()) {
          break;
        }
        //New page. Reset the definition level.
        currentDefinitionLevel = -1;
        definitionLevelsRead = 0;
        recordsReadInThisIteration = 0;
        readStartInBytes = 0;
      }

      nullRunLength = 0;
      runLength = 0;

      // If we are reentering this loop, the currentDefinitionLevel has already been read
      if (currentDefinitionLevel < 0) {
        currentDefinitionLevel = pageReader.definitionLevels.readInteger();
      }

      haveMoreData = readCount < recordsToReadInThisPass
        && (writeCount + nullRunLength) < valueVec.getValueCapacity()
        && definitionLevelsRead < pageReader.currentPageCount;

      while (haveMoreData && currentDefinitionLevel < columnDescriptor.getMaxDefinitionLevel()) {
        readCount++;
        nullRunLength++;
        definitionLevelsRead++;
        haveMoreData = readCount < recordsToReadInThisPass
          && (writeCount + nullRunLength) < valueVec.getValueCapacity()
          && definitionLevelsRead < pageReader.currentPageCount;
        if (haveMoreData) {
          currentDefinitionLevel = pageReader.definitionLevels.readInteger();
        }
      }

      //
      // Write the nulls if any
      //
      if (nullRunLength > 0) {
        int writerIndex = valueVec.getDataBuffer().writerIndex();
        valueVec.getDataBuffer().setIndex(0, writerIndex + (int) Math.ceil(nullRunLength * dataTypeLengthInBits / 8.0));
        writeCount += nullRunLength;
        valuesReadInCurrentPass += nullRunLength;
        recordsReadInThisIteration += nullRunLength;
      }

      //
      // Handle the run of non-null values
      //
      haveMoreData = readCount < recordsToReadInThisPass &&
        /* note: writeCount+runLength */(writeCount + runLength) < valueVec.getValueCapacity() &&
        definitionLevelsRead < pageReader.currentPageCount;

      while (haveMoreData && currentDefinitionLevel >= columnDescriptor.getMaxDefinitionLevel()) {
        readCount++;
        runLength++;
        definitionLevelsRead++;
        castedVectorMutator.setIndexDefined(writeCount + runLength - 1); //set the nullable bit to indicate a non-null value
        haveMoreData = readCount < recordsToReadInThisPass
          && (writeCount + runLength) < valueVec.getValueCapacity()
          && definitionLevelsRead < pageReader.currentPageCount;
        if (haveMoreData) {
          currentDefinitionLevel = pageReader.definitionLevels.readInteger();
        }
      }

      //
      // Write the non-null values
      //
      if (runLength > 0) {
        // set up metadata
        // This _must_ be set so that the call to readField works correctly for all datatypes
        this.recordsReadInThisIteration += runLength;

        this.readStartInBytes = pageReader.readPosInBytes;
        this.readLengthInBits = runLength * dictionaryWidthBits;
        // We don't worry about readPosInBytes being set accurate since we never use pageReader to read data
        this.readLength = (int) Math.ceil(readLengthInBits / 8.0);

        readField(runLength);

        writeCount += runLength;
        valuesReadInCurrentPass += runLength;
        pageReader.readPosInBytes = readStartInBytes + readLength;
      }

      pageReader.valuesRead += recordsReadInThisIteration;
      totalValuesRead += runLength + nullRunLength;

      logger.trace("" + "recordsToReadInThisPass: {} \t "
          + "Run Length: {} \t Null Run Length: {} \t readCount: {} \t writeCount: {} \t "
          + "recordsReadInThisIteration: {} \t valuesReadInCurrentPass: {} \t "
          + "totalValuesRead: {} \t readStartInBytes: {} \t readLength: {} \t pageReader.byteLength: {} \t "
          + "definitionLevelsRead: {} \t pageReader.currentPageCount: {}",
        recordsToReadInThisPass, runLength, nullRunLength, readCount,
        writeCount, recordsReadInThisIteration, valuesReadInCurrentPass,
        totalValuesRead, readStartInBytes, readLength, pageReader.byteLength,
        definitionLevelsRead, pageReader.currentPageCount);

    }

    valueVec.getMutator().setValueCount(valuesReadInCurrentPass);
  }

  @Override
  protected void readField(long recordsToReadInThisPass) {
    for (int i = 0; i < recordsToReadInThisPass; i++) {
      final int id = pageReader.dictionaryValueReader.readValueDictionaryId();
      assert id >=0 && id < localIdToGlobalId.length;
      valueVec.getMutator().setSafe(valuesReadInCurrentPass + i, localIdToGlobalId[id]);
    }
  }

  @Override
  public void clear() {
    super.clear();
  }
}
