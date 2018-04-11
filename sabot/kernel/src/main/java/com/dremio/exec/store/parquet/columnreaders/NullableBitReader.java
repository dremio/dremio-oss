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

import org.apache.arrow.vector.NullableBitVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.common.exceptions.ExecutionSetupException;

/**
 * This class is used in conjunction with its superclass to read nullable bit columns in a parquet file.
 * It currently is using an inefficient value-by-value approach.
 * TODO - make this more efficient by copying runs of values like in NullableFixedByteAlignedReader
 * This will also involve incorporating the ideas from the BitReader (the reader for non-nullable nodes)
 * because page/batch boundaries that do not land on byte boundaries require shifting of all of the values
 * in the next batch.
 */
final class NullableBitReader extends ColumnReader<NullableBitVector> {

  NullableBitReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                    boolean fixedLength, NullableBitVector v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  @Override
  public void readField(long recordsToReadInThisPass) {

    recordsReadInThisIteration = Math.min(pageReader.currentPageCount
        - pageReader.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);
    int defLevel;
    for (int i = 0; i < recordsReadInThisIteration; i++){
      defLevel = pageReader.definitionLevels.readInteger();
      // if the value is defined
      if (defLevel == columnDescriptor.getMaxDefinitionLevel()){
        valueVec.setSafe(i + valuesReadInCurrentPass,
            pageReader.valueReader.readBoolean() ? 1 : 0 );
      }
      // otherwise the value is skipped, because the bit vector indicating nullability is zero filled
    }
  }

}
