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
import java.util.List;

public class VarLenBinaryReader {

  DeprecatedParquetVectorizedReader parentReader;
  final List<VarLengthColumn<?>> columns;

  public VarLenBinaryReader(DeprecatedParquetVectorizedReader parentReader, List<VarLengthColumn<?>> columns) {
    this.parentReader = parentReader;
    this.columns = columns;
  }

  public List<VarLengthColumn<?>> getColumns() {
    return columns;
  }

  /**
   * Reads as many variable length values as possible.
   *
   * @param recordsToReadInThisPass - the number of records recommended for reading form the reader
   * @param firstColumnStatus - a reference to the first column status in the parquet file to grab metatdata from
   * @return - the number of fixed length fields that will fit in the batch
   * @throws IOException
   */
  public long readFields(long recordsToReadInThisPass, ColumnReader<?> firstColumnStatus) throws IOException {

    long recordsReadInCurrentPass = 0;
    long totalVariableLengthData = 0;
    boolean exitLengthDeterminingLoop = false;
    // write the first 0 offset
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.reset();
    }

    do {
      for (VarLengthColumn<?> columnReader : columns) {
        if ( !exitLengthDeterminingLoop ) {
          exitLengthDeterminingLoop = columnReader.determineSize(recordsReadInCurrentPass, 0 /* TODO remove, unused */);
        } else {
          break;
        }
      }
      if (exitLengthDeterminingLoop) {
        break;
      }
      for (VarLengthColumn<?> columnReader : columns ) {
        columnReader.updateReadyToReadPosition();
        columnReader.currDefLevel = -1;
      }
      recordsReadInCurrentPass++;
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.readRecords(columnReader.pageReader.valuesReadyToRead);
    }
    for (VarLengthColumn<?> columnReader : columns) {
      columnReader.valueVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }

}
