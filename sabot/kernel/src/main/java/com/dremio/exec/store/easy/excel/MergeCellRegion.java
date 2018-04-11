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
package com.dremio.exec.store.easy.excel;

import static org.apache.poi.ss.util.CellReference.convertColStringToIndex;

import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;

import com.dremio.common.types.TypeProtos.MinorType;

import io.netty.buffer.ArrowBuf;

/**
 * Holder class to represent the merged cell boundaries and provide helpful methods to match.
 */
class MergeCellRegion {
  final int colStart;
  final int colEnd; // inclusive
  final int rowStart;
  final int rowEnd; // inclusive

  /**
   * Value writer and value for the merged region. These are set when the cell in top-left
   * corner of the merged region is read from sheet data.
   */
  ValueWriter writer;
  Object value;

  private MergeCellRegion(int colStart, int colEnd, int rowStart, int rowEnd) {
    this.colStart = colStart;
    this.colEnd = colEnd;
    this.rowStart = rowStart;
    this.rowEnd = rowEnd;
  }

  boolean containsRow(long rowNum) {
    return rowStart <= rowNum && rowNum <= rowEnd;
  }

  boolean hasValue() {
    return writer != null && value != null;
  }

  void write(ArrowBuf arrowBuf, MapWriter mapWriter, String colName) {
    writer.write(this, arrowBuf, mapWriter, colName);
  }

  void setValue(MinorType type, Object value) {
    switch (type) {
      case BIT:
        writer = new BitWriter();
        break;
      case FLOAT8:
        writer = new Float8Writer();
        break;
      case TIMESTAMP:
        writer = new TimeStampMilliWriter();
        break;
      case VARCHAR:
        writer = new VarCharWriter();
        break;
    }
    this.value = value;
  }

  public static MergeCellRegion create(String startCell, String endCell) {

    int startCellIndex = ExcelUtil.getRowNumberStartIndex(startCell);
    int endCellIndex = ExcelUtil.getRowNumberStartIndex(endCell);

    return new MergeCellRegion(
        convertColStringToIndex(startCell.substring(0, startCellIndex)),
        convertColStringToIndex(endCell.substring(0, endCellIndex)),
        Integer.parseInt(startCell.substring(startCellIndex)),
        Integer.parseInt(endCell.substring(endCellIndex))
    );
  }

  interface ValueWriter {
    void write(MergeCellRegion mcr, ArrowBuf arrowBuf, MapWriter writer, String colName);
  }

  class BitWriter implements ValueWriter {
    @Override
    public void write(MergeCellRegion mcr, ArrowBuf arrowBuf, MapWriter writer, String colName) {
      writer.bit(colName).writeBit((Integer) mcr.value);
    }
  }

  class Float8Writer implements ValueWriter {
    @Override
    public void write(MergeCellRegion mcr, ArrowBuf arrowBuf, MapWriter writer, String colName) {
      writer.float8(colName).writeFloat8((Double) mcr.value);
    }
  }

  class TimeStampMilliWriter implements ValueWriter {
    @Override
    public void write(MergeCellRegion mcr, ArrowBuf arrowBuf, MapWriter writer, String colName) {
      writer.timeStampMilli(colName).writeTimeStampMilli((Long) mcr.value);
    }
  }

  class VarCharWriter implements ValueWriter {
    @Override
    public void write(MergeCellRegion mcr, ArrowBuf arrowBuf, MapWriter writer, String colName) {
      byte[] b = (byte[]) mcr.value;
      arrowBuf = arrowBuf.reallocIfNeeded(b.length);
      arrowBuf.setBytes(0, b);
      writer.varChar(colName).writeVarChar(0, b.length, arrowBuf);
    }
  }
}
