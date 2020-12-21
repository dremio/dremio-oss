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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.DecimalHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.common.exceptions.ExecutionSetupException;

public class VarLengthColumnReaders {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLengthColumnReaders.class);

  public static class Decimal28Column extends VarLengthValuesColumn<DecimalVector> {

    protected DecimalVector decimalVector;

    Decimal28Column(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, DecimalVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      this.decimalVector = v;
    }

    @Override
    public boolean setSafe(int index, ArrowBuf bytebuf, int start, int length) {
      /* data read from Parquet into the bytebuf is already in BE format, no need
       * swap bytes to construct BigDecimal. only when we write BigDecimal to
       * data buffer of decimal vector, we need to swap bytes which the DecimalUtility
       * function already does.
       */
      BigDecimal intermediate = DecimalHelper.getBigDecimalFromBEArrowBuf(bytebuf, index, schemaElement.getScale());
      if (index >= decimalVector.getValueCapacity()) {
        return false;
      }
      /* this will swap bytes as we are writing to the buffer of DecimalVector */
      DecimalUtility.writeBigDecimalToArrowBuf(intermediate, decimalVector.getDataBuffer(), index, DecimalVector.TYPE_WIDTH);
      decimalVector.setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(decimalVector.getDataBuffer().capacity());
    }
  }

  public static class NullableDecimalColumn extends NullableVarLengthValuesColumn<DecimalVector> {

    protected DecimalVector decimalVector;

    NullableDecimalColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, DecimalVector v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimalVector = v;
    }

    @Override
    public boolean setSafe(int index, ArrowBuf bytebuf, int start, int length) {
      /* data read from Parquet into the bytebuf is already in BE format, no need
       * swap bytes to construct BigDecimal. only when we write BigDecimal to
       * data buffer of decimal vector, we need to swap bytes which the DecimalUtility
       * function already does.
       */
      BigDecimal intermediate = DecimalHelper.getBigDecimalFromBEArrowBuf(bytebuf, index, schemaElement.getScale());
      if (index >= decimalVector.getValueCapacity()) {
        return false;
      }
      /* this will swap bytes as we are writing to the buffer of DecimalVector */
      DecimalUtility.writeBigDecimalToArrowBuf(intermediate, decimalVector.getDataBuffer(), index, DecimalVector.TYPE_WIDTH);
      decimalVector.setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(decimalVector.getDataBuffer().capacity());
    }
  }

  public static class VarCharColumn extends VarLengthValuesColumn<VarCharVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected final VarCharVector varCharVector;

    VarCharColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarCharVector v,
                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varCharVector = v;
    }

    @Override
    public boolean setSafe(int index, ArrowBuf bytebuf, int start, int length) {
      if (index >= varCharVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        varCharVector.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        varCharVector.setSafe(index, 1, start, start + length, bytebuf);
      }
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(varCharVector.getDataBuffer().capacity());
    }
  }

  public static class NullableVarCharColumn extends NullableVarLengthValuesColumn<VarCharVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    private final VarCharVector vector;

    NullableVarCharColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarCharVector v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      vector = v;
    }

    @Override
    public boolean setSafe(int index, ArrowBuf value, int start, int length) {
      if (index >= vector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        vector.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        vector.setSafe(index, 1, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(vector.getDataBuffer().capacity());
    }
  }

  public static class VarBinaryColumn extends VarLengthValuesColumn<VarBinaryVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    private final VarBinaryVector varBinaryVector;

    VarBinaryColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varBinaryVector = v;
    }

    @Override
    public boolean setSafe(int index, ArrowBuf value, int start, int length) {
      if (index >= varBinaryVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        currDictValToWrite = pageReader.dictionaryValueReader.readBytes();
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        varBinaryVector.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        varBinaryVector.setSafe(index, 1, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(varBinaryVector.getDataBuffer().capacity());
    }
  }

  public static class NullableVarBinaryColumn extends NullableVarLengthValuesColumn<VarBinaryVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    private final VarBinaryVector nullableVarBinaryVector;

    NullableVarBinaryColumn(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableVarBinaryVector = v;
    }


    @Override
    public boolean setSafe(int index, ArrowBuf value, int start, int length) {
      if (index >= nullableVarBinaryVector.getValueCapacity()) {
        return false;
      }

      if (usingDictionary) {
        ByteBuffer buf = currDictValToWrite.toByteBuffer();
        nullableVarBinaryVector.setSafe(index, buf, buf.position(), currDictValToWrite.length());
      } else {
        nullableVarBinaryVector.setSafe(index, 1, start, start + length, value);
      }
      return true;
    }

    @Override
    public int capacity() {
      return LargeMemoryUtil.checkedCastToInt(nullableVarBinaryVector.getDataBuffer().capacity());
    }

  }
}
