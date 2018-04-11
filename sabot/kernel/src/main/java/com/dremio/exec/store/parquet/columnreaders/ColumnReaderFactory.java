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

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateMilliVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTimeMilliVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.parquet.columnreaders.VarLengthColumnReaders.NullableDecimalColumn;

public class ColumnReaderFactory {

  /**
   * @param fixedLength
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize - the size of the vector to create
   * @return
   * @throws SchemaChangeException
   */
  static ColumnReader<?> createFixedColumnReader(DeprecatedParquetVectorizedReader recordReader, boolean fixedLength, ColumnDescriptor descriptor,
                                                 ColumnChunkMetaData columnChunkMetaData, int allocateSize, ValueVector v,
                                                 SchemaElement schemaElement, CompleteType type)
      throws Exception {

    ConvertedType convertedType = schemaElement.getConverted_type();
    // if the column is required, or repeated (in which case we just want to use this to generate our appropriate
    // ColumnReader for actually transferring data into the data vector inside of our repeated vector
    if (descriptor.getMaxDefinitionLevel() == 0 || descriptor.getMaxRepetitionLevel() > 0){
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new BitReader(recordReader, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, (NullableBitVector) v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ||
          columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT96) {
        if (convertedType == ConvertedType.DECIMAL){
          int length = schemaElement.type_length;
          if (length <= 12) {
            return new FixedByteAlignedReader.DecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          } else if (length <= 16) {
            return new FixedByteAlignedReader.DecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          }
        } else if (convertedType == ConvertedType.INTERVAL) {
          throw new UnsupportedOperationException("unsupported type " + type);
        }
        else {
          return new FixedByteAlignedReader.FixedBinaryReader(recordReader, allocateSize, descriptor, columnChunkMetaData, (VariableWidthVector) v, schemaElement);
        }
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        switch(recordReader.getDateCorruptionStatus()) {
          case META_SHOWS_CORRUPTION:
            return new FixedByteAlignedReader.CorruptDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector) v, schemaElement);
          case META_SHOWS_NO_CORRUPTION:
            return new FixedByteAlignedReader.DateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector) v, schemaElement);
          case META_UNCLEAR_TEST_VALUES:
            return new FixedByteAlignedReader.CorruptionDetectingDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector) v, schemaElement);
          default:
            // See DRILL-4203
            throw new ExecutionSetupException(
                String.format("Issue setting up parquet reader for date type, " +
                    "unrecognized date corruption status %s.",
                    recordReader.getDateCorruptionStatus()));
        }
      } else{
        if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
          switch (columnChunkMetaData.getType()) {
            case INT32:
              if (convertedType == null) {
                return new ParquetFixedWidthDictionaryReaders.DictionaryIntReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableIntVector) v, schemaElement);
              }
              switch (convertedType) {
                case DECIMAL:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryIntDecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
                case TIME_MILLIS:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryTimeReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableTimeMilliVector) v, schemaElement);
                default:
                  throw new ExecutionSetupException("Unsupported dictionary converted type " + convertedType + " for primitive type INT32");
              }
            case INT64:
              if (convertedType == null) {
                return new ParquetFixedWidthDictionaryReaders.DictionaryBigIntReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableBigIntVector) v, schemaElement);
              }
              switch (convertedType) {
                case DECIMAL:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryLongDecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
                case TIMESTAMP_MILLIS:
                  return new ParquetFixedWidthDictionaryReaders.DictionaryTimeStampReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableTimeStampMilliVector) v, schemaElement);
                default:
                  throw new ExecutionSetupException("Unsupported dictionary converted type " + convertedType + " for primitive type INT64");
              }
            case FLOAT:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat4Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableFloat4Vector) v, schemaElement);
            case DOUBLE:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFloat8Reader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableFloat8Vector) v, schemaElement);
            case FIXED_LEN_BYTE_ARRAY:
              return new ParquetFixedWidthDictionaryReaders.DictionaryFixedBinaryReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported dictionary column type " + descriptor.getType().name() );
          }

        } else {
          return new FixedByteAlignedReader<>(recordReader, allocateSize, descriptor, columnChunkMetaData,
              fixedLength, v, schemaElement);
        }
      }
    }
    else { // if the column is nullable
      if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.BOOLEAN){
        return new NullableBitReader(recordReader, allocateSize, descriptor, columnChunkMetaData,
            fixedLength, (NullableBitVector) v, schemaElement);
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.INT32 && convertedType == ConvertedType.DATE){
        switch(recordReader.getDateCorruptionStatus()) {
          case META_SHOWS_CORRUPTION:
            return new NullableFixedByteAlignedReaders.NullableCorruptDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector)v, schemaElement);
          case META_SHOWS_NO_CORRUPTION:
            return new NullableFixedByteAlignedReaders.NullableDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector) v, schemaElement);
          case META_UNCLEAR_TEST_VALUES:
            return new NullableFixedByteAlignedReaders.CorruptionDetectingNullableDateReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDateMilliVector) v, schemaElement);
          default:
            // See DRILL-4203
            throw new ExecutionSetupException(
                String.format("Issue setting up parquet reader for date type, " +
                        "unrecognized date corruption status %s.",
                    recordReader.getDateCorruptionStatus()));
        }
      } else if (columnChunkMetaData.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        if (convertedType == ConvertedType.DECIMAL) {
          int length = schemaElement.type_length;
          if (length <= 12) {
            return new NullableFixedByteAlignedReaders.NullableDecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          } else if (length <= 16) {
            return new NullableFixedByteAlignedReaders.NullableDecimalReader(recordReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          }
        } else if (convertedType == ConvertedType.INTERVAL) {
          throw new UnsupportedOperationException("interval");
        }
      } else {
        return getNullableColumnReader(recordReader, allocateSize, descriptor,
            columnChunkMetaData, fixedLength, v, schemaElement);
      }
    }
    throw new Exception("Unexpected parquet metadata configuration.");
  }

  static VarLengthValuesColumn<?> getReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v,
                                            SchemaElement schemaElement
  ) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (descriptor.getMaxDefinitionLevel()) {
      case 0:
        if (convertedType == null) {
          return new VarLengthColumnReaders.VarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }
        switch (convertedType) {
          case UTF8:
            return new VarLengthColumnReaders.VarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarCharVector) v, schemaElement);
          case DECIMAL:
            return new VarLengthColumnReaders.Decimal28Column(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          default:
            return new VarLengthColumnReaders.VarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }
      default:
        if (convertedType == null) {
          return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }

        switch (convertedType) {
          case UTF8:
            return new VarLengthColumnReaders.NullableVarCharColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarCharVector) v, schemaElement);
          case DECIMAL:
            return new NullableDecimalColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) v, schemaElement);
          default:
            return new VarLengthColumnReaders.NullableVarBinaryColumn(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, (NullableVarBinaryVector) v, schemaElement);
        }
    }
  }

  public static NullableColumnReader<?> getNullableColumnReader(DeprecatedParquetVectorizedReader parentReader, int allocateSize,
                                                                ColumnDescriptor columnDescriptor,
                                                                ColumnChunkMetaData columnChunkMetaData,
                                                                boolean fixedLength,
                                                                ValueVector valueVec,
                                                                SchemaElement schemaElement) throws ExecutionSetupException {
    ConvertedType convertedType = schemaElement.getConverted_type();

    if (! columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT96) {
         // TODO: check convertedType once parquet support TIMESTAMP_NANOS type annotation.
        if (parentReader.readInt96AsTimeStamp()) {
          return new NullableFixedByteAlignedReaders.NullableFixedBinaryAsTimeStampReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, true, (NullableTimeStampMilliVector) valueVec, schemaElement);
        } else {
          return new NullableFixedByteAlignedReaders.NullableFixedBinaryReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, true, (NullableVarBinaryVector) valueVec, schemaElement);
        }
      }else{
        return new NullableFixedByteAlignedReaders.NullableFixedByteAlignedReader<>(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, valueVec, schemaElement);
      }
    } else {
      switch (columnDescriptor.getType()) {
        case INT32:
          if (convertedType == null) {
            return new NullableFixedByteAlignedReaders.NullableDictionaryIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableIntVector) valueVec, schemaElement);
          }
          switch (convertedType) {
            case DECIMAL:
              return new NullableFixedByteAlignedReaders.NullableDictionaryDecimal9Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector) valueVec, schemaElement);
            case TIME_MILLIS:
              return new NullableFixedByteAlignedReaders.NullableDictionaryTimeReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableTimeMilliVector)valueVec, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported nullable converted type " + convertedType + " for primitive type INT32");
          }
        case INT64:
          if (convertedType == null) {
            return new NullableFixedByteAlignedReaders.NullableDictionaryBigIntReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableBigIntVector)valueVec, schemaElement);
          }
          switch (convertedType) {
            case DECIMAL:
              return new NullableFixedByteAlignedReaders.NullableDictionaryDecimal18Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableDecimalVector)valueVec, schemaElement);
            case TIMESTAMP_MILLIS:
              return new NullableFixedByteAlignedReaders.NullableDictionaryTimeStampReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableTimeStampMilliVector)valueVec, schemaElement);
            default:
              throw new ExecutionSetupException("Unsupported nullable converted type " + convertedType + " for primitive type INT64");
          }
        case INT96:
          // TODO: check convertedType once parquet support TIMESTAMP_NANOS type annotation.
          if (parentReader.readInt96AsTimeStamp()) {
            return new NullableFixedByteAlignedReaders.NullableFixedBinaryAsTimeStampReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, true, (NullableTimeStampMilliVector) valueVec, schemaElement);
          } else {
            return new NullableFixedByteAlignedReaders.NullableFixedBinaryReader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, true, (NullableVarBinaryVector) valueVec, schemaElement);
          }
        case FLOAT:
          return new NullableFixedByteAlignedReaders.NullableDictionaryFloat4Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableFloat4Vector)valueVec, schemaElement);
        case DOUBLE:
          return new NullableFixedByteAlignedReaders.NullableDictionaryFloat8Reader(parentReader, allocateSize, columnDescriptor, columnChunkMetaData, fixedLength, (NullableFloat8Vector)valueVec, schemaElement);
        default:
          throw new ExecutionSetupException("Unsupported nullable column type " + columnDescriptor.getType().name() );
      }
    }
  }
}
