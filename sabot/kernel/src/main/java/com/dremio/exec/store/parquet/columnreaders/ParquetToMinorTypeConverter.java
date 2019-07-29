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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.PrimitiveType;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.CoreDecimalUtility;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.options.OptionManager;

public class ParquetToMinorTypeConverter {

  private static TypeProtos.MinorType getDecimalType(SchemaElement schemaElement) {
    return MinorType.DECIMAL;
  }

  private static TypeProtos.MinorType getMinorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                                   SchemaElement schemaElement, OptionManager options, Field arrowField,
                                                   final boolean readInt96AsTimeStamp) {

    ConvertedType convertedType = schemaElement.getConverted_type();

    switch (primitiveTypeName) {
      case BINARY:
        if (convertedType == null) {
          return TypeProtos.MinorType.VARBINARY;
        }
        switch (convertedType) {
          case UTF8:
            return TypeProtos.MinorType.VARCHAR;
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return getDecimalType(schemaElement);
          default:
            return TypeProtos.MinorType.VARBINARY;
        }
      case INT64:
        if (convertedType == null) {
          return TypeProtos.MinorType.BIGINT;
        }
        switch(convertedType) {
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return TypeProtos.MinorType.DECIMAL;
          // TODO - add this back if it is decided to be added upstream, was removed form our pull request July 2014
//              case TIME_MICROS:
//                throw new UnsupportedOperationException();
          case TIMESTAMP_MILLIS:
            return TypeProtos.MinorType.TIMESTAMP;
          default:
            throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
        }
      case INT32:
        if (convertedType == null) {
          return TypeProtos.MinorType.INT;
        }
        switch(convertedType) {
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return TypeProtos.MinorType.DECIMAL;
          case DATE:
            return TypeProtos.MinorType.DATE;
          case TIME_MILLIS:
            return TypeProtos.MinorType.TIME;
          default:
            throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
        }
      case BOOLEAN:
        return TypeProtos.MinorType.BIT;
      case FLOAT:
        return TypeProtos.MinorType.FLOAT4;
      case DOUBLE:
        return TypeProtos.MinorType.FLOAT8;
      // TODO - Both of these are not supported by the parquet library yet (7/3/13),
      // but they are declared here for when they are implemented
      case INT96:
        if (readInt96AsTimeStamp) {
          return TypeProtos.MinorType.TIMESTAMP;
        } else {
          return TypeProtos.MinorType.VARBINARY;
        }
      case FIXED_LEN_BYTE_ARRAY:
        if (convertedType == null) {
          checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
          return TypeProtos.MinorType.VARBINARY;
        } else if (convertedType == ConvertedType.DECIMAL) {
          ParquetReaderUtility.checkDecimalTypeEnabled(options);
          return getDecimalType(schemaElement);
        } else if (convertedType == ConvertedType.INTERVAL) {
          if (arrowField != null) {
            if (arrowField.getType().getTypeID() == ArrowTypeID.Interval) {
              switch (((Interval)arrowField.getType()).getUnit()) {
                case DAY_TIME:
                  return TypeProtos.MinorType.INTERVALDAY;
                case YEAR_MONTH:
                  return TypeProtos.MinorType.INTERVALYEAR;
              }
            }
            throw new IllegalArgumentException("incompatible type " + arrowField);
          }
          // TODO: older versions of Drill generated this
          return TypeProtos.MinorType.VARBINARY;
        }
      default:
        throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
    }
  }

  public static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                          TypeProtos.DataMode mode, SchemaElement schemaElement,
                                          OptionManager options, Field arrowField, final boolean readInt96AsTimeStamp) {
    MinorType minorType = getMinorType(primitiveTypeName, length, schemaElement, options, arrowField, readInt96AsTimeStamp);
    TypeProtos.MajorType.Builder typeBuilder = TypeProtos.MajorType.newBuilder().setMinorType(minorType).setMode(mode);

    if (CoreDecimalUtility.isDecimalType(minorType)) {
      typeBuilder.setPrecision(schemaElement.getPrecision()).setScale(schemaElement.getScale());
    }
    return typeBuilder.build();
  }
}
