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
package com.dremio.dac.explore;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.dac.proto.model.dataset.DataType;

/**
 * Tools to transform from execution types to UI types.
 */
public final class DataTypeUtil {

  private DataTypeUtil(){}

  public static DataType getDataType(MajorType type) {
    return getDataType(type.getMinorType());
  }

  public static DataType getDataType(org.apache.arrow.vector.types.Types.MinorType minorType) {
    return getDataType(getMinorTypeFromArrowMinorType(minorType));
  }

  public static DataType getDataType(MinorType minorType) {
    switch (minorType) {
    case FIXED16CHAR:
    case FIXEDCHAR:
    case VAR16CHAR:
    case VARCHAR:
      return DataType.TEXT;
    case FIXEDSIZEBINARY:
    case VARBINARY:
      return DataType.BINARY;
    case BIT:
      return DataType.BOOLEAN;
    case FLOAT4:
    case FLOAT8:
      return DataType.FLOAT;
    case BIGINT:
    case INT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case SMALLINT:
      return DataType.INTEGER;
    case DECIMAL:
      return DataType.DECIMAL;
    case UNION:
      return DataType.MIXED;
    case DATE:
      return DataType.DATE;
    case TIME:
    case TIMETZ:
      return DataType.TIME;
    case TIMESTAMP:
    case TIMESTAMPTZ:
      return DataType.DATETIME;
    case LIST:
      return DataType.LIST;
    case STRUCT:
    case GENERIC_OBJECT:
      return DataType.MAP;
//    case ???:
//      return DataType.GEO;
    case MONEY:
    case NULL:
    case INTERVAL:
    case INTERVALDAY:
    case INTERVALYEAR:
    case LATE:
      return DataType.OTHER;
    default:
      throw new UnsupportedOperationException(minorType.name());
    }
  }

  public static DataType getDataType(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return DataType.INTEGER;
      case DECIMAL:
        return DataType.DECIMAL;
      case REAL:
      case FLOAT:
      case DOUBLE:
        return DataType.FLOAT;
      case DATE:
        return DataType.DATE;
      case TIME:
        return DataType.TIME;
      case TIMESTAMP:
        return DataType.DATETIME;
      case CHAR:
      case VARCHAR:
        return DataType.TEXT;
      case BINARY:
      case VARBINARY:
        return DataType.BINARY;
      case ARRAY:
        return DataType.LIST;
      case MAP:
        return DataType.MAP;
      case DISTINCT:
      case MULTISET:
      case STRUCTURED:
      case ROW:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case ANY:
      case SYMBOL:
      case OTHER:
      case NULL:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return DataType.OTHER;

      default:
        return DataType.OTHER;
    }
  }

  public static String getStringValueForDateType(DataType type) {
    switch (type) {
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case DATETIME:
        return "TIMESTAMP";
      default:
        throw new UnsupportedOperationException("Unknown type " + type);
    }
  }

}
