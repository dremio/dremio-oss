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
package com.dremio.exec.store.hive;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class HiveSchemaConverter {

  public static Field getArrowFieldFromHivePrimitiveType(String name, TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      PrimitiveTypeInfo pTypeInfo = (PrimitiveTypeInfo) typeInfo;
      switch (pTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:

        return new Field(name, true, new Bool(), null);
      case BYTE:
        return new Field(name, true, new Int(32, true), null);
      case SHORT:
        return new Field(name, true, new Int(32, true), null);

      case INT:
        return new Field(name, true, new Int(32, true), null);

      case LONG:
        return new Field(name, true, new Int(64, true), null);

      case FLOAT:
        return new Field(name, true, new FloatingPoint(FloatingPointPrecision.SINGLE), null);

      case DOUBLE:
        return new Field(name, true, new FloatingPoint(FloatingPointPrecision.DOUBLE), null);

      case DATE:
        return new Field(name, true, new Date(DateUnit.MILLISECOND), null);

      case TIMESTAMP:
        return new Field(name, true, new Timestamp(TimeUnit.MILLISECOND, null), null);

      case BINARY:
        return new Field(name, true, new Binary(), null);
      case DECIMAL: {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) pTypeInfo;
        return new Field(name, true, new Decimal(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale()), null);
      }

      case STRING:
      case VARCHAR:
      case CHAR: {
        return new Field(name, true, new Utf8(), null);
      }
      case UNKNOWN:
      case VOID:
      default:
        // fall through.
      }
    default:
    }

    return null;
  }
}
