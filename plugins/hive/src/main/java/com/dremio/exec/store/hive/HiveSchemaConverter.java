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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.mapred.InputFormat;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class HiveSchemaConverter {

  public static boolean areComplexTypesSupported(InputFormat<?, ?> format) {
    return (format instanceof OrcInputFormat);
  }

  public static Field getArrowFieldFromHiveType(String name, TypeInfo typeInfo, InputFormat<?, ?> format) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return HiveSchemaConverter.getArrowFieldFromHivePrimitiveType(name, typeInfo);
      case LIST: {
        if (!HiveSchemaConverter.areComplexTypesSupported(format)) {
          return null;
        }
        ListTypeInfo lti = (ListTypeInfo) typeInfo;
        TypeInfo elementTypeInfo = lti.getListElementTypeInfo();
        Field inner = HiveSchemaConverter.getArrowFieldFromHiveType("$data$", elementTypeInfo, format);
        if (inner == null) {
          return null;
        }
        return new Field(name, true, Types.MinorType.LIST.getType(),
          Collections.singletonList(inner));
      }
      case STRUCT: {
        if (!HiveSchemaConverter.areComplexTypesSupported(format)) {
          return null;
        }
        StructTypeInfo sti = (StructTypeInfo) typeInfo;
        ArrayList<String> fieldNames = sti.getAllStructFieldNames();
        ArrayList<Field> structFields = new ArrayList<Field>();
        for (String fieldName : fieldNames) {
          TypeInfo fieldTypeInfo = sti.getStructFieldTypeInfo(fieldName);
          Field f = HiveSchemaConverter.getArrowFieldFromHiveType(fieldName,
            fieldTypeInfo, format);
          if (f == null) {
            return null;
          }
          structFields.add(f);
        }
        return new Field(name, true, Types.MinorType.STRUCT.getType(),
          structFields);
      }
      case MAP: {
        if (!HiveSchemaConverter.areComplexTypesSupported(format)) {
          return null;
        }
        // Treating hive map datatype as a "list of structs" datatype in arrow.
        MapTypeInfo mti = (MapTypeInfo) typeInfo;

        ArrayList<Field> mapFields = new ArrayList<Field>();

        TypeInfo keyTypeInfo = mti.getMapKeyTypeInfo();
        Field keyField = HiveSchemaConverter.getArrowFieldFromHiveType(HiveUtilities.MAP_KEY_FIELD_NAME,
          keyTypeInfo, format);
        if (keyField == null) {
          return null;
        }

        TypeInfo valTypeInfo = mti.getMapValueTypeInfo();
        Field valField = HiveSchemaConverter.getArrowFieldFromHiveType(HiveUtilities.MAP_VALUE_FIELD_NAME,
          valTypeInfo, format);
        if (valField == null) {
          return null;
        }
        mapFields.add(keyField);
        mapFields.add(valField);
        Field inner = new Field("$data$", true, Types.MinorType.STRUCT.getType(),
          mapFields);
        return new Field(name, true, Types.MinorType.LIST.getType(),
          Collections.singletonList(inner));
      }
      case UNION: {
        if (!HiveSchemaConverter.areComplexTypesSupported(format)) {
          return null;
        }
        UnionTypeInfo uti = (UnionTypeInfo) typeInfo;
        ArrayList<Field> unionFields = new ArrayList<Field>();
        final List<TypeInfo> objectTypeInfos = uti.getAllUnionObjectTypeInfos();
        int[] typeIds = new int[objectTypeInfos.size()];
        for (int idx = 0; idx < objectTypeInfos.size(); ++idx) {
          TypeInfo fieldTypeInfo = objectTypeInfos.get(idx);
          Field fieldToGetArrowType = HiveSchemaConverter.getArrowFieldFromHiveType("", fieldTypeInfo, format);
          if (fieldToGetArrowType == null) {
            return null;
          }
          ArrowType arrowType = fieldToGetArrowType.getType();
          if (arrowType == null) {
            return null;
          }
          // In a union, Arrow expects the field name for each member to be the same as the "minor type" name.
          Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
          Field f = HiveSchemaConverter.getArrowFieldFromHiveType(minorType.name().toLowerCase(), fieldTypeInfo, format);
          if (f == null) {
            return null;
          }
          typeIds[idx] = Types.getMinorTypeForArrowType(f.getType()).ordinal();
          unionFields.add(f);
        }
        return new Field(name, FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, typeIds)), unionFields);
      }
      default:
        return null;
    }
  }

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
