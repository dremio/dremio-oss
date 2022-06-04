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
package com.dremio.exec.store.hive.iceberg;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * This class is a duplicate of HiveSchemaUtil in iceberg-hive-metastore, copied here to avoid
 * adding dependency on the iceberg jar.
 */
public class HiveSchemaUtil {

  private HiveSchemaUtil() {
  }

  public static List<FieldSchema> convert(Schema schema) {
    return schema.columns().stream()
      .map(col -> new FieldSchema(col.name(), convertToTypeString(col.type()), col.doc()))
      .collect(Collectors.toList());
  }

  /**
   * Converts an Iceberg type to a Hive TypeInfo object.
   * @param type The Iceberg type
   * @return The Hive type
   */
  public static TypeInfo convert(Type type) {
    return TypeInfoUtils.getTypeInfoFromTypeString(convertToTypeString(type));
  }

  private static String convertToTypeString(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
      case STRING:
      case UUID:
        return "string";
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        // TODO : Handle this case
        // if (MetastoreUtil.hive3PresentOnClasspath() && timestampType.shouldAdjustToUTC()) {
          //return "timestamp with local time zone";
        // }
        return "timestamp";
      case FIXED:
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
          .map(f -> String.format("%s:%s", f.name(), convert(f.type())))
          .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", convert(listType.elementType()));
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }
}
