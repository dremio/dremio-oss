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
package com.dremio.exec.store.parquet;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.store.parquet2.LogicalListL1Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ParquetTypeHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetTypeHelper.class);
  private static Map<MinorType, PrimitiveTypeName> typeMap;
  private static Map<DataMode, Repetition> modeMap;
  private static Map<MinorType, OriginalType> originalTypeMap;

  static {
    typeMap = new HashMap<>();
    typeMap.put(MinorType.TINYINT, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.UINT1, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.UINT2, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.SMALLINT, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.INT, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.UINT4, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.FLOAT4, PrimitiveTypeName.FLOAT);
    typeMap.put(MinorType.INTERVALYEAR, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(MinorType.TIME, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.BIGINT, PrimitiveTypeName.INT64);
    typeMap.put(MinorType.UINT8, PrimitiveTypeName.INT64);
    typeMap.put(MinorType.FLOAT8, PrimitiveTypeName.DOUBLE);
    typeMap.put(MinorType.DATE, PrimitiveTypeName.INT32);
    typeMap.put(MinorType.TIMESTAMP, PrimitiveTypeName.INT64);
    typeMap.put(MinorType.INTERVALDAY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(MinorType.DECIMAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(MinorType.VARBINARY, PrimitiveTypeName.BINARY);
    typeMap.put(MinorType.VARCHAR, PrimitiveTypeName.BINARY);
    typeMap.put(MinorType.BIT, PrimitiveTypeName.BOOLEAN);

    originalTypeMap = new HashMap<>();
    originalTypeMap.put(MinorType.DECIMAL, OriginalType.DECIMAL);
    originalTypeMap.put(MinorType.VARCHAR, OriginalType.UTF8);
    originalTypeMap.put(MinorType.DATE, OriginalType.DATE);
    originalTypeMap.put(MinorType.TIME, OriginalType.TIME_MILLIS);
    originalTypeMap.put(MinorType.TIMESTAMP, OriginalType.TIMESTAMP_MILLIS);
    originalTypeMap.put(MinorType.INTERVALDAY, OriginalType.INTERVAL);
    originalTypeMap.put(MinorType.INTERVALYEAR, OriginalType.INTERVAL);
    originalTypeMap.put(MinorType.INTERVAL, OriginalType.INTERVAL);
  }

  public static PrimitiveTypeName getPrimitiveTypeNameForMinorType(MinorType minorType) {
    PrimitiveTypeName primitiveTypeName = typeMap.get(minorType);
    return primitiveTypeName;
  }

  public static OriginalType getOriginalTypeForMinorType(MinorType minorType) {
    return originalTypeMap.get(minorType);
  }

  public static DecimalMetadata getDecimalMetadataForField(MajorType type) {
    switch (type.getMinorType()) {
      case DECIMAL:
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL28DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL38DENSE:
        return new DecimalMetadata(type.getPrecision(), type.getScale());
      default:
        return null;
    }
  }

  public static int getLengthForMinorType(MinorType minorType) {
    switch (minorType) {
      case INTERVALDAY:
      case INTERVALYEAR:
      case INTERVAL:
        return 12;
      case DECIMAL28SPARSE:
        return 12;
      case DECIMAL:
      case DECIMAL38SPARSE:
        return 16;
      default:
        return 0;
    }
  }

  /**
   * Returns an arrow vector field for a parquet primitive field
   *
   * @param colPath       schema path of the column
   * @param primitiveType parquet primitive type
   * @param originalType  parquet original type
   * @param schemaHelper  schema helper used for type conversions
   * @return arrow vector field
   */
  public static Field createField(SchemaPath colPath,
                                  PrimitiveType primitiveType,
                                  OriginalType originalType,
                                  SchemaDerivationHelper schemaHelper) {
    return createField(colPath, primitiveType, originalType, schemaHelper, true);
  }

  private static Field createField(SchemaPath colPath,
                            PrimitiveType primitiveType,
                            OriginalType originalType,
                            SchemaDerivationHelper schemaHelper,
                            boolean isNullable) {
    final String colName = colPath.getAsNamePart().getName();
    switch (primitiveType.getPrimitiveTypeName()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        if (originalType == OriginalType.UTF8) {
          return CompleteType.VARCHAR.toField(colName, isNullable);
        }
        if (originalType == OriginalType.DECIMAL) {

          return CompleteType.fromDecimalPrecisionScale(primitiveType.getDecimalMetadata()
            .getPrecision(), primitiveType.getDecimalMetadata().getScale()).toField(colName, isNullable);
        }
        if (schemaHelper.isVarChar(colPath)) {
          return CompleteType.VARCHAR.toField(colName, isNullable);
        }
        return CompleteType.VARBINARY.toField(colName, isNullable);
      case BOOLEAN:
        return CompleteType.BIT.toField(colName, isNullable);
      case DOUBLE:
        return CompleteType.DOUBLE.toField(colName, isNullable);
      case FLOAT:
        return CompleteType.FLOAT.toField(colName, isNullable);
      case INT32:
        if (originalType == OriginalType.DATE) {
          return CompleteType.DATE.toField(colName, isNullable);
        } else if (originalType == OriginalType.TIME_MILLIS) {
          return CompleteType.TIME.toField(colName, isNullable);
        } else if (originalType == OriginalType.DECIMAL) {
          return CompleteType.fromDecimalPrecisionScale(primitiveType.getDecimalMetadata()
            .getPrecision(), primitiveType.getDecimalMetadata().getScale()).toField(colName, isNullable);
        }
        return CompleteType.INT.toField(colName, isNullable);
      case INT64:
        if (originalType == OriginalType.TIMESTAMP_MILLIS || originalType == OriginalType.TIMESTAMP_MICROS) {
          return CompleteType.TIMESTAMP.toField(colName, isNullable);
        } else if (originalType == OriginalType.TIME_MICROS) {
          return CompleteType.TIME.toField(colName, isNullable);
        } else if (originalType == OriginalType.DECIMAL) {
          return CompleteType.fromDecimalPrecisionScale(primitiveType.getDecimalMetadata()
            .getPrecision(), primitiveType.getDecimalMetadata().getScale()).toField(colName, isNullable);
        }
        return CompleteType.BIGINT.toField(colName, isNullable);
      case INT96:
        if (schemaHelper.readInt96AsTimeStamp()) {
          return CompleteType.TIMESTAMP.toField(colName, isNullable);
        }
        return CompleteType.VARBINARY.toField(colName, isNullable);
      default:
        throw UserException.unsupportedError()
          .message("Parquet Primitive Type '%s', Original Type '%s' combination not supported. Column '%s'",
            primitiveType.toString(), originalType != null ? originalType : "Not Available", colName)
          .build();
    }
  }

  public static Optional<Field> toField(final Type parquetField, final SchemaDerivationHelper schemaHelper) {
    return toField(parquetField, schemaHelper, false, true);
  }

  private static Optional<Field> toField(final Type parquetField, final SchemaDerivationHelper schemaHelper, boolean convertToStruct, boolean isNullable) {
    if (parquetField.isPrimitive()) {
      SchemaPath columnSchemaPath = SchemaPath.getCompoundPath(parquetField.getName());
      Field field = createField(columnSchemaPath, parquetField.asPrimitiveType(), parquetField.getOriginalType(), schemaHelper, isNullable);
      if (parquetField.isRepetition(REPEATED)) {
        Field listChild = new Field("$data$", new FieldType(isNullable, field.getType(), field.getDictionary()), field.getChildren());
        return Optional.of(new Field(field.getName(), new FieldType(isNullable, new ArrowType.List(), field.getDictionary()), Arrays.asList(listChild)));
      }
      return Optional.of(field);
    }

    // Handle non-primitive cases
    final GroupType complexField = (GroupType) parquetField;
    if (OriginalType.LIST == complexField.getOriginalType() && LogicalListL1Converter.isSupportedSchema(complexField)) {
      GroupType repeatedField = (GroupType) complexField.getFields().get(0);
      Optional<Field> subField = toField(repeatedField.getFields().get(0), schemaHelper);
      subField = subField.map(sf -> new Field("$data$", new FieldType(isNullable, sf.getType(), sf.getDictionary()), sf.getChildren()));
      return subField.map(sf -> new Field(complexField.getName(), new FieldType(isNullable, new ArrowType.List(), null), Arrays.asList(new Field[] {sf})));
    } else if (OriginalType.LIST == complexField.getOriginalType()) {
      if (complexField.getFieldCount() == 1) {
        Type type = complexField.getType(0);
        Optional<Field> subField = toField(type, schemaHelper);
        if (complexField.isRepetition(REPEATED)) {
          subField = subField.map(sf -> new Field("$data$", new FieldType(isNullable, new ArrowType.Struct(), null), Arrays.asList(new Field[] {sf})));
          return subField.map(sf -> new Field(complexField.getName(), new FieldType(isNullable, new ArrowType.List(), null), Arrays.asList(new Field[] {sf})));
        } else {
          return subField.map(sf -> new Field(complexField.getName(), new FieldType(isNullable, new ArrowType.Struct(), null), Arrays.asList(new Field[] {sf})));
        }
      }
      throw UserException.unsupportedError()
              .message("Parquet List Type is expected to contain only one sub type. Column '%s' contains %d", parquetField.getName(), complexField.getFieldCount())
              .build();
    }

    if (OriginalType.MAP == complexField.getOriginalType() && !convertToStruct) {
      GroupType repeatedField = (GroupType) complexField.getFields().get(0);

      // should have only one child field type
      if (repeatedField.isPrimitive() || !repeatedField.isRepetition(REPEATED) || repeatedField.asGroupType().getFields().size() != 2) {
        throw UserException.unsupportedError()
          .message("Parquet Map Type is expected to contain key and value fields. Column '%s' contains %d", parquetField.getName(), repeatedField.getFieldCount())
          .build();
      }
      if (isEligibleForMap(schemaHelper, repeatedField)) {
        List<Field> keyValueFields = new ArrayList<>();
        keyValueFields.add(toField(repeatedField.getType(0), schemaHelper, false, false).get());
        keyValueFields.add(toField(repeatedField.getType(1), schemaHelper, false, true).get());
        Field anonymousStruct = CompleteType.struct(keyValueFields).toField(MapVector.DATA_VECTOR_NAME, false);
        return Optional.of(new Field(complexField.getName(), new FieldType(true, CompleteType.MAP.getType(), null), Arrays.asList(new Field[]{anonymousStruct})));
      } else {
        Optional<Field> subField = toField(repeatedField, schemaHelper,true, isNullable);
        if (complexField.isRepetition(REPEATED)) {
          subField = subField.map(sf -> new Field("$data$", new FieldType(true, sf.getType(), sf.getDictionary()), sf.getChildren()));
          subField = subField.map(sf -> new Field(repeatedField.getName(), new FieldType(true, new ArrowType.List(), null), Arrays.asList(new Field[] {sf})));
          subField = subField.map(sf -> new Field("$data$", new FieldType(true, new ArrowType.Struct(), null), Arrays.asList(new Field[] {sf})));
          return subField.map(sf -> new Field(complexField.getName(), new FieldType(true, new ArrowType.List(), null), Arrays.asList(new Field[] {sf})));
        } else {
          subField = subField.map(sf -> new Field("$data$", new FieldType(true, sf.getType(), sf.getDictionary()), sf.getChildren()));
          subField = subField.map(sf -> new Field(repeatedField.getName(), new FieldType(true, new ArrowType.List(), null), Arrays.asList(new Field[] {sf})));
          return subField.map(sf -> new Field(complexField.getName(), new FieldType(true, new ArrowType.Struct(), null), Arrays.asList(new Field[] {sf})));
        }
      }
    }

    final boolean isStructType = complexField.getOriginalType() == null || convertToStruct;
    if (isStructType) { // it is struct
      return toComplexField(complexField, new ArrowType.Struct(), schemaHelper, convertToStruct);
    }

    // Unsupported complex type
    return Optional.empty();
  }

  private static boolean isEligibleForMap(SchemaDerivationHelper schemaHelper, GroupType repeatedField) {
    if (schemaHelper.isMapDataTypeEnabled()) {
      boolean isEligible = true;
      if (repeatedField.getType(0).getOriginalType() != OriginalType.UTF8) {
        isEligible = false;
        logger.debug(String.format(" Key of map Field %s is not of VarChar (String) type or is a Complex Type", repeatedField.getName()));
      }
      if (!repeatedField.getType(1).isPrimitive()) {
        isEligible = false;
        logger.debug(String.format(" Value of map Field %s is a Complex Type",repeatedField.getName()));
      }
      return isEligible;
    }
    return false;
  }

  private static Optional<Field> toComplexField(GroupType complexField, ArrowType arrowType, SchemaDerivationHelper schemaHelper, boolean convertToStruct) {
    List<Field> subFields = new ArrayList<>(complexField.getFieldCount());
    for (int fieldIdx = 0; fieldIdx < complexField.getFieldCount(); fieldIdx++) {
      Optional<Field> subField = toField(complexField.getType(fieldIdx), schemaHelper);
      if (!subField.isPresent()) {
        return Optional.empty();
      } else {
        subFields.add(subField.get());
      }
    }
    Field field = new Field(complexField.getName(), new FieldType(true, arrowType, null), subFields);
    if (complexField.isRepetition(REPEATED)
      && !convertToStruct
      && OriginalType.MAP_KEY_VALUE != complexField.getOriginalType()
      && OriginalType.MAP != complexField.getOriginalType()
      && OriginalType.LIST != complexField.getOriginalType()) {
      Field listChild = new Field("$data$", new FieldType(true, field.getType(), field.getDictionary()), field.getChildren());
      return Optional.of(new Field(field.getName(), new FieldType(true, new ArrowType.List(), field.getDictionary()), Arrays.asList(listChild)));
    }
    return Optional.of(field);
  }

  private static boolean includeChunk(String name, SortedSet<String> allProjectedPaths, boolean allowPartialMatch) {
    boolean included =  allProjectedPaths == null ||
      allProjectedPaths.contains(name);
    if (!included && allowPartialMatch) {
      included = !allProjectedPaths.subSet(name + ".", name + "." + Character.MAX_VALUE).isEmpty();
    }
    return included;
  }

  public static SortedMap<String, ColumnChunkMetaData> unWrapParquetSchema(BlockMetaData block, MessageType schema,
                                                                           SortedSet<String> allProjectedPaths) {
    SortedMap<String, ColumnChunkMetaData> unwrappedColumns = new TreeMap<>(String::compareToIgnoreCase);
    for(ColumnChunkMetaData c : block.getColumns()) {
      ColumnDescriptor columnDesc = schema.getColumnDescription(c.getPath().toArray());
      Type type = schema;
      List<String> columnPath = Lists.newArrayList(columnDesc.getPath());
      int index = 0;
      boolean chunkIncluded = includeChunk(columnPath.get(0), allProjectedPaths, true);
      if (!chunkIncluded) {
        continue;
      }
      chunkIncluded = false;
      StringBuilder stringBuilder = new StringBuilder();
      while (!type.isPrimitive()) {
        type = type.asGroupType().getType(columnPath.get(index));
        if (index > 0) {
          stringBuilder.append(".");
        }
        stringBuilder.append(columnPath.get(index));
        chunkIncluded = chunkIncluded || includeChunk(stringBuilder.toString(), allProjectedPaths, false);
        if (type.getOriginalType() == OriginalType.LIST)  {
          if (!LogicalListL1Converter.isSupportedSchema(type.asGroupType())) {
            throw UserException.dataReadError()
              .message("Unsupported LOGICAL LIST parquet schema")
              .addContext("schema: %s", schema)
              .buildSilently();
          }
          type = type.asGroupType().getType(columnPath.get(index+1));
          stringBuilder.append(".list");
          columnPath.remove(index+1);

          type = type.asGroupType().getType(columnPath.get(index+1));

          while(type.getOriginalType() == OriginalType.LIST)  {
            if (!LogicalListL1Converter.isSupportedSchema(type.asGroupType())) {
              throw UserException.dataReadError()
                .message("Unsupported LOGICAL LIST parquet schema")
                .addContext("schema: %s", schema)
                .buildSilently();
            }
            stringBuilder.append(".element");
            chunkIncluded = chunkIncluded || includeChunk(stringBuilder.toString(), allProjectedPaths, false);
            columnPath.remove(index+1);

            type = type.asGroupType().getType(columnPath.get(index+1));
            stringBuilder.append(".list");
            columnPath.remove(index+1);

            type = type.asGroupType().getType(columnPath.get(index+1));
          }

          stringBuilder.append(".element");
          chunkIncluded = chunkIncluded || includeChunk(stringBuilder.toString(), allProjectedPaths, false);
          columnPath.remove(index+1);
        }
        if (type.getOriginalType() == OriginalType.MAP) {
          index++;
          Preconditions.checkState(type.asGroupType().getFieldCount() == 1, "Map column has more than one field");
          type = type.asGroupType().getFields().get(0);
          stringBuilder.append("." + type.getName());
          chunkIncluded = chunkIncluded || includeChunk(stringBuilder.toString(), allProjectedPaths, false);
          stringBuilder.append(".list.element");
          chunkIncluded = chunkIncluded || includeChunk(stringBuilder.toString(), allProjectedPaths, false);
        }
        index++;
      }
      if (chunkIncluded) {
        unwrappedColumns.put(stringBuilder.toString().toLowerCase(), c);
      }
    }
    return unwrappedColumns;
  }
}
