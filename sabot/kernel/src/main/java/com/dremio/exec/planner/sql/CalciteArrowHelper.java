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
package com.dremio.exec.planner.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

import io.protostuff.ByteString;
/**
 * A set of tools for translating between Calcite and Arrow types.
 */
public class CalciteArrowHelper {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CalciteArrowHelper.class);

  public static class Schema {

    private final BatchSchema bs;

    private Schema(BatchSchema bs) {
      this.bs = bs;
    }

    /**
     * Converts a Schema as RelDataType. The method maps ArrowType fields to Calcite type fields.
     * If a filtering predicate is provided, the method would only add fields that are permitted into RelDataType.
     *
     * @param factory the RelDataTypeFactory to convert ArrowTypes to Calcite data types.
     * @param inclusionPredicate the inclusion predicate that filters the BatchSchema. Can be {@code null} if no filtering is required.
     * @return a RelDataType containing all fields permitted by the inclusionPredicate.
     */
    public RelDataType toCalciteRecordType(RelDataTypeFactory factory, Function<Field, Boolean> inclusionPredicate, boolean withComplexTypeSupport) {
      FieldInfoBuilder builder = new FieldInfoBuilder(factory);



      if (inclusionPredicate == null) {
        bs.forEach(f -> builder.add(f.getName(), toCalciteType(f, factory, withComplexTypeSupport)));
      } else {
        bs.forEach(f -> {
          if (inclusionPredicate.apply(f)) {
            builder.add(f.getName(), toCalciteType(f, factory, withComplexTypeSupport));}});
      }

      RelDataType rowType = builder.build();

      if(rowType.getFieldCount() == 0){
        throw UserException.dataReadError().message("Selected table has no columns.").build(logger);
      }

      return rowType;
    }

    public RelDataType toCalciteRecordType(RelDataTypeFactory factory, boolean withComplexTypeSupport){
      return toCalciteRecordType(factory, null, withComplexTypeSupport);
    }
  }

  public static Schema wrap(BatchSchema bs) {
    return new Schema(bs);
  }

  public static CompleteTypeWrapper wrap(CompleteType ct) {
    return new CompleteTypeWrapper(ct);
  }

  public static BatchSchema fromDataset(DatasetConfig config){
    try{
      ByteString bytes = DatasetHelper.getSchemaBytes(config);
      if(bytes == null){
        throw new IllegalStateException(String.format("Schema is currently unavailable for dataset %s.", PathUtils.constructFullPath(config.getFullPathList())));
      }
      return BatchSchema.deserialize(bytes);
    }catch(Exception ex){
      throw new IllegalStateException(String.format("Schema for dataset %s is corrupt.", PathUtils.constructFullPath(config.getFullPathList())), ex);
    }
  }

  public static Optional<Field> fieldFromCalciteRowType(final String name, final RelDataType relDataType) {
    MinorType minorType = TypeInferenceUtils.getMinorTypeFromCalciteType(relDataType);
    if (minorType != null) {
      final TypeProtos.MajorType majorType;
      if (minorType == TypeProtos.MinorType.DECIMAL) {
        majorType = Types.withScaleAndPrecision(
            minorType, TypeProtos.DataMode.OPTIONAL, relDataType.getScale(), relDataType.getPrecision());
      } else if (minorType == MinorType.STRUCT) {
        return Optional.of(getStructField(name, relDataType));
      } else if (minorType == MinorType.LIST) {
        return Optional.of(getListField(name, relDataType));
      } else {
        majorType = Types.optional(minorType);
      }

      return Optional.of(MajorTypeHelper.getFieldForNameAndMajorType(name, majorType));
    }
    return Optional.empty();
  }

  private static Field getStructField(String name, RelDataType relDataType) {
    final List<Field> children = new ArrayList<>();
    for (Map.Entry<String, RelDataType> field : relDataType.getFieldList()) {
      fieldFromCalciteRowType(field.getKey(), field.getValue()).ifPresent(children::add);
    }
    return new Field(
      name,
      true,
      MajorTypeHelper.getArrowTypeForMajorType(Types.optional(MinorType.STRUCT)),
      children
    );
  }

  private static Field getListField(String name, RelDataType relDataType) {
    final List<Field> onlyChild = new ArrayList<>();
    fieldFromCalciteRowType("component", relDataType.getComponentType()).ifPresent(onlyChild::add);
    return new Field(
      name,
      true,
      MajorTypeHelper.getArrowTypeForMajorType(Types.optional(MinorType.LIST)),
      onlyChild
    );
  }

  public static BatchSchema fromCalciteRowType(final RelDataType relDataType) {
    Preconditions.checkArgument(relDataType.isStruct());

    SchemaBuilder builder = BatchSchema.newBuilder();
    for (Map.Entry<String, RelDataType> field : relDataType.getFieldList()) {
      fieldFromCalciteRowType(field.getKey(), field.getValue()).ifPresent(builder::addField);
    }
    return builder.build();
  }

  public static BatchSchema fromCalciteRowTypeJson(final RelDataType relDataType) {
    Preconditions.checkArgument(relDataType.isStruct());

    SchemaBuilder builder = BatchSchema.newBuilder();
    for (Map.Entry<String,RelDataType> field : relDataType.getFieldList()) {
      MinorType minorType = TypeInferenceUtils.getMinorTypeFromCalciteType(field.getValue());
      if (minorType != null) {

        // if we're using json/rels reader, the types are going to be larger than typical.
        if (minorType == MinorType.INT) {
          minorType = MinorType.BIGINT;
        } else if (minorType == MinorType.FLOAT4) {
          minorType = MinorType.FLOAT8;
        }

        final TypeProtos.MajorType majorType;
        if (minorType == TypeProtos.MinorType.DECIMAL) {
          majorType = Types.withScaleAndPrecision(
            minorType, TypeProtos.DataMode.OPTIONAL, field.getValue().getScale(), field.getValue().getPrecision());
        } else {
          majorType = Types.optional(minorType);
        }
        final Field f = MajorTypeHelper.getFieldForNameAndMajorType(field.getKey(), majorType);
        builder.addField(f);
      }
    }
    return builder.build();
  }

  /**
   * Given a Dremio's TypeProtos.MinorType, return a Calcite's corresponding SqlTypeName
   */
  public static SqlTypeName getCalciteTypeFromMinorType(final TypeProtos.MinorType type) {
    if(!CalciteTypeMaps.MINOR_TO_CALCITE_TYPE_MAPPING.containsKey(type)) {
      return SqlTypeName.ANY;
    }

    return CalciteTypeMaps.MINOR_TO_CALCITE_TYPE_MAPPING.get(type);
  }

  public static class CompleteTypeWrapper {

    private final CompleteType completeType;

    private CompleteTypeWrapper(CompleteType completeType) {
      this.completeType = completeType;
    }


    public RelDataType toCalciteType(RelDataTypeFactory typeFactory, boolean withComplexTypeSupport) {
      final MinorType type = completeType.toMinorType();

        if (completeType.isList()) {
          if (withComplexTypeSupport) {
            RelDataType childType = new CompleteTypeWrapper(completeType.getOnlyChildType()).toCalciteType(typeFactory, true);
            return typeFactory.createTypeWithNullability(typeFactory.createArrayType(childType, -1), true);
          } else {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
          }
        }
        if (completeType.isStruct()) {
          if (withComplexTypeSupport) {
            return convertFieldsToStruct(completeType.getChildren(), typeFactory, true);
          } else {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
          }
        }

      final SqlTypeName sqlTypeName = getCalciteTypeFromMinorType(type);

      if(completeType.isVariableWidthScalar()){
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName, 1 << 16), true);
      }

      if(completeType.isDecimal()){
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName, completeType.getPrecision(), completeType.getScale()), true);
      }

      if (completeType.getType().getTypeID() == ArrowTypeID.Timestamp ||
          completeType.getType().getTypeID() == ArrowTypeID.Time) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName, completeType.getPrecision()), true);
      }

      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
    }


    public RelDataType convertFieldsToStruct(List<Field> fields, RelDataTypeFactory typeFactory, boolean withComplexTypeSupport) {
      List<RelDataType> types = new ArrayList<>();
      List<String> names = new ArrayList<>();
      for (Field field : fields) {
        types.add(toCalciteFieldType(field, typeFactory, withComplexTypeSupport));
        names.add(field.getName());
      }
      return typeFactory.createTypeWithNullability(typeFactory.createStructType(types, names), true);
    }
  }

  public static RelDataType toCalciteType(Field field, RelDataTypeFactory typeFactory, boolean withComplexTypeSupport) {
    return wrap(CompleteType.fromField(field)).toCalciteType(typeFactory, withComplexTypeSupport);
  }

  public static RelDataType toCalciteFieldType(Field field, RelDataTypeFactory typeFactory, boolean withComplexTypeSupport) {
    return wrap(CompleteType.fromField(field)).toCalciteType(typeFactory, withComplexTypeSupport);
  }

  public static CompleteType fromRelAndMinorType(RelDataType type, MinorType minorType) {
    if (type.getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
      return CompleteType.fromDecimalPrecisionScale(type.getPrecision(), type.getScale());
    }
    return CompleteType.fromMinorType(minorType);
  }
}
