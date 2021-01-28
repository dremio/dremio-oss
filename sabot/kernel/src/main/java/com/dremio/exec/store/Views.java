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
package com.dremio.exec.store;

import static com.dremio.exec.util.ViewFieldsHelper.serializeField;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.dotfile.View.FieldType;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * Utilities to persist a view definition in protos
 */
public class Views {

  private static <E extends Enum<E>> E en(Class<E> e, String name) {
    return name == null ? null : Enum.valueOf(e, name);
  }

  public static View fieldTypesToView(String name, String sql, List<ViewFieldType> fieldTypes, List<String> context) {
    return fieldTypesToView(name, sql, fieldTypes, context, null);
  }

  public static View fieldTypesToView(String name, String sql, List<ViewFieldType> fieldTypes, List<String> context, BatchSchema schema) {
    if (fieldTypes == null) {
      throw new NullPointerException();
    }
    List<FieldType> fields = new ArrayList<>();
    boolean requiresUpdate = false;
    for (ViewFieldType sqlField : fieldTypes) {
      SqlTypeName type = en(SqlTypeName.class, sqlField.getType());
      final String fieldName = sqlField.getName();
      Field field = null;
      if (sqlField.getSerializedField() != null) {
        field = ViewFieldsHelper.deserializeField(sqlField.getSerializedField());
      }
      if (field == null && (type.equals(SqlTypeName.ANY) || type.equals(SqlTypeName.ARRAY)) && schema != null) {
        // update old view to support complex type.
        // get complex field type and information from schema
        Field fieldFromSchema = schema.findField(fieldName);
        if (fieldFromSchema != null) {
          SqlTypeName fieldType = TypeInferenceUtils.getCalciteTypeFromMinorType(
            MajorTypeHelper.getMinorTypeFromArrowMinorType(getMinorTypeForArrowType(fieldFromSchema.getFieldType().getType())));
          if (isComplexType(fieldType)) {
            field = fieldFromSchema;
            type = fieldType;
            requiresUpdate = true;
          }
        }
      }
      FieldType fieldType = new View.FieldType(
        fieldName,
        type,
        sqlField.getPrecision(), sqlField.getScale(),
        en(TimeUnit.class, sqlField.getStartUnit()),
        en(TimeUnit.class, sqlField.getEndUnit()),
        sqlField.getFractionalSecondPrecision(),
        sqlField.getIsNullable(),
        field
      );
      fields.add(fieldType);
    }
    return new View(name, sql, fields, null, context, requiresUpdate);
  }

  private static String name(Enum<?> e) {
    return e == null ? null : e.name();
  }

  public static List<ViewFieldType> viewToFieldTypes(List<FieldType> fields) {
    List<ViewFieldType> sqlFields = new ArrayList<>();
    for (FieldType fieldType : fields) {
      ViewFieldType sqlField =
          new ViewFieldType(fieldType.getName(), fieldType.getType().getName());
      sqlField.setPrecision(fieldType.getPrecision());
      sqlField.setScale(fieldType.getScale());
      sqlField.setStartUnit(name(fieldType.getStartUnit()));
      sqlField.setEndUnit(name(fieldType.getEndUnit()));
      sqlField.setFractionalSecondPrecision(fieldType.getFractionalSecondPrecision());
      sqlField.setIsNullable(fieldType.getIsNullable());
      String typeFamily = fieldType.getType().getFamily() == null ? null : fieldType.getType().getFamily().toString();
      sqlField.setTypeFamily(typeFamily);
      sqlFields.add(sqlField);
      Field field = fieldType.getField();
      if (field != null) {
        sqlField.setSerializedField(ByteString.copyFrom(serializeField(field)));
      }
    }
    return sqlFields;
  }

  public static boolean isComplexType(SqlTypeName type) {
    return type.equals(SqlTypeName.ARRAY) || type.equals(SqlTypeName.MAP) || type.equals(SqlTypeName.ROW);
  }

  public static List<FieldType> relDataTypeToFieldType(final RelDataType rowType) {
    return Lists.transform(rowType.getFieldList(), new Function<RelDataTypeField, FieldType>() {
      @Nullable
      @Override
      public FieldType apply(@Nullable final RelDataTypeField field) {
        return new FieldType(field.getName(), field.getType());
      }
    });
  }
}
