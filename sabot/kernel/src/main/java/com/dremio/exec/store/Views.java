/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.dotfile.View.FieldType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Utilities to persist a view definition in protos
 */
public class Views {

  private static <E extends Enum<E>> E en(Class<E> e, String name) {
    return name == null ? null : Enum.valueOf(e, name);
  }

  public static View fieldTypesToView(String name, String sql, List<ViewFieldType> fieldTypes, List<String> context) {
    if (fieldTypes == null) {
      throw new NullPointerException();
    }
    List<FieldType> fields = new ArrayList<>();
    for (ViewFieldType sqlField : fieldTypes) {
      FieldType fieldType = new View.FieldType(
          sqlField.getName(),
          en(SqlTypeName.class, sqlField.getType()),
          sqlField.getPrecision(), sqlField.getScale(),
          en(TimeUnit.class, sqlField.getStartUnit()),
          en(TimeUnit.class, sqlField.getEndUnit()),
          sqlField.getFractionalSecondPrecision(),
          sqlField.getIsNullable()
          );
      fields.add(fieldType);
    }
    return new View(name, sql, fields, context);
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
      sqlField.setTypeFamily(fieldType.getType().getFamily().toString());
      sqlFields.add(sqlField);
    }
    return sqlFields;
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
