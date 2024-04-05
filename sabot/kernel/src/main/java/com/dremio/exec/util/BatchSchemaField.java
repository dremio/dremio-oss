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
package com.dremio.exec.util;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Class derived from Arrow Field class. Helps in overriding type names */
public class BatchSchemaField extends Field {
  public BatchSchemaField(String name, boolean nullable, ArrowType type, List<Field> children) {
    super(name, new FieldType(nullable, type, null), children);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (this.getName() != null) {
      sb.append(this.getName()).append(": ");
    }

    // Calcite mapping gives MAP for STRUCT and ARRAY for LIST.
    // So, handle complex types separately
    if (this.getFieldType().getType().isComplex()) {
      sb.append(CompleteType.fromField(this).toMinorType());
    } else {
      sb.append(
          CalciteArrowHelper.getCalciteTypeFromMinorType(
              CompleteType.fromField(this).toMinorType()));
    }

    // append child field types
    if (!this.getChildren().isEmpty()) {
      sb.append("<")
          .append(
              (String)
                  this.getChildren().stream()
                      .map(
                          (t) -> {
                            return t.toString();
                          })
                      .collect(Collectors.joining(", ")))
          .append(">");
    }

    return sb.toString();
  }

  public static BatchSchemaField fromField(Field field) {
    List<Field> children =
        field.getChildren().stream().map(BatchSchemaField::fromField).collect(Collectors.toList());

    return new BatchSchemaField(field.getName(), field.isNullable(), field.getType(), children);
  }
}
