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
package com.dremio.exec.planner.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 * Visitor to flatten the nested schema. If a joiner is given, it will join the the nested names
 * only for the first level nodes using this joiner and the rest will be used as leaf node names. If
 * no joiner is provided, it will use all the leaf nodes as the names. For example if the nested
 * structure looks like:
 *
 * <p><br>
 * level1 (STRUCT) - a1 (VARCHAR) - b1 (BIGINT) - c1 (DOUBLE) - level2 (STRUCT) - a2 (INTEGER) -
 * level3 (STRUCT) - a3 (BOOLEAN) - b3 (FLOAT)
 *
 * <p><br>
 * The flattened output will be:
 *
 * <p>If a joiner is given, say "_": level1_a1, level1_b1, level1_c1, a2, a3, b3
 *
 * <p>If no joiner is given: a1, b1, c1, a2, a3, b3
 *
 * <p><br>
 * The reason we join only the first level with a joiner is because of the need for this flattener
 * during DeltaLake scan expansion.
 */
public class ComplexSchemaFlattener {
  private final String joiner;
  private final RexBuilder rexBuilder;
  private final List<RexNode> exps; // Flattened expressions
  private final List<RelDataType> typeList; // Expression types
  private final List<String> fields; // Expression names
  private final Set<String> fieldSet;

  public ComplexSchemaFlattener(RexBuilder rexBuilder, String joiner) {
    this.rexBuilder = rexBuilder;
    this.joiner = joiner;
    this.exps = new ArrayList<>();
    this.typeList = new ArrayList<>();
    this.fields = new ArrayList<>();
    this.fieldSet = new HashSet<>();
  }

  public void flatten(RelDataType rowType) {
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    for (int i = 0; i < fieldList.size(); i++) {
      RelDataTypeField field = fieldList.get(i);
      flatten(field, i);
    }
  }

  public void flatten(RelDataTypeField parent, int i) {
    if (parent.getType().getStructKind() == StructKind.FULLY_QUALIFIED) {
      for (RelDataTypeField child : parent.getType().getFieldList()) {
        RexNode rexNode =
            rexBuilder.makeFieldAccess(
                rexBuilder.makeInputRef(parent.getType(), i), child.getName(), false);
        if (child.getType().getStructKind() == StructKind.FULLY_QUALIFIED) {
          flattenRecursive(child, rexNode);
        } else {
          String fieldName =
              joiner == null ? child.getName() : parent.getName() + joiner + child.getName();
          exps.add(rexNode);
          typeList.add(rexNode.getType());
          uniquifyFieldName(fieldName);
        }
      }
    } else {
      RexNode rexNode = rexBuilder.makeInputRef(parent.getType(), i);
      exps.add(rexNode);
      typeList.add(rexNode.getType());
      uniquifyFieldName(parent.getName());
    }
  }

  private void flattenRecursive(RelDataTypeField parent, RexNode rexNode) {
    if (parent.getType().getStructKind() == StructKind.FULLY_QUALIFIED) {
      for (RelDataTypeField child : parent.getType().getFieldList()) {
        RexNode nestedNode = rexBuilder.makeFieldAccess(rexNode, child.getName(), false);
        flattenRecursive(child, nestedNode);
      }
    } else {
      exps.add(rexNode);
      typeList.add(rexNode.getType());
      uniquifyFieldName(parent.getName());
    }
  }

  private void uniquifyFieldName(String fieldName) {
    if (!fieldSet.contains(fieldName)) {
      fieldSet.add(fieldName);
      fields.add(fieldName);
    } else {
      // While this may not be the best way to uniquify field names, we don't expect
      // a lot of nested fields to have the same name.
      int i = 1;
      String newFieldName = fieldName + i++;
      while (fieldSet.contains(newFieldName)) {
        newFieldName = fieldName + i++;
      }
      fieldSet.add(newFieldName);
      fields.add(newFieldName);
    }
  }

  public List<RexNode> getExps() {
    return exps;
  }

  public List<RelDataType> getTypeList() {
    return typeList;
  }

  public List<String> getFields() {
    return fields;
  }
}
