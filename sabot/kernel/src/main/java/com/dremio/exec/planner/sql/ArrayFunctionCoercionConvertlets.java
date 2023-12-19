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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

/**
 * Convertlet to add coercion for ARRAY_FUNCTIONS.
 *
 * For example if the user writes a query like:
 *
 * SELECT ARRAY_APPEND(ARRAY[1, 2, 3], 4.5)
 *
 * and the element type does not match up
 * the user will get an error saying that the array contains INT which doesn’t line up with the decimal type.
 *
 * We can add a cast for the user like so to simulate implict coercion:
 *
 * SELECT ARRAY_APPEND(CAST(ARRAY[1, 2, 3] AS DECIMAL ARRAY), 4.5)
 *
 */
public final class ArrayFunctionCoercionConvertlets {
  private ArrayFunctionCoercionConvertlets() {}

  public static final class ElementAndArray implements SqlRexConvertlet {
    public static final ElementAndArray INSTANCE = new ElementAndArray();

    private ElementAndArray() {}

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      List<RexNode> operands = call.getOperandList()
        .stream()
        .map(cx::convertExpression)
        .collect(Collectors.toList());

      RexNode element = operands.get(0);
      RexNode array = operands.get(1);

      RexBuilder rexBuilder = cx.getRexBuilder();
      Pair<RexNode, RexNode> ensuredArrayAndElement = ensureArrayAndElement(rexBuilder, array, element);
      RexNode ensuredArray = ensuredArrayAndElement.left;
      RexNode ensuredElement = ensuredArrayAndElement.right;

      return rexBuilder.makeCall(call.getOperator(), ensuredElement, ensuredArray);
    }
  }

  public static final class ArrayAndElement implements SqlRexConvertlet {
    public static final ArrayAndElement INSTANCE = new ArrayAndElement();

    private ArrayAndElement() {}

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      List<RexNode> operands = call.getOperandList()
        .stream()
        .map(cx::convertExpression)
        .collect(Collectors.toList());

      RexNode array = operands.get(0);
      RexNode element = operands.get(1);

      RexBuilder rexBuilder = cx.getRexBuilder();
      Pair<RexNode, RexNode> ensuredArrayAndElement = ensureArrayAndElement(rexBuilder, array, element);
      RexNode ensuredArray = ensuredArrayAndElement.left;
      RexNode ensuredElement = ensuredArrayAndElement.right;

      return rexBuilder.makeCall(call.getOperator(), ensuredArray, ensuredElement);
    }
  }

  public static final class ArrayAndArray implements SqlRexConvertlet {
    public static final ArrayAndArray INSTANCE = new ArrayAndArray();

    private ArrayAndArray() {}

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      List<RexNode> operands = call.getOperandList()
        .stream()
        .map(cx::convertExpression)
        .collect(Collectors.toList());

      // Calcite has a bug where it doesn't properly take the least restrictive of ARRAY<STRING> but it does do STRING properly
      // Basically the largest precision isn't taken.
      RexBuilder rexBuilder = cx.getRexBuilder();
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      List<RelDataType> componentTypes = RexUtil.types(operands)
        .stream()
        .map(RelDataType::getComponentType)
        .collect(Collectors.toList());
      RelDataType leastRestrictiveComponentType = typeFactory.leastRestrictive(componentTypes);
      RelDataType leastRestrictiveArrayType = typeFactory.createArrayType(leastRestrictiveComponentType, -1);

      List<RexNode> ensuredOperands;
      if (leastRestrictiveComponentType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
        // For string types the only difference can be the precision, so we just no op
        ensuredOperands = operands;
      } else {
        ensuredOperands = operands
          .stream()
          .map(operand -> rexBuilder.ensureType(leastRestrictiveArrayType, operand, true))
          .collect(Collectors.toList());
      }

      return rexBuilder.makeCall(call.getOperator(), ensuredOperands);
    }
  }

  private static Pair<RexNode, RexNode> ensureArrayAndElement(
    RexBuilder rexBuilder,
    RexNode array,
    RexNode element) {
    RelDataType arrayType = array.getType();
    RelDataType elementType = element.getType();

    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    List<RelDataType> elementTypes = ImmutableList.of(arrayType.getComponentType(), elementType);
    RelDataType newElementType = typeFactory.leastRestrictive(elementTypes);

    RelDataType newArrayType = rexBuilder.getTypeFactory().createArrayType(newElementType, -1);
    newArrayType = typeFactory.createTypeWithNullability(newArrayType, array.getType().isNullable());

    RexNode arrayEnsured = rexBuilder.ensureType(newArrayType, array, true);
    // We don't want to add a cast if the types are both strings and only differ in precision
    RexNode elementEnsured;
    if (newElementType.getSqlTypeName() == SqlTypeName.VARCHAR && elementType.getSqlTypeName() == SqlTypeName.VARCHAR) {
      elementEnsured = element;
    } else {
      elementEnsured = rexBuilder.ensureType(newElementType, element, true);
    }

    return Pair.of(arrayEnsured, elementEnsured);
  }
}
