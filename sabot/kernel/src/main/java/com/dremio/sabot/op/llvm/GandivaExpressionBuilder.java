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
package com.dremio.sabot.op.llvm;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.FloatExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.record.VectorAccessible;
import com.google.common.base.Preconditions;

/**
 * Convert LogicalExpression to Gandiva Expressions.
 */
public class GandivaExpressionBuilder extends AbstractExprVisitor<TreeNode, Void, RuntimeException> {
  private final VectorAccessible incoming;
  private final Set<Field> referencedFields;

  private GandivaExpressionBuilder(VectorAccessible incoming, Set<Field> referencedFields) {
    this.incoming = incoming;
    this.referencedFields = referencedFields;
  }

  /**
   * Take an expression tree and convert it into a Gandiva Expression.
   */
  public static ExpressionTree serializeExpr(VectorAccessible incoming, LogicalExpression ex,
                                             FieldVector out, Set<Field> referencedFields) {
    GandivaExpressionBuilder serializer = new GandivaExpressionBuilder(incoming, referencedFields);
    TreeNode expr = ex.accept(serializer, null);
    return TreeBuilder.makeExpression(expr, out.getField());
  }

  /**
   * Converts a logical expression into a filter condition
   * @param incoming the Schema for the incoming batch
   * @param expr logical expression to serialize
   * @return Condition for the expression.
   */
  public static Condition serializeExprToCondition(VectorAccessible incoming,
                                                   LogicalExpression expr,
                                                   Set<Field> referencedFields) {
    GandivaExpressionBuilder serializer = new GandivaExpressionBuilder(incoming, referencedFields);
    TreeNode expression = expr.accept(serializer, null);
    return TreeBuilder.makeCondition(expression);
  }

  /*
   * work-around for older JDK compilers (see DX-13315).
   */
  private TreeNode acceptExpression(LogicalExpression expression) throws RuntimeException {
    try {
      return expression.accept(GandivaExpressionBuilder.this, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TreeNode visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
    AbstractFunctionHolder definition = (AbstractFunctionHolder)holder.getHolder();
    Preconditions.checkState(definition instanceof BaseFunctionHolder || definition instanceof
      GandivaFunctionHolder);

    List<TreeNode> children = holder.args
      .stream()
      .map(this::acceptExpression)
      .collect(Collectors.toList());

    return TreeBuilder.makeFunction(holder.getName(), children, definition.getReturnType(holder.args).getType());
  }

  @Override
  public TreeNode visitIfExpression(IfExpression inExpr, Void value) {
    TreeNode ifCondition = inExpr.ifCondition.condition.accept(GandivaExpressionBuilder.this, null);
    TreeNode thenE = inExpr.ifCondition.expression.accept(GandivaExpressionBuilder.this, null);
    TreeNode elseE = inExpr.elseExpression.accept(GandivaExpressionBuilder.this, null);

    return TreeBuilder.makeIf(ifCondition, thenE, elseE, inExpr.getCompleteType().getType());
  }

  @Override
  public TreeNode visitBooleanOperator(BooleanOperator operator, Void value) {
    List<TreeNode> children = operator.args
      .stream()
      .map(this::acceptExpression)
      .collect(Collectors.toList());

    if (operator.isAnd()) {
      return TreeBuilder.makeAnd(children);
    } else if (operator.isOr()) {
      return TreeBuilder.makeOr(children);
    } else {
      throw new UnsupportedOperationException("BooleanOperator can only be booleanAnd, booleanOr. You are using " +
        operator.getName());
    }
  }

  @Override
  public TreeNode visitUnknown(LogicalExpression e, Void value) {
    if (e instanceof ValueVectorReadExpression) {
      return visitValueVectorReadExpression((ValueVectorReadExpression) e, value);
    } else {
      return super.visitUnknown(e, value);
    }
  }

  private TreeNode visitValueVectorReadExpression(ValueVectorReadExpression readExpr, Void value) {

    FieldVector vector = incoming.getValueAccessorById(FieldVector.class,
                                                       readExpr.getTypedFieldId().getFieldIds())
                                 .getValueVector();
    referencedFields.add(vector.getField());
    return TreeBuilder.makeField(vector.getField());
  }

  @Override
  public TreeNode visitQuotedStringConstant(QuotedString e, Void value) {
    return TreeBuilder.makeStringLiteral(e.getString());
  }

  @Override
  public TreeNode visitIntConstant(IntExpression intExpr, Void value) {
    return TreeBuilder.makeLiteral(intExpr.getInt());
  }

  @Override
  public TreeNode visitLongConstant(LongExpression longExpr, Void value) {
    return TreeBuilder.makeLiteral(longExpr.getLong());
  }

  @Override
  public TreeNode visitFloatConstant(FloatExpression floatExpr, Void value) {
    return TreeBuilder.makeLiteral(floatExpr.getFloat());
  }

  @Override
  public TreeNode visitDoubleConstant(DoubleExpression doubleExpr, Void value) {
    return TreeBuilder.makeLiteral(doubleExpr.getDouble());
  }

  @Override
  public TreeNode visitBooleanConstant(BooleanExpression booleanExpr, Void value) {
    return TreeBuilder.makeLiteral(booleanExpr.getBoolean());
  }

  @Override
  public TreeNode visitNullConstant(TypedNullConstant constant, Void value) {
    return TreeBuilder.makeNull(constant.getCompleteType().getType());
  }

}
