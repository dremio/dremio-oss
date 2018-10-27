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
package com.dremio.sabot.op.llvm.expr;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.FloatExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.expr.ExpressionSplitHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.collect.Lists;
import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.Set;

/**
 * Returns whether an expression can be executed in native code.
 * Also, annotates the expression in case the expression can be evaluated in native code.
 */
public class GandivaPushdownSieve extends AbstractExprVisitor<Boolean, Void, GandivaException> implements ExpressionSplitHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GandivaPushdownSieve.class);

  private static final List<String> supportedBooleanOperators = Lists.newArrayList("booleanAnd",
    "booleanOr");

  public boolean canEvaluate(BatchSchema.SelectionVectorMode incomingSelectionVectorMode, LogicalExpression expr) {
    try {
      // we do not support var len output.
      if (!isSupportedReturnType(expr.getCompleteType())) {
        return false;
      }

      if (incomingSelectionVectorMode == BatchSchema.SelectionVectorMode.NONE &&
        expr.accept(this, null)) {
        return true;
      }
    } catch (GandivaException gandivaException) {
      logger.error("Exception in running sieve", gandivaException);
      // ignore exception
      // fall-through and return false
    }

    return false;
  }

  @Override
  public boolean canSplitAt(LogicalExpression e) {
    try {
      return isSupportedReturnType(e.getCompleteType());
    } catch (GandivaException e1) {
      // We dont know if we can split at this point due to the exception
      // Assuming that we can't
      logger.error("Exception in running sieve", e1);
      return false;
    }
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, Void value) throws GandivaException {
    if (e instanceof ValueVectorReadExpression) {
      if (isSupportedField((ValueVectorReadExpression) e)) {
        e.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
        return true;
      } else {
        return false;
      }
    } else {
      logger.info("GandivaPushdownSieve : unknown expression type {}", e.getClass().getCanonicalName());
      return false;
    }
  }

  private boolean isSupportedField(ValueVectorReadExpression e) throws GandivaException {
    TypedFieldId fieldId = e.getTypedFieldId();
    // use intermediate type to understand if it is complex.
    CompleteType type = fieldId.getIntermediateType() != null ? fieldId.getIntermediateType()
      : fieldId.getFinalType();

    // reads into structs and lists will have same types
    // and is distinguished by field id list having more
    // than 1 element.
    boolean isComplexRead = fieldId.getFieldIds().length > 1;
    return isSupportedType(type) && !isComplexRead;
  }

  @Override
  public Boolean visitBooleanOperator(BooleanOperator operator, Void value) throws GandivaException {
    // check that operator is supported.
    if (!supportedBooleanOperators.contains(operator.getName())) {
      return false;
    }

    // check return type;
    if (!isSupportedType(operator.getCompleteType())) {
      return false;
    }

    for (LogicalExpression arg : operator.args) {
      if (!arg.accept(this, value)) {
        return false;
      }
    }

    operator.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
    return true;
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws GandivaException {

    // ignore hive functions.
    // will be removed once we integrate into dremio fn repository.
    if (holder.getHolder() == null || !(holder.getHolder() instanceof BaseFunctionHolder)) {
      return false;
    }

    if (!isSupportedType(holder.getCompleteType())) {
      return false;
    }

    if (!isFunctionSupported(holder)) {
      return false;
    }

    for (LogicalExpression arg : holder.args) {
      if (!arg.accept(this, value)) {
        return false;
      }
    }

    holder.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
    return true;
  }

  private boolean isFunctionSupported(FunctionHolderExpression holder) throws GandivaException {
    Set<FunctionSignature> supportedFunctions = ExpressionRegistry.getInstance()
      .getSupportedFunctions();
    String name = holder.getName();
    ArrowType returnType = holder.getCompleteType().getType();
    List<ArrowType> argTypes = Lists.newArrayList();
    for (LogicalExpression arg : holder.args) {
      argTypes.add(arg.getCompleteType().getType());
    }
    FunctionSignature functionSignature = new FunctionSignature(name, returnType, argTypes);
    if (!supportedFunctions.contains(functionSignature) || !isSpecificFuntionSupported(holder)) {
      logger.info("function signature not supported in gandiva : " + functionSignature);
      return false;
    }
    return true;
  }

  /**
   * Checks for any overrides for specific functions
   * @param holder
   * @return true if function can be evaluated in Gandiva.
   */
  private boolean isSpecificFuntionSupported(FunctionHolderExpression holder) {
    // gandiva cannot yet process date patterns with timezones in it.
    if (holder.getName().equalsIgnoreCase("to_date")) {
       ValueExpressions.QuotedString pattern = (ValueExpressions.QuotedString)holder.args.get(1);
       return !pattern.getString().contains("tz");
    }
    return true;
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, Void value) throws GandivaException {
    // check return type of the expression first.
    if (!isSupportedType(ifExpr.getCompleteType())) {
      return false;
    }

    IfCondition conditions = ifExpr.ifCondition;
    if (!ifExpr.elseExpression.accept(this, value) ||
      !conditions.condition.accept(this, value) ||
      !conditions.expression.accept(this, value)) {
      return false;
    }

    ifExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
    return true;
  }

  private Boolean annotateAndCheckSupportedType(LogicalExpression e) throws GandivaException {
    if (isSupportedType(e.getCompleteType())) {
      e.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      return true;
    }

    return false;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(e);
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression booleanExpr, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(booleanExpr);
  }

  @Override
  public Boolean visitIntConstant(IntExpression intExpr, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(intExpr);
  }

  @Override
  public Boolean visitLongConstant(LongExpression longExpr, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(longExpr);
  }

  @Override
  public Boolean visitFloatConstant(FloatExpression floatExpr, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(floatExpr);
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression doubleExpr, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(doubleExpr);
  }

  @Override
  public Boolean visitNullConstant(TypedNullConstant nullConstant, Void value) throws GandivaException {
    return annotateAndCheckSupportedType(nullConstant);
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, Void value) throws GandivaException {
    throw new GandivaException("Function calls should have been replaced before operator.");
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, Void value) throws GandivaException {
    // to do : needed?
    return visitUnknown(path, value);
  }


  @Override
  public Boolean visitDecimalConstant(ValueExpressions.DecimalExpression decExpr,
                                      Void value) throws GandivaException {
    return annotateAndCheckSupportedType(decExpr);
  }

  @Override
  public Boolean visitDateConstant(ValueExpressions.DateExpression dateExpr, Void value)
    throws GandivaException {
    return annotateAndCheckSupportedType(dateExpr);
  }

  @Override
  public Boolean visitTimeConstant(ValueExpressions.TimeExpression timExpr,
                                   Void value) throws GandivaException {
    return annotateAndCheckSupportedType(timExpr);
  }

  @Override
  public Boolean visitTimeStampConstant(ValueExpressions.TimeStampExpression timeStampExpr,
                                        Void value) throws GandivaException {
    return annotateAndCheckSupportedType(timeStampExpr);
  }

  @Override
  public Boolean visitIntervalYearConstant(ValueExpressions.IntervalYearExpression intervalYearExpr,
                                           Void value) throws GandivaException {
    return annotateAndCheckSupportedType(intervalYearExpr);
  }

  @Override
  public Boolean visitIntervalDayConstant(ValueExpressions.IntervalDayExpression intervalDayExpr,
                                          Void value) throws GandivaException {
    return annotateAndCheckSupportedType(intervalDayExpr);
  }

  @Override
  public Boolean visitNullExpression(NullExpression e, Void value) throws GandivaException {
    return visitUnknown(e, value);
  }

  private boolean isSupportedType(CompleteType type) throws GandivaException {
    final Set<ArrowType> supportedTypes = ExpressionRegistry.getInstance().getSupportedTypes();
    return supportedTypes.contains(type.getType());
  }

  private boolean isSupportedReturnType(CompleteType type) throws GandivaException {
    return isSupportedType(type) && type.isFixedWidthScalar();
  }
}
