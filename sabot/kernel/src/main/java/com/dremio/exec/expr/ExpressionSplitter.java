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
package com.dremio.exec.expr;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorContainer;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits an expression into sub-expressions such that each sub-expression can be evaluated
 * by either Gandiva or Java. Each sub-expression has a unique name that is used to track
 * dependencies
 */
public class ExpressionSplitter extends AbstractExprVisitor<LogicalExpression, EvaluationType, RuntimeException> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionSplitter.class);
  private static final String DEFAULT_TMP_OUTPUT_NAME = "_split_expr";

  // flag to preserve short circuits in case of if-then-else and boolean operators
  public static boolean preserveShortCircuits = false;

  final List<NamedExpression> splitExpressions;
  final VectorContainer vectorContainer;
  final ExpressionSplitHelper gandivaSplitHelper;
  String outputFieldPrefix;
  int outputFieldCounter = 0;

  public ExpressionSplitter(VectorContainer container, ExpressionSplitHelper gandivaSplitHelper) {
    this(container, gandivaSplitHelper, ExpressionSplitter.DEFAULT_TMP_OUTPUT_NAME);
  }

  public ExpressionSplitter(VectorContainer container, ExpressionSplitHelper gandivaSplitHelper, String outputPrefix) {
    this.vectorContainer = container;
    this.gandivaSplitHelper = gandivaSplitHelper;
    this.splitExpressions = Lists.newArrayList();
    this.outputFieldPrefix = outputPrefix;
  }

  public List<NamedExpression> splitExpression(LogicalExpression expr) {
    LogicalExpression e = expr.accept(this, expr.getEvaluationType());
    splitAndGenerateVectorReadExpression(e);
    return splitExpressions;
  }

  // Generate unique name for the split
  private String getOutputNameForSplit() {
    String tempStr;

    do {
      tempStr = this.outputFieldPrefix + this.outputFieldCounter;
      this.outputFieldCounter++;
    } while (vectorContainer.getValueVectorId(SchemaPath.getSimplePath(tempStr)) != null);

    return tempStr;
  }

  private boolean gandivaSupportsReturnType(LogicalExpression expression) {
    return gandivaSplitHelper.canSplitAt(expression);
  }

  // Create a split at this expression
  // Adds the output field to the schema
  private ValueVectorReadExpression splitAndGenerateVectorReadExpression(LogicalExpression expr) {
    String exprName = getOutputNameForSplit();
    SchemaPath path = SchemaPath.getSimplePath(exprName);
    FieldReference ref = new FieldReference(path);
    this.splitExpressions.add(new NamedExpression(expr, ref));
    Field outputField = expr.getCompleteType().toField(ref);

    vectorContainer.addOrGet(outputField);
    TypedFieldId fieldId = vectorContainer.getValueVectorId(ref);
    return new ValueVectorReadExpression(fieldId);
  }

  // Checks if the expression is a candidate for split
  // Split only functions, if expressions and boolean operators
  private boolean candidateForSplit(LogicalExpression e) {
    if (e instanceof FunctionHolderExpression) {
      return true;
    }

    if (e instanceof BooleanOperator) {
      return true;
    }

    if (e instanceof IfExpression) {
      return true;
    }

    return false;
  }

  // Consider the following expression tree with 3 nodes P, N and C where N is P's child and C is N's child
  // N can be evaluated in Gandiva (but, Gandiva cannot return N's N return type), and
  // C cannot be evaluated in Gandiva
  //
  // Should we split at C or not? Consider the following 2 cases:
  // case 1. P can be evaluated in Gandiva and
  // case 2. P cannot be evaluated in Gandiva
  //
  // In both cases, there is no split at N. Case 2 doesn't split at N because Gandiva cannot return N's return type
  //
  // In case 1, we want to split at C because N is going to be evaluated in Gandiva
  // In case 2, we dont want to split at C because N is going to be evaluated in Java
  //
  // This function determines the effective evaluation type of N:
  // Gandiva in case 1, and
  // Java in case 2
  private EvaluationType getEffectiveNodeEvaluationType(EvaluationType parentEvalType, LogicalExpression expr) {
    EvaluationType evalType = expr.getEvaluationType();

    if (parentEvalType.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      // parent supports Gandiva
      // if expr supports Gandiva - no split
      // if expr supports Java - split at expr
      // TODO: Need to handle the case where Java is able to evaluate an expression, but cannot return data
      // associated with the function
      return evalType;
    }

    // parent supports Java
    if (!expr.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      // expression does not support Gandiva. No split at expr
      return evalType;
    }

    // parent supports Java
    // expression supports Gandiva. We might want to split and evaluate using Gandiva
    if (gandivaSupportsReturnType(expr)) {
      // expr can be split
      return evalType;
    }

    // expr cannot be tree-root in Gandiva
    evalType.removeEvaluationType(EvaluationType.ExecutionType.GANDIVA);
    return evalType;
  }

  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, EvaluationType parentEvalType) throws RuntimeException {
    EvaluationType evaluationType = getEffectiveNodeEvaluationType(parentEvalType, holder);
    List<LogicalExpression> newArgs = Lists.newArrayList();

    if (evaluationType.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      // function call is supported by Gandiva
      for(LogicalExpression arg : holder.args) {
        // Traverse down the tree to see if the expression changes. When there is a split
        // the expression changes as the operator is replaced by the newly created split
        LogicalExpression newArg = arg.accept(this, evaluationType);

        if (!candidateForSplit(arg) ||
            arg.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
          // Gandiva supports the function and this argument
          newArgs.add(newArg);
          continue;
        }

        // this arg is not supported by Gandiva
        // this is a split point
        ValueVectorReadExpression readExpression = splitAndGenerateVectorReadExpression(newArg);
        readExpression.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
        newArgs.add(readExpression);
      }

      // Create new function holder and annotate it for Gandiva execution
      FunctionHolderExpr result = new FunctionHolderExpr(holder.getName(), (BaseFunctionHolder)holder.getHolder(), newArgs);
      result.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      return result;
    }

    // the function cannot be executed in Gandiva
    // go over the arguments to see if any argument can be executed in Gandiva
    for(LogicalExpression arg: holder.args) {
      LogicalExpression newArg = arg.accept(this, evaluationType);

      if (!candidateForSplit(arg) ||
          !arg.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        // Gandiva cannot execute this argument
        // Lets evaluate this in Java
        newArgs.add(newArg);
        continue;
      }

      if (!gandivaSupportsReturnType(newArg)) {
        // Gandiva does not support the return type of the split
        newArgs.add(newArg);
        continue;
      }

      // Gandiva can execute this argument
      // this is a split point
      ValueVectorReadExpression readExpression = splitAndGenerateVectorReadExpression(newArg);
      readExpression.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      newArgs.add(readExpression);
    }

    // Create the new function holder
    FunctionHolderExpr result = new FunctionHolderExpr(holder.getName(), (BaseFunctionHolder)holder.getHolder(), newArgs);
    return result;
  }

  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpr, EvaluationType parentEvalType) throws RuntimeException {
    EvaluationType evaluationType = getEffectiveNodeEvaluationType(parentEvalType, ifExpr);
    LogicalExpression newCond = ifExpr.ifCondition.condition.accept(this, evaluationType);
    LogicalExpression thenExpr = ifExpr.ifCondition.expression.accept(this, evaluationType);
    LogicalExpression elseExpr = ifExpr.elseExpression.accept(this, evaluationType);

    // TODO: Try harder to avoid extra work
    // if (cond) then then-expr else else-expr
    // It is possible that the then-expr executes in Java and the else-expr executes in Gandiva
    // This will end up evaluating both the expressions even when the cond is false most of the time
    //
    // cond in Java; then-expr and else-expr in Gandiva (this is already how it is done)
    // 1. Evaluate cond in Java - output in xxx
    // 2. Evaluate if (xxx) then then-expr else else-expr in Gandiva
    //
    // cond and else-expr in Gandiva, then-expr in Java (this case can be improved)
    // 1. Evaluate cond in Gandiva - output in xxx
    // 2. Evaluate if (xxx) then then-expr else const in Java - output in yyy
    // 3. Evaluate if (xxx) then yyy else else-expr in Gandiva - final output
    if (evaluationType.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.ifCondition.condition) &&
          !ifExpr.ifCondition.condition.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        // the condition cannot be evaluated using Gandiva
        newCond = splitAndGenerateVectorReadExpression(newCond);
        newCond.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.ifCondition.expression) &&
          !ifExpr.ifCondition.expression.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        thenExpr = splitAndGenerateVectorReadExpression(thenExpr);
        thenExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.elseExpression) &&
          !ifExpr.elseExpression.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        elseExpr = splitAndGenerateVectorReadExpression(elseExpr);
        elseExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }
    } else {
      // Gandiva cannot evaluate the if-expression
      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.ifCondition.condition) &&
          gandivaSupportsReturnType(ifExpr.ifCondition.condition) &&
          ifExpr.ifCondition.condition.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        // the condition can be evaluated using Gandiva
        newCond = splitAndGenerateVectorReadExpression(newCond);
        newCond.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.ifCondition.expression) &&
          gandivaSupportsReturnType(ifExpr.ifCondition.expression) &&
          ifExpr.ifCondition.expression.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        thenExpr = splitAndGenerateVectorReadExpression(thenExpr);
        thenExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      if (!ExpressionSplitter.preserveShortCircuits &&
          candidateForSplit(ifExpr.elseExpression) &&
          gandivaSupportsReturnType(ifExpr.elseExpression) &&
          ifExpr.elseExpression.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        elseExpr = splitAndGenerateVectorReadExpression(elseExpr);
        elseExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }
    }

    IfExpression.IfCondition condition = new IfExpression.IfCondition(newCond, thenExpr);
    IfExpression result = IfExpression.newBuilder()
      .setIfCondition(condition)
      .setElse(elseExpr)
      .setOutputType(ifExpr.outputType)
      .build();
    if (evaluationType.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      result.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
    }
    return result;
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, EvaluationType parentEvalType) throws RuntimeException {
    EvaluationType evaluationType = getEffectiveNodeEvaluationType(parentEvalType, op);
    List<LogicalExpression> newArgs = Lists.newArrayList();

    if (evaluationType.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
      // Boolean expression is supported by Gandiva
      for(LogicalExpression arg : op.args) {
        // traverse down the tree to see if the expression changes
        LogicalExpression newArg = arg.accept(this, evaluationType);

        // TODO: Try harder to preserve short circuits while evaluating boolean operators
        if (ExpressionSplitter.preserveShortCircuits ||
            !candidateForSplit(arg) ||
            arg.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
          newArgs.add(newArg);
          continue;
        }

        // this arg is not supported by Gandiva
        // this is a split point
        ValueVectorReadExpression readExpression = splitAndGenerateVectorReadExpression(newArg);
        readExpression.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
        newArgs.add(readExpression);
      }

      BooleanOperator result = new BooleanOperator(op.getName(), newArgs);
      result.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      return result;
    }

    // the boolean expression cannot be executed in Gandiva
    // go over the arguments to see if any argument can be executed in Gandiva
    for(LogicalExpression arg: op.args) {
      LogicalExpression newArg = arg.accept(this, evaluationType);

      if (ExpressionSplitter.preserveShortCircuits ||
          !candidateForSplit(arg) ||
          !arg.isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA)) {
        // Gandiva cannot execute this argument
        newArgs.add(newArg);
        continue;
      }

      if (!gandivaSupportsReturnType(newArg)) {
        newArgs.add(newArg);
        continue;
      }

      // Gandiva can execute this argument
      // this is a split point
      ValueVectorReadExpression readExpression = splitAndGenerateVectorReadExpression(newArg);
      readExpression.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      newArgs.add(readExpression);
    }

    BooleanOperator result = new BooleanOperator(op.getName(), newArgs);
    return result;
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, EvaluationType parentEvalType) throws RuntimeException {
    // All other cases, return the expression as-is
    // TODO: Need to check how the splitter handles complex data types
    // Ideally like(a.b, "Barbie") should execute in Gandiva with a.b evaluated in Java to produce a ValueVector
    return e;
  }
}
