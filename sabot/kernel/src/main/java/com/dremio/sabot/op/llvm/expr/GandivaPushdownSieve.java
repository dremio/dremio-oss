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
package com.dremio.sabot.op.llvm.expr;

import java.util.List;
import java.util.Set;

import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.FloatExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.expr.CodeGenContext;
import com.dremio.exec.expr.ExpressionSplitHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.expr.fn.GandivaRegistryWrapper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Annotates the expression in case the expression can be evaluated in native code.
 */
public class GandivaPushdownSieve extends AbstractExprVisitor<CodeGenContext, CodeGenContext, GandivaException>
  implements ExpressionSplitHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GandivaPushdownSieve.class);

  private static final List<String> supportedBooleanOperators = Lists.newArrayList("booleanAnd",
    "booleanOr");

  private final boolean isDecimalV2Enabled;
  private Set<FunctionSignature> supportedFunctions = null;

  static final Set<FunctionSignature> functionsToHide = Sets.newHashSet();

  @VisibleForTesting
  static void addFunctionToHide(FunctionSignature signature) {
    functionsToHide.add(signature);
  }

  @VisibleForTesting
  static void removeFunctionToHide(FunctionSignature signature) {
    functionsToHide.remove(signature);
  }

  public GandivaPushdownSieve(boolean isDecimalV2Enabled) {
    this.isDecimalV2Enabled = isDecimalV2Enabled;
  }

  public CodeGenContext annotateExpression(BatchSchema batchSchema, CodeGenContext contextExpr) {
    try {
      BatchSchema.SelectionVectorMode incomingSelectionVectorMode = batchSchema.getSelectionVectorMode();

      if (incomingSelectionVectorMode != BatchSchema.SelectionVectorMode.NONE) {
        return contextExpr;
      }

      CodeGenContext codeGenContextModifiedExpr = contextExpr.getChild().accept(this, contextExpr);

      if (!isSupportedReturnType(contextExpr.getCompleteType())) {
        // If the final expression's return type is not supported, remove Gandiva support
        codeGenContextModifiedExpr.removeSupporteExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        codeGenContextModifiedExpr.removeSupporteExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      }

      return codeGenContextModifiedExpr;
    } catch (GandivaException gandivaException) {
      logger.error("Exception in running sieve", gandivaException);
      // ignore exception
      // fall-through and return false
    }
    return contextExpr;
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
  public CodeGenContext visitUnknown(LogicalExpression e, CodeGenContext context) throws GandivaException {

    if (e instanceof ValueVectorReadExpression) {
      if (isSupportedField((ValueVectorReadExpression) e)) {
        context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      }
    } else {
      logger.info("GandivaPushdownSieve : unknown expression type {}", e.getClass()
        .getCanonicalName());
    }

    return context;
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

  // Returns true if the entire sub-tree represented by every element in the list can be evaluated in Gandiva
  // First, annotate the arguments
  // Second, check that the entire sub-tree can be evaluated in Gandiva
  private boolean gandivaSupportsAllSubExprs(List<LogicalExpression> args) throws GandivaException {
    // first annotate all arguments
    for(LogicalExpression arg : args) {
      arg.accept(this, (CodeGenContext)arg);
    }

    for(int i = 0; i < args.size(); i++) {
      CodeGenContext codegenWrapper = (CodeGenContext)args.get(i);
      if (!codegenWrapper.isSubExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
        // this sub-expression cannot be evaluated entirely in Gandiva
        return false;
      }
    }

    return true;
  }

  // Check if all column reads involve data types supported by Gandiva
  private boolean gandivaSupportsAllValueVectorReads(List<LogicalExpression> exps) {
    for(LogicalExpression exp : exps) {
      CodeGenContext codeGenContext = (CodeGenContext) exp;
      if (codeGenContext.getChild() instanceof ValueVectorReadExpression) {
        if (!codeGenContext.isExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public CodeGenContext visitBooleanOperator(BooleanOperator operator, CodeGenContext context) throws GandivaException {
    boolean allArgTreesSupported = gandivaSupportsAllSubExprs(operator.args);

    // check that operator is supported.
    if (!supportedBooleanOperators.contains(operator.getName())) {
      return context;
    }

    // check return type;
    if (!isSupportedType(operator.getCompleteType())) {
      return context;
    }

    if (allArgTreesSupported) {
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
    }

    return context;
  }

  @Override
  public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression holder, CodeGenContext context) throws GandivaException {
    // will be removed once we integrate into dremio fn repository.
    if (holder.getHolder() == null) {
      // No need to walk down the tree to annotate
      return context;
    }

    if (!(holder.getHolder() instanceof BaseFunctionHolder) &&
        !(holder.getHolder() instanceof GandivaFunctionHolder)) {
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.JAVA);
    }

    boolean allArgTreesSupported = gandivaSupportsAllSubExprs(holder.args);

    // make sure we remove the support for a gandiva holder expression if sub expr cant be executed
    // in Gandiva.
    if (!allArgTreesSupported) {
      context.removeSupporteExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
    }

    // use context to strip context before looking for output type.
    if (!isSupportedType(context.getCompleteType())) {
      return context;
    }

    if (!isFunctionSupported(holder, context.getCompleteType())) {
      return context;
    }

    // The function can definitely be evaluated in Gandiva
    if (allArgTreesSupported) {
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      return context;
    }

    // check if gandiva supports all reads
    // If so, the function can be evaluated in Gandiva even though the entire sub-tree may not be evaluated in Gandiva
    if (gandivaSupportsAllValueVectorReads(holder.args)) {
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
    }

    return context;
  }

  private boolean isFunctionSupported(FunctionHolderExpression holder, CompleteType completeType) throws GandivaException {
    if (this.supportedFunctions == null) {
      GandivaRegistryWrapper instance = GandivaRegistryWrapper.getInstance();
      this.supportedFunctions = isDecimalV2Enabled ? instance
        .getSupportedFunctionsIncludingDecimal() : instance.getSupportedFunctionsExcludingDecimal();
      if (!functionsToHide.isEmpty()) {
        // test code adds/hides functions
        // need to make a copy so that we dont modify the original list of Gandiva Functions
        Set<FunctionSignature> supportedFnsClone = Sets.newHashSet();
        supportedFnsClone.addAll(this.supportedFunctions);
        supportedFnsClone.removeAll(functionsToHide);
        this.supportedFunctions = supportedFnsClone;
      }
    }

    String name = holder.getName();
    ArrowType returnType = completeType.getType();
    List<ArrowType> argTypes = Lists.newArrayList();
    convertArgsToArrowType(holder, argTypes);
    returnType = generifyDecimalType(returnType);
    FunctionSignature functionSignature = new FunctionSignature(name, returnType, argTypes);

    if (!supportedFunctions.contains(functionSignature) || !isSpecificFuntionSupported(holder)) {
      logger.debug("function signature not supported in gandiva : " + functionSignature);
      return false;
    }
    return true;
  }

  /*
   * Decimals can be of arbitrary precision and scale. Hence making
   * the type a constant since Gandiva supports arbitrary precision
   * and scale of range 0-38,0-38
   */
  private ArrowType generifyDecimalType(ArrowType returnType) {
    if (returnType.getTypeID() == ArrowType.ArrowTypeID.Decimal) {
      returnType = new ArrowType.Decimal(0,0);
    }
    return returnType;
  }

  private void convertArgsToArrowType(FunctionHolderExpression holder, List<ArrowType> argTypes) {
    for (LogicalExpression arg : holder.args) {
      ArrowType type = arg.getCompleteType().getType();
      type = generifyDecimalType(type);
      argTypes.add(type);
    }
  }

  /**
   * Checks for any overrides for specific functions
   * @param holder
   * @return true if function can be evaluated in Gandiva.
   */
  private boolean isSpecificFuntionSupported(FunctionHolderExpression holder) {
    // gandiva cannot yet process date patterns with timezones in it.
    if (holder.getName().equalsIgnoreCase("to_date") && holder.args.size() > 1) {
      CodeGenContext context = (CodeGenContext)holder.args.get(1);
      ValueExpressions.QuotedString pattern = (ValueExpressions.QuotedString)context.getChild();
      return !pattern.getString().contains("tz");
    }
    return true;
  }

  @Override
  public CodeGenContext visitIfExpression(IfExpression ifExpr, CodeGenContext context) throws GandivaException {
    List<LogicalExpression> codeGenTree = Lists.newArrayList(ifExpr.ifCondition.condition,
      ifExpr.ifCondition.expression, ifExpr.elseExpression);
    // check return type of the expression, use context to remove
    // context nodes before computing the return type.
    if (!isSupportedType(context.getCompleteType())) {
      return context;
    }

    boolean ifExprSupported = gandivaSupportsAllSubExprs(codeGenTree);

    if (ifExprSupported) {
      context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      return context;
    }

    if (((CodeGenContext) ifExpr.ifCondition.condition).isExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA) ||
      ((CodeGenContext) ifExpr.ifCondition.expression).isExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA) ||
      ((CodeGenContext) ifExpr.elseExpression).isExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
      if (gandivaSupportsAllValueVectorReads(codeGenTree)) {
        context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      }
    }

    return context;
  }

  private CodeGenContext annotateAndCheckSupportedType(LogicalExpression e, CodeGenContext
    context) throws GandivaException {
    if (!isSupportedType(e.getCompleteType())) {
      return context;
    }
    context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
    context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
    return context;
  }

  @Override
  public CodeGenContext visitQuotedStringConstant(QuotedString e, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(e, context);
  }

  @Override
  public CodeGenContext visitBooleanConstant(BooleanExpression booleanExpr, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(booleanExpr, context);
  }

  @Override
  public CodeGenContext visitIntConstant(IntExpression intExpr, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(intExpr, context);
  }

  @Override
  public CodeGenContext visitLongConstant(LongExpression longExpr, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(longExpr, context);
  }

  @Override
  public CodeGenContext visitFloatConstant(FloatExpression floatExpr, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(floatExpr, context);
  }

  @Override
  public CodeGenContext visitDoubleConstant(DoubleExpression doubleExpr, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(doubleExpr, context);
  }

  @Override
  public CodeGenContext visitNullConstant(TypedNullConstant nullConstant, CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(nullConstant, context);
  }

  @Override
  public CodeGenContext visitFunctionCall(FunctionCall call, CodeGenContext context) throws GandivaException {
    throw new GandivaException("Function calls should have been replaced before operator.");
  }

  @Override
  public CodeGenContext visitSchemaPath(SchemaPath path, CodeGenContext context) throws GandivaException {
    // to do : needed?
    return visitUnknown(path, context);
  }


  @Override
  public CodeGenContext visitDecimalConstant(ValueExpressions.DecimalExpression decExpr,
                                   CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(decExpr, context);
  }

  @Override
  public CodeGenContext visitDateConstant(ValueExpressions.DateExpression dateExpr, CodeGenContext context)
    throws GandivaException {
    return annotateAndCheckSupportedType(dateExpr, context);
  }

  @Override
  public CodeGenContext visitTimeConstant(ValueExpressions.TimeExpression timExpr,
                                CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(timExpr, context);
  }

  @Override
  public CodeGenContext visitTimeStampConstant(ValueExpressions.TimeStampExpression timeStampExpr,
                                     CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(timeStampExpr, context);
  }

  @Override
  public CodeGenContext visitIntervalYearConstant(ValueExpressions.IntervalYearExpression intervalYearExpr,
                                        CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(intervalYearExpr, context);
  }

  @Override
  public CodeGenContext visitIntervalDayConstant(ValueExpressions.IntervalDayExpression intervalDayExpr,
                                       CodeGenContext context) throws GandivaException {
    return annotateAndCheckSupportedType(intervalDayExpr, context);
  }

  @Override
  public CodeGenContext visitNullExpression(NullExpression e, CodeGenContext context) throws GandivaException {
    return visitUnknown(e, context);
  }

  private boolean isSupportedType(CompleteType type) throws GandivaException {
    final Set<ArrowType> supportedTypes = GandivaRegistryWrapper.getInstance().getSupportedTypes();
    ArrowType argType = type.getType();
    argType = generifyDecimalType(argType);
    return supportedTypes.contains(argType);
  }

  private boolean isSupportedReturnType(CompleteType type) throws GandivaException {
    return isSupportedType(type) && type.isScalar();
  }
}
