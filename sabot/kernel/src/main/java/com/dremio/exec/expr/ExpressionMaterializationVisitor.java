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
package com.dremio.exec.expr;

import static com.dremio.common.types.Types.isFixedWidthType;
import static com.dremio.exec.expr.fn.impl.DecimalFunctions.DECIMAL_CAST_NULL_ON_OVERFLOW;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.apache.arrow.vector.complex.FieldIdUtil2;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.CastExpressionWithOverflow;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.ListAggExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.Ordering;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.BooleanExpression;
import com.dremio.common.expression.ValueExpressions.DateExpression;
import com.dremio.common.expression.ValueExpressions.DecimalExpression;
import com.dremio.common.expression.ValueExpressions.DoubleExpression;
import com.dremio.common.expression.ValueExpressions.FloatExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.expression.ValueExpressions.IntervalDayExpression;
import com.dremio.common.expression.ValueExpressions.IntervalYearExpression;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.ValueExpressions.TimeExpression;
import com.dremio.common.expression.ValueExpressions.TimeStampExpression;
import com.dremio.common.expression.fn.CastFunctions;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.expression.visitors.ExpressionValidator;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.CoreDecimalUtility;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.ComplexWriterFunctionHolder;
import com.dremio.exec.expr.fn.ExceptionFunction;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.resolver.TypeCastRules;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class ExpressionMaterializationVisitor
    extends AbstractExprVisitor<LogicalExpression, FunctionLookupContext, RuntimeException> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionMaterializationVisitor.class);
  public static final boolean ALLOW_MIXED_DECIMALS = true;
  private ExpressionValidator validator = new ExpressionValidator();
  private ErrorCollector errorCollector;
  private Deque<ErrorCollector> errorCollectors = new ArrayDeque<>();
  private final BatchSchema schema;
  private final boolean allowComplexWriter;
  // Only some operators (eg. project and filter) have been modified to use gandiva for code
  // generation. The others do not have that functionality yet. This flag would indicate if the
  // calling context supports Gandiva code generation.
  private final boolean allowGandivaFunctions;

  public ExpressionMaterializationVisitor(BatchSchema schema, ErrorCollector errorCollector,
                                          boolean allowComplexWriter, boolean allowGandivaFunctions) {
    this.schema = schema;
    this.errorCollector = errorCollector;
    this.allowComplexWriter = allowComplexWriter;
    this.allowGandivaFunctions = allowGandivaFunctions;
  }

  private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
    newExpr.accept(validator, errorCollector);
    return newExpr;
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, FunctionLookupContext functionLookupContext)
      throws RuntimeException {
    return e;
  }

  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder,
      FunctionLookupContext functionLookupContext) throws RuntimeException {
    // a function holder is already materialized, no need to rematerialize.
    // generally this won't be used unless we materialize a partial tree and
    // rematerialize the whole tree.
    return holder;
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, FunctionLookupContext functionLookupContext) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (int i = 0; i < op.args.size(); ++i) {
      LogicalExpression newExpr = op.args.get(i).accept(this, functionLookupContext);
      assert newExpr != null : String.format("Materialization of %s return a null expression.", op.args.get(i));
      args.add(newExpr);
    }
    // replace with a new function call, since its argument could be changed.
    return new BooleanOperator(op.getName(), args);
  }

  @Override
  public LogicalExpression visitFunctionCall(FunctionCall call, FunctionLookupContext functionLookupContext) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (int i = 0; i < call.args.size(); ++i) {
      LogicalExpression newExpr = call.args.get(i).accept(this, functionLookupContext);
      assert newExpr != null : String.format("Materialization of %s returned a null expression.", call.args.get(i));
      args.add(newExpr);
    }

    // replace with a new function call, since its argument could be changed.
    call = new FunctionCall(call.getName(), args);

    FunctionHolderExpression expr = getExactFunctionIfExists(call, functionLookupContext);
    if (expr != null) {
      return expr;
    }

    AbstractFunctionHolder matchedFuncHolder = functionLookupContext.findFunctionWithCast(call,
      allowGandivaFunctions);

    if (matchedFuncHolder != null) {
      return getFunctionHolderExpr(call, matchedFuncHolder,
        functionLookupContext, errorCollector);
    }

    // as no primary function is found, search for a non-primary function.
    final AbstractFunctionHolder matchedNonFunctionHolder = functionLookupContext.findNonFunction(call);

    if (matchedNonFunctionHolder != null) {
      return getFunctionHolderExpr(call, matchedNonFunctionHolder, functionLookupContext, errorCollector);
    }

    if (hasUnionInput(call)) {
      return rewriteUnionFunction(call, functionLookupContext);
    }

    ExpressionTreeMaterializer.logFunctionResolutionError(errorCollector, call);
    return NullExpression.INSTANCE;
  }

  private FunctionHolderExpression getExactFunctionIfExists(FunctionCall call, FunctionLookupContext functionLookupContext) {
    AbstractFunctionHolder matchedFuncHolder = functionLookupContext.findExactFunction(call, allowGandivaFunctions);
    if (matchedFuncHolder instanceof ComplexWriterFunctionHolder && !allowComplexWriter) {
      errorCollector.addGeneralError("Only Project can use a function with complex output. The complex function output is %s.", call.getName());
    }
    if (matchedFuncHolder != null) {
      return getFunctionHolderExpr(call, matchedFuncHolder,
        functionLookupContext, errorCollector);
    }

    return null;
  }

  private FunctionHolderExpression getFunctionHolderExpr(FunctionCall call, AbstractFunctionHolder matchedFuncHolder, FunctionLookupContext functionLookupContext, ErrorCollector errorCollector) {
    // new arg lists, possible with implicit cast inserted.
    final List<LogicalExpression> argsWithCast = Lists.newArrayList();

    // Compare param type against arg type. Insert cast on top of arg, whenever
    // necessary.
    for (int i = 0; i < call.args.size(); ++i) {

      final LogicalExpression currentArg = call.args.get(i);

      final CompleteType argType = currentArg.getCompleteType();
      final CompleteType parmType = matchedFuncHolder.getParamType(i);

      if (currentArg.equals(NullExpression.INSTANCE) && parmType != CompleteType.LATE) {
        argsWithCast.add(new TypedNullConstant(parmType));
      } else if (
          // types are identical
          parmType.equals(argType)

          // a field reader accepts anything.
          || matchedFuncHolder.isFieldReader(i)

          // both sides are decimal (no need to match on scale/precision since param type's definition are meaningless.
          || (parmType.isDecimal() && currentArg.getCompleteType().isDecimal())
          ) {

        // no cast needed.
        argsWithCast.add(currentArg);

      } else {
        argsWithCast.add(ExpressionTreeMaterializer.addImplicitCastExact(currentArg, parmType, functionLookupContext, errorCollector, allowGandivaFunctions));
      }
    }

    return matchedFuncHolder.getExpr(call.getName(), argsWithCast);
  }

  private boolean hasUnionInput(FunctionCall call) {
    for (LogicalExpression arg : call.args) {
      if (arg.getCompleteType().isUnion()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Converts a function call with a Union type input into a case statement,
   * where each branch of the case corresponds to one of the subtypes of the
   * Union type. The function call is materialized in each of the branches, with
   * the union input cast to the specific type corresponding to the branch of
   * the case statement
   *
   * @param call
   * @param functionLookupContext
   * @return
   */
  private LogicalExpression rewriteUnionFunction(FunctionCall call, FunctionLookupContext functionLookupContext) {
    LogicalExpression[] args = new LogicalExpression[call.args.size()];
    call.args.toArray(args);

    for (int i = 0; i < args.length; i++) {
      LogicalExpression arg = call.args.get(i);
      final CompleteType argType = arg.getCompleteType();

      if (!argType.isUnion()) {
        continue;
      }

      List<CompleteType> subTypes = Lists.transform(argType.getChildren(), new Function<Field, CompleteType>(){

        @Override
        public CompleteType apply(Field field) {
          return CompleteType.fromField(field);
        }});

      Preconditions.checkState(subTypes.size() > 0, "Union type has no subtypes");

      Queue<IfCondition> ifConditions = Lists.newLinkedList();

      for (CompleteType subType : subTypes) {
        final MinorType minorType = subType.toMinorType();
        final LogicalExpression ifCondition = getIsTypeExpressionForType(minorType, arg.accept(new CloneVisitor(), null));
        args[i] = getUnionAssertFunctionForType(minorType, arg.accept(new CloneVisitor(), null));

        List<LogicalExpression> newArgs = Lists.newArrayList();
        for (LogicalExpression e : args) {
          newArgs.add(e.accept(new CloneVisitor(), null));
        }

        // When expanding the expression tree to handle the different subtypes,
        // we will not throw an exception if one
        // of the branches fails to find a function match, since it is possible
        // that code path will never occur in execution
        // So instead of failing to materialize, we generate code to throw the
        // exception during execution if that code
        // path is hit.

        errorCollectors.push(errorCollector);
        errorCollector = new ErrorCollectorImpl();

        LogicalExpression thenExpression = new FunctionCall(call.getName(), newArgs).accept(this, functionLookupContext);

        if (errorCollector.hasErrors()) {
          thenExpression = getExceptionFunction(errorCollector.toErrorString());
        }

        errorCollector = errorCollectors.pop();

        IfExpression.IfCondition condition = new IfCondition(ifCondition, thenExpression);
        ifConditions.add(condition);
      }

      LogicalExpression ifExpression = ifConditions.poll().expression;

      while (!ifConditions.isEmpty()) {
        ifExpression = IfExpression.newBuilder().setIfCondition(ifConditions.poll()).setElse(ifExpression).build();
      }

      args[i] = ifExpression;
      return ifExpression.accept(this, functionLookupContext);
    }
    throw new UnsupportedOperationException("Did not find any Union input types");
  }

  /**
   * Returns the function call whose purpose is to throw an Exception if that
   * code is hit during execution
   *
   * @param message
   *          the exception message
   * @return
   */
  private LogicalExpression getExceptionFunction(String message) {
    QuotedString msg = new QuotedString(message);
    List<LogicalExpression> args = Lists.newArrayList();
    args.add(msg);
    FunctionCall call = new FunctionCall(ExceptionFunction.EXCEPTION_FUNCTION_NAME, args);
    return call;
  }

  /**
   * Returns the function which asserts that the current subtype of a union type
   * is a specific type, and allows the materializer to bind to that specific
   * type when doing function resolution
   *
   * @param type
   * @param arg
   * @return
   */
  private LogicalExpression getUnionAssertFunctionForType(MinorType type, LogicalExpression arg) {
    if (type == MinorType.UNION) {
      return arg;
    }
    if (type == MinorType.LIST || type == MinorType.STRUCT) {
      return getExceptionFunction(String.format("Unable to convert given types. Operation unsupported for %s types", type));
    }
    // TODO(DX-13724): not all "assert_..." functions are available
    String castFuncName = String.format("assert_%s", type.toString());
    Collections.singletonList(arg);
    return new FunctionCall(castFuncName, Collections.singletonList(arg));
  }

  /**
   * Get the function that tests whether a union type is a specific type
   *
   * @param type
   * @param arg
   * @return
   */
  private LogicalExpression getIsTypeExpressionForType(MinorType type, LogicalExpression arg) {
    // TODO(DX-13724): not all "is_..." functions are available
    String isFuncName = String.format("is_%s", type.toString());
    List<LogicalExpression> args = Lists.newArrayList();
    args.add(arg);
    return new FunctionCall(isFuncName, args);
  }

  @Override
  public LogicalExpression visitCaseExpression(CaseExpression caseExpression, FunctionLookupContext functionLookupContext) throws RuntimeException {
    List<CaseExpression.CaseConditionNode> newConditions = new ArrayList<>();
    LogicalExpression newElseExpr = caseExpression.elseExpr.accept(this, functionLookupContext);
    CompleteType outputType = caseExpression.outputType;
    boolean newElseExprReWritten = false;

    for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
      LogicalExpression newWhen = conditionNode.whenExpr.accept(this, functionLookupContext);
      LogicalExpression newThen = conditionNode.thenExpr.accept(this, functionLookupContext);
      CaseExpression.CaseConditionNode condition = new CaseExpression.CaseConditionNode(newWhen, newThen);

      final CompleteType newelseType = newElseExpr.getCompleteType();
      final MinorType newelseMinor = newelseType.toMinorType();
      final CompleteType newthenType = newThen.getCompleteType();
      final MinorType newthenMinor = newthenType.toMinorType();

      // if the types aren't equal (and one of them isn't null), we need to unify them.
      if(!newthenType.equals(newelseType) && !newthenType.isNull() && !newelseType.isNull()){

        final MinorType leastRestrictive = TypeCastRules.getLeastRestrictiveType((Arrays.asList
          (newthenMinor, newelseMinor)));
        if (leastRestrictive != newthenMinor && leastRestrictive != newelseMinor && leastRestrictive !=
          null) {
          // Implicitly cast then and else to common type
          CompleteType toType = CompleteType.fromMinorType(leastRestrictive);
          condition = new CaseExpression.CaseConditionNode(newWhen, ExpressionTreeMaterializer
            .addImplicitCastExact(newThen, toType, functionLookupContext, errorCollector, allowGandivaFunctions));
          newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, toType, functionLookupContext, errorCollector, allowGandivaFunctions);
        }else if (leastRestrictive != newthenMinor) {
          // Implicitly cast the then expression
          condition = new CaseExpression.CaseConditionNode(newWhen, ExpressionTreeMaterializer
            .addImplicitCastExact(newThen, newElseExpr.getCompleteType(), functionLookupContext, errorCollector, allowGandivaFunctions));
        } else if (leastRestrictive != newelseMinor) {
          // Implicitly cast the else expression
          newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, newThen.getCompleteType(), functionLookupContext, errorCollector, allowGandivaFunctions);
        } else{
          // casting didn't work, now we need to merge the types.
          outputType = newthenType.merge(newelseType, ALLOW_MIXED_DECIMALS);
          condition = new CaseExpression.CaseConditionNode(newWhen, ExpressionTreeMaterializer
            .addImplicitCastExact(newElseExpr, outputType, functionLookupContext, errorCollector, allowGandivaFunctions));
          newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, outputType, functionLookupContext, errorCollector, allowGandivaFunctions);
        }
      }

      // Resolve NullExpression into TypedNullConstant by visiting all conditions
      // We need to do this because we want to give the correct MajorType to the
      // Null constant
      List<LogicalExpression> allExpressions = new ArrayList<>();
      allExpressions.add(newThen);
      if (!newElseExprReWritten) {
        allExpressions.add(newElseExpr);
      }

      boolean containsNullExpr = allExpressions.stream().anyMatch(input -> input instanceof NullExpression);

      if (containsNullExpr) {
        Optional<LogicalExpression> nonNullExpr = allExpressions.stream()
          .filter(input -> !input.getCompleteType().toMinorType().equals(MinorType.NULL))
          .findFirst();

        if (nonNullExpr.isPresent()) {
          CompleteType type = nonNullExpr.get().getCompleteType();
          condition = new CaseExpression.CaseConditionNode(newWhen, rewriteNullExpression(newThen, type));
          if (!newElseExprReWritten) {
            newElseExprReWritten = true;
            newElseExpr = rewriteNullExpression(newElseExpr, type);
          }
        }
      }

      newConditions.add(condition);
    }

    return validateNewExpr(
      CaseExpression.newBuilder().setCaseConditions(newConditions).setElseExpr(newElseExpr).setOutputType(outputType).build());
  }

  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpr, FunctionLookupContext functionLookupContext) {
    final IfExpression.IfCondition oldConditions = ifExpr.ifCondition;
    final LogicalExpression newCondition = oldConditions.condition.accept(this, functionLookupContext);
    final LogicalExpression newExpr = oldConditions.expression.accept(this, functionLookupContext);

    LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, functionLookupContext);
    IfExpression.IfCondition condition = new IfExpression.IfCondition(newCondition, newExpr);

    final CompleteType thenType = condition.expression.getCompleteType();
    final CompleteType elseType = newElseExpr.getCompleteType();
    final MinorType thenMinor = condition.expression.getCompleteType().toMinorType();
    final MinorType elseMinor = newElseExpr.getCompleteType().toMinorType();

    CompleteType outputType = ifExpr.outputType;

    // if the types aren't equal (and one of them isn't null), we need to unify them.
    if(!thenType.equals(elseType) && !thenType.isNull() && !elseType.isNull()){

      final MinorType leastRestrictive = TypeCastRules.getLeastRestrictiveType((Arrays.asList
        (thenMinor, elseMinor)));
      if (leastRestrictive != thenMinor && leastRestrictive != elseMinor && leastRestrictive !=
        null) {
        // Implicitly cast then and else to common type
        CompleteType toType = CompleteType.fromMinorType(leastRestrictive);
        condition = new IfExpression.IfCondition(newCondition, ExpressionTreeMaterializer
          .addImplicitCastExact(condition.expression, toType, functionLookupContext, errorCollector, allowGandivaFunctions));
        newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, toType, functionLookupContext, errorCollector, allowGandivaFunctions);
      }else if (leastRestrictive != thenMinor) {
        // Implicitly cast the then expression
        condition = new IfExpression.IfCondition(newCondition, ExpressionTreeMaterializer.addImplicitCastExact(condition.expression, newElseExpr.getCompleteType(), functionLookupContext, errorCollector, allowGandivaFunctions));
      } else if (leastRestrictive != elseMinor) {
        // Implicitly cast the else expression
        newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, condition.expression.getCompleteType(), functionLookupContext, errorCollector, allowGandivaFunctions);
      } else{
        // casting didn't work, now we need to merge the types.
        outputType = thenType.merge(elseType, ALLOW_MIXED_DECIMALS);
        condition = new IfExpression.IfCondition(newCondition, ExpressionTreeMaterializer.addImplicitCastExact(condition.expression, outputType, functionLookupContext, errorCollector, allowGandivaFunctions));
        newElseExpr = ExpressionTreeMaterializer.addImplicitCastExact(newElseExpr, outputType, functionLookupContext, errorCollector, allowGandivaFunctions);
      }
    }

    // Resolve NullExpression into TypedNullConstant by visiting all conditions
    // We need to do this because we want to give the correct MajorType to the
    // Null constant
    List<LogicalExpression> allExpressions = Lists.newArrayList();
    allExpressions.add(condition.expression);
    allExpressions.add(newElseExpr);

    boolean containsNullExpr = Iterables.any(allExpressions, new Predicate<LogicalExpression>() {
      @Override
      public boolean apply(LogicalExpression input) {
        return input instanceof NullExpression;
      }
    });

    if (containsNullExpr) {
      Optional<LogicalExpression> nonNullExpr = allExpressions.stream()
        .filter(input -> !input.getCompleteType().toMinorType().equals(MinorType.NULL))
        .findFirst();

      if (nonNullExpr.isPresent()) {
        CompleteType type = nonNullExpr.get().getCompleteType();
        condition = new IfExpression.IfCondition(condition.condition, rewriteNullExpression(condition.expression, type));
        newElseExpr = rewriteNullExpression(newElseExpr, type);
      }
    }

    LogicalExpression expr = validateNewExpr(
        IfExpression.newBuilder().setElse(newElseExpr).setIfCondition(condition).setOutputType(outputType).build());
    return expr;
  }

  private LogicalExpression rewriteNullExpression(LogicalExpression expr, CompleteType type) {
    if (expr instanceof NullExpression) {
      return new TypedNullConstant(type);
    } else {
      return expr;
    }
  }

  @Override
  public LogicalExpression visitSchemaPath(SchemaPath path, FunctionLookupContext functionLookupContext) {
    TypedFieldId tfId = FieldIdUtil2.getFieldId(schema, path);
    if (tfId == null) {
      throw UserException.validationError().message("Unable to find the referenced field: [%s].", path.getAsUnescapedPath()).build(logger);
    } else {
      ValueVectorReadExpression e = new ValueVectorReadExpression(tfId);
      return e;
    }
  }

  @Override
  public LogicalExpression visitIntConstant(IntExpression intExpr, FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitFloatConstant(FloatExpression fExpr, FunctionLookupContext functionLookupContext) {
    return fExpr;
  }

  @Override
  public LogicalExpression visitLongConstant(LongExpression intExpr, FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitDateConstant(DateExpression intExpr, FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitTimeConstant(TimeExpression intExpr, FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitTimeStampConstant(TimeStampExpression intExpr,
      FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitNullConstant(TypedNullConstant nullConstant,
      FunctionLookupContext functionLookupContext) throws RuntimeException {
    return nullConstant;
  }

  @Override
  public LogicalExpression visitIntervalYearConstant(IntervalYearExpression intExpr,
      FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitIntervalDayConstant(IntervalDayExpression intExpr,
      FunctionLookupContext functionLookupContext) {
    return intExpr;
  }

  @Override
  public LogicalExpression visitDecimalConstant(DecimalExpression decExpr,
      FunctionLookupContext functionLookupContext) {
    return decExpr;
  }

  @Override
  public LogicalExpression visitDoubleConstant(DoubleExpression dExpr, FunctionLookupContext functionLookupContext) {
    return dExpr;
  }

  @Override
  public LogicalExpression visitBooleanConstant(BooleanExpression e, FunctionLookupContext functionLookupContext) {
    return e;
  }

  @Override
  public LogicalExpression visitQuotedStringConstant(QuotedString e, FunctionLookupContext functionLookupContext) {
    return e;
  }

  @Override
  public LogicalExpression visitConvertExpression(ConvertExpression e, FunctionLookupContext functionLookupContext) {
    String convertFunctionName = e.getConvertFunction() + e.getEncodingType();

    List<LogicalExpression> newArgs = Lists.newArrayList();
    newArgs.add(e.getInput()); // input_expr

    FunctionCall fc = new FunctionCall(convertFunctionName, newArgs);
    return fc.accept(this, functionLookupContext);
  }

  @Override
  public LogicalExpression visitOrdering(Ordering e, FunctionLookupContext functionLookupContext) throws RuntimeException {
    LogicalExpression newInput = e.getField().accept(this, functionLookupContext);
    return new Ordering(newInput, e.getDirection(), e.getNullDirection());
  }

  @Override
  public LogicalExpression visitListAggExpression(ListAggExpression e, FunctionLookupContext functionLookupContext) throws RuntimeException {
    List<LogicalExpression> newArgs = new ArrayList<>();
    for (LogicalExpression ex : e.args) {
      newArgs.add(ex.accept(this, functionLookupContext));
    }

    List<Ordering> orderings = new ArrayList<>();
    for (Ordering ex : e.getOrderings()) {
      orderings.add((Ordering) ex.accept(this, functionLookupContext));
    }

    List<LogicalExpression> extraExpressions = new ArrayList<>();
    for (LogicalExpression ex : e.getExtraExpressions()) {
      extraExpressions.add(ex.accept(this, functionLookupContext));
    }

    return new ListAggExpression(e.getName(), newArgs, e.isDistinct(), orderings, extraExpressions);
  }

  @Override
  public LogicalExpression visitCastExpression(CastExpression e, FunctionLookupContext functionLookupContext) {

    final LogicalExpression input = e.getInput().accept(this, functionLookupContext);

    // if the cast is pointless, remove it.
    final MajorType targetType = e.retrieveMajorType();
    final MinorType inputMinorType = input.getCompleteType().toMinorType(); // Input type
    if (castEqual(input.getCompleteType(), targetType)) {
      return input; // don't do pointless cast.
    }

    if (inputMinorType == MinorType.NULL) {
      // if input is a NULL expression, remove cast expression and return a
      // TypedNullConstant directly.
      return new TypedNullConstant(e.getCompleteType());
    } else {
      // if the type is fully bound, convert to functioncall and materialze the
      // function.

      // Get the cast function name from the map
      String castFuncWithType = CastFunctions.getCastFunc(targetType.getMinorType());

      // this is coming from the hive coercion reader
      // switch to functions that throw error on overflow if we are converting string/decimal
      // to decimal.
      if (e instanceof CastExpressionWithOverflow && (input.getCompleteType().isDecimal() ||
              input.getCompleteType().isText())) {
        castFuncWithType = DECIMAL_CAST_NULL_ON_OVERFLOW;
      }

      List<LogicalExpression> newArgs = Lists.newArrayList();
      newArgs.add(input); // input_expr

      // VarLen type
      if (!isFixedWidthType(targetType)) {
        newArgs.add(new ValueExpressions.LongExpression(targetType.getWidth()));
      }
      if (CoreDecimalUtility.isDecimalType(targetType)) {
        newArgs.add(new ValueExpressions.LongExpression(targetType.getPrecision()));
        newArgs.add(new ValueExpressions.LongExpression(targetType.getScale()));
      }
      FunctionCall fc = new FunctionCall(castFuncWithType, newArgs);
      return fc.accept(this, functionLookupContext);
    }
  }

  private boolean castEqual(CompleteType from, MajorType to) {
    MinorType fromMinor = from.toMinorType();
    if(from.toMinorType() != to.getMinorType()){
      return false;
    }

    switch (fromMinor) {
    case FLOAT4:
    case FLOAT8:
    case INT:
    case BIGINT:
    case BIT:
    case TINYINT:
    case SMALLINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case TIME:
    case TIMESTAMP:
    case TIMESTAMPTZ:
    case DATE:
    case INTERVAL:
    case INTERVALDAY:
    case INTERVALYEAR:
    case NULL:
      // nothing else matters.
      return true;
    case DECIMAL:
    case DECIMAL9:
    case DECIMAL18:
    case DECIMAL28DENSE:
    case DECIMAL28SPARSE:
    case DECIMAL38DENSE:
    case DECIMAL38SPARSE:
      Decimal fromDecimal = from.getType(Decimal.class);
      return to.getScale() == fromDecimal.getScale() && to.getPrecision() == fromDecimal.getPrecision();
    case VARBINARY:
    case VARCHAR:
      return to.getWidth() == BasicTypeHelper.VARCHAR_DEFAULT_CAST_LEN || to.getWidth() == 0;

    default:
      errorCollector.addGeneralError("Casting rules are unknown for type %s.", from);
      return false;
    }
  }
}
