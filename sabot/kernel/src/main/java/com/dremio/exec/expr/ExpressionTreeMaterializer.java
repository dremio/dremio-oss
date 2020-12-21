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

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.fn.CastFunctions;
import com.dremio.common.expression.visitors.ConditionalExprOptimizer;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.types.RelDataTypeSystemImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.exec.resolver.FunctionResolverFactory;
import com.dremio.exec.util.DecimalUtils;
import com.dremio.sabot.op.llvm.expr.GandivaPushdownSieve;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ExpressionTreeMaterializer {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializer.class);
  public static final boolean DISALLOW_GANDIVA_FUNCTIONS = false;
  public static final boolean ALLOW_GANDIVA_FUNCTIONS = true;

  private ExpressionTreeMaterializer() {
  };

  public static LogicalExpression materialize(LogicalExpression expr, BatchSchema schema, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext) {
    return ExpressionTreeMaterializer.materialize(expr, schema, errorCollector, functionLookupContext, false);
  }

  public static LogicalExpression materializeAndCheckErrors(LogicalExpression expr, BatchSchema schema, FunctionLookupContext functionLookupContext) throws SchemaChangeException {
    return materializeAndCheckErrors(expr, schema, functionLookupContext, false);
  }

  public static LogicalExpression materializeAndCheckErrors(LogicalExpression expr, BatchSchema schema, FunctionLookupContext functionLookupContext, boolean allowComplex) throws SchemaChangeException {
    return materializeAndCheckErrors(expr, schema, functionLookupContext, allowComplex, false);
  }

  public static LogicalExpression materializeAndCheckErrors(LogicalExpression expr, BatchSchema
    schema, FunctionLookupContext functionLookupContext, boolean allowComplex, boolean
    allowGandivaFunctions) throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();
    LogicalExpression e = ExpressionTreeMaterializer.materialize(expr, schema, collector,
      functionLookupContext, allowComplex, allowGandivaFunctions);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("%s.", collector.toErrorString()));
    }
    return e;
  }

  public static Field materializeField(NamedExpression expr, BatchSchema incoming, FunctionLookupContext context){
    LogicalExpression out = materializeAndCheckErrors(expr.getExpr(), incoming, context);
    return out.getCompleteType().toField(expr.getRef());
  }

  public static SchemaBuilder materializeFields(List<NamedExpression> exprs, BatchSchema incoming, FunctionLookupContext context) {
    return materializeFields(exprs, incoming, context, false);
  }

  public static SchemaBuilder materializeFields(List<NamedExpression> exprs, BatchSchema
    incoming, FunctionLookupContext context, boolean allowComplex) {
    return materializeFields(exprs, incoming, context, allowComplex,false);
  }

  public static SchemaBuilder materializeFields(List<NamedExpression> exprs, BatchSchema
    incoming, FunctionLookupContext context, boolean allowComplex, boolean allowGandivaFunctions) {
    final SchemaBuilder builder = BatchSchema.newBuilder();
    for(NamedExpression e : exprs){
      CompleteType type = ExpressionTreeMaterializer.materializeAndCheckErrors(e.getExpr(),
        incoming, context, allowComplex, allowGandivaFunctions).getCompleteType();
      builder.addField(type.toField(e.getRef()));
    }
    return builder;
  }

  public static LogicalExpression materialize(LogicalExpression expr, BatchSchema schema, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext,
      boolean allowComplexWriterExpr, boolean allowGandivaFunctions) {
    LogicalExpression out =  expr.accept(new ExpressionMaterializationVisitor(schema,
      errorCollector, allowComplexWriterExpr, allowGandivaFunctions), functionLookupContext);

    if (!errorCollector.hasErrors()) {
      out = out.accept(ConditionalExprOptimizer.INSTANCE, null);
    }

    if (out instanceof NullExpression) {
      return new TypedNullConstant(CompleteType.INT);
    } else {
      return out;
    }
  }

  public static LogicalExpression materialize(LogicalExpression expr, BatchSchema schema, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext,
                                              boolean allowComplexWriterExpr) {
   return materialize(expr, schema, errorCollector, functionLookupContext,
     allowComplexWriterExpr, DISALLOW_GANDIVA_FUNCTIONS);
  }

  /**
   * ONLY for Projector and Filter to use for setting up code generation to follow.
   * Use API without the options parameter for all other cases.
   */
  public static LogicalExpression materialize(ExpressionEvaluationOptions options,
                                              LogicalExpression expr,
                                              BatchSchema schema,
                                              ErrorCollector errorCollector,
                                              FunctionLookupContext functionLookupContext,
                                              boolean allowComplexWriterExpr) {
    Preconditions.checkNotNull(options);
    LogicalExpression out = materialize(expr, schema, errorCollector, functionLookupContext,
      allowComplexWriterExpr, ALLOW_GANDIVA_FUNCTIONS);

    SupportedEngines.CodeGenOption codeGenOption = options.getCodeGenOption();

    CodeGenerationContextAnnotator contextAnnotator = new CodeGenerationContextAnnotator();
    // convert expression tree to a context tree first
    CodeGenContext contextTree = out.accept(contextAnnotator, null);

    if(contextAnnotator.isExpHasComplexField() == true) {
      codeGenOption = SupportedEngines.CodeGenOption.Java;
    }

    switch (codeGenOption) {
      case Gandiva:
      case GandivaOnly:
        return annotateGandivaExecution(codeGenOption, schema, contextTree, functionLookupContext.isDecimalV2Enabled());
      case Java:
      default:
        return contextTree;
    }
  }

  private static LogicalExpression annotateGandivaExecution(SupportedEngines.CodeGenOption codeGenOption,
                                                            BatchSchema schema,
                                                            CodeGenContext expr,
                                                            boolean isDecimalV2Enabled) {
    GandivaPushdownSieve gandivaPushdownSieve = new GandivaPushdownSieve(isDecimalV2Enabled);
    LogicalExpression modifiedExpression = gandivaPushdownSieve.annotateExpression(schema, expr);

    // Return the expression always
    // The splitter will handle the error in case of the expression cannot be handled in Gandiva and the
    // option is GandivaOnly
    return modifiedExpression;
  }

  public static LogicalExpression convertToNullableType(LogicalExpression fromExpr, MinorType toType, FunctionLookupContext functionLookupContext, ErrorCollector errorCollector) {
    String funcName = "convertToNullable" + toType.toString();
    List<LogicalExpression> args = Lists.newArrayList();
    args.add(fromExpr);
    FunctionCall funcCall = new FunctionCall(funcName, args);
    FunctionResolver resolver = FunctionResolverFactory.getResolver(funcCall);

    AbstractFunctionHolder matchedConvertToNullableFuncHolder = functionLookupContext
      .findExactFunction(funcCall, DISALLOW_GANDIVA_FUNCTIONS);
    if (matchedConvertToNullableFuncHolder == null) {
      logFunctionResolutionError(errorCollector, funcCall);
      return NullExpression.INSTANCE;
    }

    return matchedConvertToNullableFuncHolder.getExpr(funcName, args);
  }

  public static LogicalExpression addImplicitCastExact(
    LogicalExpression input,
    CompleteType toType,
    FunctionLookupContext functionLookupContext,
    ErrorCollector errorCollector, boolean allowGandivaFunctions) {
    return addImplicitCast(input, toType, functionLookupContext, errorCollector, true, allowGandivaFunctions);
  }

  public static LogicalExpression addImplicitCastApproximate(
    LogicalExpression input,
    CompleteType toType,
    FunctionLookupContext functionLookupContext,
    ErrorCollector errorCollector, boolean allowGandivaFunctions) {
    return addImplicitCast(input, toType, functionLookupContext, errorCollector, false, allowGandivaFunctions);
  }

  private static LogicalExpression addImplicitCast(
    LogicalExpression input,
    CompleteType toType,
    FunctionLookupContext functionLookupContext,
    ErrorCollector errorCollector,
    boolean exactResolver, boolean allowGandivaFunctions) {
    String castFuncName = CastFunctions.getCastFunc(toType.toMinorType());
    List<LogicalExpression> castArgs = Lists.newArrayList();
    castArgs.add(input);  //input_expr

    CompleteType fromType = input.getCompleteType();
    if(fromType.isUnion() && toType.isUnion()) {
      return input;
    }

    // add extra parameters as necessary.
    if (toType.isVariableWidthScalar()) {
      /* We are implicitly casting to VARCHAR so we don't have a max length. */
      castArgs.add(new ValueExpressions.LongExpression(RelDataTypeSystemImpl.DEFAULT_PRECISION));

    } else if (toType.isDecimal()) {
        Decimal decimal = toType.getType(Decimal.class);
        // Add the scale and precision to the arguments of the implicit cast
        // If it is long/int add the precision and scale for max values.
        if (decimal.getPrecision() == 0) {
          // determine precision
          if (input.getCompleteType().equals(CompleteType.INT)) {
            if (input instanceof ValueExpressions.IntExpression) {
              // if it's a constant, we know the exact value.
              int value = ((ValueExpressions.IntExpression) input).getInt();
              int precision = DecimalUtils.getPrecisionForValue(value);
              castArgs.add(new ValueExpressions.LongExpression(precision));
            } else {
              castArgs.add(new ValueExpressions.LongExpression(10));
            }
            castArgs.add(new ValueExpressions.LongExpression(0));
          }
          else if (input.getCompleteType().equals(CompleteType.BIGINT)) {
            if (input instanceof ValueExpressions.LongExpression) {
              // if it's a constant, we know the exact value.
              long value = ((ValueExpressions.LongExpression) input)
                .getLong();
              int precision = DecimalUtils.getPrecisionForValue(value);;
              castArgs.add(new ValueExpressions.LongExpression(precision));
            } else {
              castArgs.add(new ValueExpressions.LongExpression(19));
            }
          castArgs.add(new ValueExpressions.LongExpression(0));
          } else {
           throw new UnsupportedOperationException("Unsupported primitive type with precision 0.");
          }
        } else {
          castArgs.add(new ValueExpressions.LongExpression(decimal.getPrecision()));
          castArgs.add(new ValueExpressions.LongExpression(decimal.getScale()));
        }
    }
    // done adding extra parameters.

    final FunctionCall castCall = new FunctionCall(castFuncName, castArgs);
    final FunctionResolver resolver = exactResolver ? FunctionResolverFactory.getExactResolver(castCall) : FunctionResolverFactory.getResolver(castCall);
    final AbstractFunctionHolder matchedCastFuncHolder = functionLookupContext.findExactFunction
      (castCall, allowGandivaFunctions);

    if (matchedCastFuncHolder == null) {
      logFunctionResolutionError(errorCollector, castCall);
      return NullExpression.INSTANCE;
    }

    return matchedCastFuncHolder.getExpr(castFuncName, castArgs);
  }

  static void logFunctionResolutionError(ErrorCollector errorCollector, FunctionCall call) {
    // if function is a cast* function, handle it in
    // logCastFunctionResolutionError.
    if (call.getName().toLowerCase().startsWith("cast")) {
      logCastFunctionResolutionError(errorCollector, call);
    }
    // add error to collector
    else {
      StringBuilder sb = new StringBuilder();
      sb.append("Failure finding function: ");
      sb.append(call.getName());
      sb.append("(");
      boolean first = true;
      for (LogicalExpression e : call.args) {
        CompleteType type = e.getCompleteType();
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(Describer.describe(type));
      }
      sb.append(")");

      errorCollector.addGeneralError(sb.toString());
    }
  }

  /**
   * DX-21665, this function is created to print out helpful error messages when
   * CAST is not supported from one data type to another, For example, LIST to VARCHAR.
   * @param errorCollector collect error message and return
   * @param call function resolution call, expect a cast Function call.
   */
  static void logCastFunctionResolutionError(ErrorCollector errorCollector, FunctionCall call){
    // add error to collector
    StringBuilder sb = new StringBuilder();
    String targetType = call.getName();
    sb.append("Dremio does not support casting or coercing ");
    LogicalExpression targetTypeExpression = call.args.get(0);
    sb.append(Describer.describe(targetTypeExpression.getCompleteType()).toLowerCase());
    sb.append(" to ");
    sb.append(targetType.replace("cast", "").toLowerCase());
    errorCollector.addGeneralError(sb.toString());
  }
}
