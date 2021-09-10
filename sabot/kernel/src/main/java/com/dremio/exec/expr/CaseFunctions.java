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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Wrapper class that extracts functions used by the {@code CaseExpressionSplitter} for splits.
 * <p>
 * Use a singleton here to avoid loading the function. This class is made immutable.
 * </p>
 */
public class CaseFunctions {
  private static final String EQUAL_FUNCTION_NAME = "equal";
  private static final String GT_FUNCTION_NAME = "greater_than";
  private static final String LT_FUNCTION_NAME = "less_than";

  private static volatile CaseFunctions INSTANCE = null;

  private final AbstractFunctionHolder eqFnHolder;
  private final AbstractFunctionHolder gtFnHolder;
  private final AbstractFunctionHolder ltFnHolder;

  private CaseFunctions(AbstractFunctionHolder eqFnHolder, AbstractFunctionHolder gtFnHolder,
                        AbstractFunctionHolder ltFnHolder) {
    this.eqFnHolder = eqFnHolder;
    this.gtFnHolder = gtFnHolder;
    this.ltFnHolder = ltFnHolder;
  }

  public static CaseFunctions getInstance() {
    return INSTANCE;
  }

  public FunctionHolderExpression getEqFnExpr(List<LogicalExpression> args) {
    return eqFnHolder.getExpr(EQUAL_FUNCTION_NAME, args);
  }

  public FunctionHolderExpression getGtFnExpr(List<LogicalExpression> args) {
    return gtFnHolder.getExpr(GT_FUNCTION_NAME, args);
  }

  public FunctionHolderExpression getLtFnExpr(List<LogicalExpression> args) {
    return ltFnHolder.getExpr(LT_FUNCTION_NAME, args);
  }

  public static boolean isCommonToAllEngines(String fnName) {
    return fnName.equalsIgnoreCase(EQUAL_FUNCTION_NAME) || fnName.equalsIgnoreCase(GT_FUNCTION_NAME) ||
      fnName.equalsIgnoreCase(LT_FUNCTION_NAME);
  }

  public static void loadInstance(OperatorContext context) {
    CaseFunctions lref = INSTANCE;
    if (lref == null) {
      synchronized (CaseFunctions.class) {
        lref = INSTANCE;
        if (lref == null) {
          final AbstractFunctionHolder eqFnHolder = lookupIntFunction(EQUAL_FUNCTION_NAME, context);
          final AbstractFunctionHolder gtFnHolder = lookupIntFunction(GT_FUNCTION_NAME, context);
          final AbstractFunctionHolder ltFnHolder = lookupIntFunction(LT_FUNCTION_NAME, context);
          if (eqFnHolder != null && gtFnHolder != null && ltFnHolder != null) {
            INSTANCE = lref = new CaseFunctions(eqFnHolder, gtFnHolder, ltFnHolder);
          } else {
            throw new RuntimeException("Unable to load/find basic functions from registry. Should never happen");
          }
        }
      }
    }
  }

  /**
   * Convert a case expression codegen context to an if expression codegen context.
   * <p>
   * Only done for nested case expressions inside "when" clauses.
   * </p>
   * @param expr original expression (e.g "when" expression)
   * @return expression that has all its case transformed to nested if/else.
   */
  public static CodeGenContext convertCaseToIf(CodeGenContext expr) {
    final CaseConditionConverter converter = new CaseConditionConverter();
    return converter.visitCodegenContext(expr);
  }

  private static boolean caseEmbedded(LogicalExpression child) {
    return child.accept(new CaseConditionChecker(), null);
  }

  /**
   * Convert cases in this sub expression to If. Used for "when" expressions before splitting.
   */
  private static class CaseConditionConverter extends AbstractExprVisitor<CodeGenContext, CodeGenContext,
    RuntimeException> {

    public CodeGenContext visitCodegenContext(CodeGenContext context) {
      if (!caseEmbedded(context.getChild())) {
        // short circuit this sub expression if no case
        return context;
      }
      return context.getChild().accept(this, context);
    }

    @Override
    public CodeGenContext visitCaseExpression(CaseExpression caseExpression, CodeGenContext context) {
      LogicalExpression elseExpression = visitCodegenContext((CodeGenContext) caseExpression.elseExpr);
      for (int i = caseExpression.caseConditions.size() - 1; i >= 0; i--) {
        final CaseExpression.CaseConditionNode node = caseExpression.caseConditions.get(i);
        final LogicalExpression whenExpr = visitCodegenContext((CodeGenContext) node.whenExpr);
        final LogicalExpression thenExpr = visitCodegenContext((CodeGenContext) node.thenExpr);
        elseExpression = CodeGenContext.buildOnSubExpression(IfExpression.newBuilder()
          .setElse(elseExpression)
          .setIfCondition(new IfExpression.IfCondition(whenExpr, thenExpr)).build());
      }
      return CodeGenContext.buildWithCurrentCodegen(context, elseExpression);
    }

    @Override
    public CodeGenContext visitBooleanOperator(BooleanOperator op, CodeGenContext context) {
      List<LogicalExpression> args = new ArrayList<>();
      for (int i = 0; i < op.args.size(); ++i) {
        args.add(visitCodegenContext((CodeGenContext) op.args.get(i)));
      }
      return CodeGenContext.buildWithCurrentCodegen(context, new BooleanOperator(op.getName(), args));
    }

    @Override
    public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression holderExpr, CodeGenContext context) {
      final List<LogicalExpression> args = new ArrayList<>();
      for (int i = 0; i < holderExpr.args.size(); i++) {
        args.add(visitCodegenContext((CodeGenContext) holderExpr.args.get(i)));
      }
      return CodeGenContext.buildWithCurrentCodegen(context, new FunctionHolderExpr(holderExpr.nameUsed,
        (BaseFunctionHolder) holderExpr.getHolder(), args));
    }

    @Override
    public CodeGenContext visitUnknown(LogicalExpression e, CodeGenContext context) {
      return context;
    }
  }

  /**
   * Check if case exists in the given expression subtree
   */
  private static class CaseConditionChecker extends AbstractExprVisitor<Boolean, Void, RuntimeException> {
    @Override
    public Boolean visitUnknown(LogicalExpression e, Void value) {
      if (e instanceof CaseExpression) {
        return true;
      }
      if (e instanceof FunctionHolderExpression || e instanceof FunctionCall || e instanceof IfExpression) {
        return checkChildren(e);
      }
      return false;
    }

    private boolean checkChildren(LogicalExpression exprToCheck) {
      boolean ret = false;
      for (LogicalExpression e : exprToCheck) {
        if (ret) {
          break;
        }
        ret = e.accept(this, null);
      }
      return ret;
    }
  }

  private static AbstractFunctionHolder lookupIntFunction(String fnName, OperatorContext context) {
    List<LogicalExpression> args = Arrays.asList(ValueExpressions.getInt(0), ValueExpressions.getInt(0));
    final FunctionCall call = new FunctionCall(fnName, args);
    final FunctionLookupContext lookupContext = context.getClassProducer().getFunctionLookupContext();
    return lookupContext.findExactFunction(call, true);
  }
}
