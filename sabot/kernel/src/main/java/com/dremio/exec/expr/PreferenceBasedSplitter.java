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

import com.dremio.common.collections.Tuple;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.compile.sig.ConstantExpressionIdentifier;
import com.google.common.collect.Lists;

/* Splits one expression into sub-expressions such that each sub-expression can be evaluated
 * by either Gandiva or Java. Each sub-expression has a unique name that is used to track
 * dependencies
 *
 * This class splits an expression based on the preferred code generator switching to preferred
 * generator when it can and retaining the non-preferred code generator when it is the only
 * choice for e.g. decimals can only be processed in Gandiva regardless of preference.
 */
public class PreferenceBasedSplitter extends AbstractExprVisitor<CodeGenContext,
  Tuple<CodeGenContext, SplitDependencyTracker>, Exception> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PreferenceBasedSplitter.class);

  final ExpressionSplitter splitter;
  final SupportedEngines.Engine preferredEngine;
  final SupportedEngines.Engine nonPreferredEngine;

  // constant expression for true. Used in boolean expressions
  final ValueExpressions.BooleanExpression trueExpr;
  // constant expression for false. Used in boolean expressions
  final ValueExpressions.BooleanExpression falseExpr;

  final CodeGenContext trueExprContext;
  final CodeGenContext falseExprContext;
  PreferenceBasedSplitter(ExpressionSplitter splitter, SupportedEngines.Engine
    preferredEngine, SupportedEngines.Engine nonPreferredEngine) {
    this.splitter = splitter;
    // true expression
    this.trueExpr = new ValueExpressions.BooleanExpression("true");
    this.trueExprContext = new CodeGenContext(trueExpr);
    trueExprContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
    trueExprContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);

    // false expression
    this.falseExpr = new ValueExpressions.BooleanExpression("false");
    this.falseExprContext = new CodeGenContext(falseExpr);
    falseExprContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
    falseExprContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);

    this.preferredEngine = preferredEngine;
    this.nonPreferredEngine = nonPreferredEngine;
  }

  CodeGenContext visitCodeGenContext(CodeGenContext context, SplitDependencyTracker
    myTracker) throws Exception{

    return context.getChild().accept(this,Tuple.<CodeGenContext, SplitDependencyTracker>of(context,
      myTracker));
  }

  @Override
  public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression holder ,
    Tuple<CodeGenContext, SplitDependencyTracker> value) throws Exception {
    CodeGenContext context = value.first;
    SplitDependencyTracker myTracker = value.second;
    if (holder.getHolder() == null) {
      return context;
    }

    if (context.isSubExpressionExecutableInEngine(this.preferredEngine)) {
      // entire expression can be done by the preferred codegen option
      logger.trace("Function evaluated in preferred {}", context);
      return context;
    }

    SupportedEngines parentEvalType = myTracker.getExecutionEngine();
    SupportedEngines executionEngine = splitter.getEffectiveNodeEvaluationType(parentEvalType, context);
    return splitFunctionExpression(holder, myTracker, executionEngine);
  }

  private CodeGenContext splitFunctionExpression(FunctionHolderExpression holder,
    SplitDependencyTracker myTracker, SupportedEngines executionEngine) throws Exception {
    List<LogicalExpression> newArgs = Lists.newArrayList();

    // figure out how to execute the function
    // executionEngine tells us all the execution types. We need to pick one now
    SupportedEngines.Engine fnExecType = this.preferredEngine;
    if (!executionEngine.contains(this.preferredEngine)) {
      fnExecType = this.nonPreferredEngine;
    }

    int i = 0;
    // initially assume we can run in a single code gen
    boolean canExecuteTreeInSingleCodeGen = true;

    logger.trace("Visiting function {}, fnExecType {}", holder, fnExecType);
    // iterate over the arguments and visit them
    for(LogicalExpression arg : holder.args) {
      // Traverse down the tree to see if the expression changes.
      // When there is a split, the expression changes as the operator is replaced by the newly created split
      SplitDependencyTracker argTracker = new SplitDependencyTracker(executionEngine, myTracker.getIfExprBranches());
      CodeGenContext newArg = visitCodeGenContext((CodeGenContext)arg, argTracker);
      boolean mustSplitAtArg = true;
      if (holder.argConstantOnly(i++)) {
        // the i-th argument is a constant
        // Cannot split as the i-th argument will be replaced by a ValueVectorRead on a split
        mustSplitAtArg = false;
      } else if (!splitter.candidateForSplit(newArg.getChild())) {
        // argument not a candidate for split
        mustSplitAtArg = false;
      }

      if (mustSplitAtArg && newArg.isExpressionExecutableInEngine(fnExecType)) {
        // function and arg can be evaluated by the function's execution type
        if (fnExecType == this.preferredEngine) {
          // if fnExecType is preferred, no need to split
          mustSplitAtArg = false;
        } else {
          // check if the arg can be executed by preferred executor
          if (!newArg.isExpressionExecutableInEngine(this.preferredEngine)) {
            // no need to split
            mustSplitAtArg = false;
          } else if (!splitter.canSplitAt(newArg, this.preferredEngine)) {
            // arg can execute at the preferred codegenerator, but it
            // cannot be split at this point
            mustSplitAtArg = false;
          } else if (ConstantExpressionIdentifier.isExpressionConstant(newArg.getChild())) {
            // there is no value in switching this to prefered as it is constant that is executable
            // in non-prefered
            mustSplitAtArg = false;
          }
        }
      }

      if (!newArg.isSubExpressionExecutableInEngine(fnExecType) || mustSplitAtArg) {
        canExecuteTreeInSingleCodeGen = false;
      }

      if (!mustSplitAtArg) {
        // cannot split here since the function expects a constant argument here
        myTracker.addAllDependencies(argTracker);
        newArgs.add(newArg);
        continue;
      }

      // this is a split point
      logger.trace("Splitting at arg {} of function {}, expression {}", i, holder.getName(), newArg);
      ExpressionSplit split = splitter.splitAndGenerateVectorReadExpression(newArg, myTracker, argTracker);
      CodeGenContext readExpressionContext = split.getReadExpressionContext();
      newArgs.add(readExpressionContext);
    }

    LogicalExpression result = holder.copy(newArgs);
    CodeGenContext outputTree = new CodeGenContext(result);
    outputTree.addSupportedExecutionEngineForExpression(fnExecType);
    if (canExecuteTreeInSingleCodeGen) {
      outputTree.addSupportedExecutionEngineForSubExpression(fnExecType);
    } else {
      outputTree.markSubExprIsMixed();
    }
    return outputTree;
  }

  boolean canExecuteInAllCodeGenerators(LogicalExpression expr) {
    // Including the true and false boolean expressions because these
    // can be evaluated by both Java and Gandiva
    // TODO: Should include TypedNullConstants that are introduced as part of if statements
    // There might be other TypedNullConstants that we haven't introduced
    return (expr instanceof CodeGenContext && ((CodeGenContext) expr).getChild() instanceof
      ValueExpressions.BooleanExpression) ||
           (expr instanceof ValueExpressions.BooleanExpression);
  }

  @Override
  public CodeGenContext visitIfExpression(IfExpression ifExpr,Tuple<CodeGenContext,
    SplitDependencyTracker> value) throws Exception {
    CodeGenContext context = value.first;
    SplitDependencyTracker myTracker = value.second;
    if (context.isSubExpressionExecutableInEngine(this.preferredEngine)) {
      // entire if can be done in preferred. No need to split
      logger.trace("Can evaluate if expression {} in preferred", context);
      return context;
    }

    logger.trace("Visiting if expr {}", ifExpr);
    SupportedEngines parentEvalType = myTracker.getExecutionEngine();
    SupportedEngines ifEvalType = splitter.getEffectiveNodeEvaluationType(parentEvalType, context);

    // Need to split either condExpr, thenExpr or elseExpr - dont know which one at this point of time
    // Consider cases where thenExpr or elseExpr contain if expressions. Examples:
    // 1) nested-if: if (c1) then (if (c2) then (t1) else (e1) end) else (if (c3) then (t2) else (e2) end) end
    // 2) arg-if: if (c1) then (t1 + (if (c2) then (t2) else (e2) end)) else (e2) end
    //
    // First, split the condition. Let this output be in c1
    // For splits in the then path, any split (s) has to be modified as if (c1) then (s) else (internal null)
    // For splits in the else path, any split (s) has to be modified as if (c1) then (internal null) else (s)
    //
    // The splits in either path (then or else) are done later. Pass the condition and context in the tracker
    // and use this later in (splitAndGenerateVectorReadExpression) to modify the split as above

    List<IfExprBranch> myPath = myTracker.getIfExprBranches();
    SplitDependencyTracker condTracker = new SplitDependencyTracker(ifEvalType, myPath);
    CodeGenContext condExprContext = visitCodeGenContext((CodeGenContext)ifExpr.ifCondition.condition, condTracker);

    // Split the condition and remember the ValueVectorReadExpression for the condition
    ExpressionSplit condSplit = splitter.splitAndGenerateVectorReadExpression(condExprContext, myTracker, condTracker);
    logger.trace("Split at condition {}", condSplit);
    CodeGenContext conditionValueVec = condSplit.getReadExpressionContext();

    // visit the then expression
    SplitDependencyTracker thenTracker = new SplitDependencyTracker(ifEvalType, myPath);
    thenTracker.addIfBranch(condSplit, true);
    logger.trace("Visiting then expr of if {}", ifExpr.ifCondition.expression);
    LogicalExpression thenExpr = visitCodeGenContext((CodeGenContext)ifExpr.ifCondition.expression, thenTracker);

    // visit the else expression
    SplitDependencyTracker elseTracker = new SplitDependencyTracker(ifEvalType, myPath);
    elseTracker.addIfBranch(condSplit, false);
    logger.trace("Visiting else expr of if {}", ifExpr.elseExpression);
    LogicalExpression elseExpr = visitCodeGenContext((CodeGenContext)ifExpr.elseExpression, elseTracker);

    if (!ifEvalType.contains(this.preferredEngine)) {
      // The if statement cannot be evaluated in the preferred type
      IfExpression newIfExpr = new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenExpr))
        .setElse(elseExpr)
        .setOutputType(ifExpr.getCompleteType())
        .build();

      // result is dependent on then and else expressions
      myTracker.addAllDependencies(thenTracker);
      myTracker.addAllDependencies(elseTracker);
      return new CodeGenContext(newIfExpr);
    }

    boolean condInPreferred = conditionValueVec.isExpressionExecutableInEngine(this.preferredEngine);
    boolean thenInPreferred = ((CodeGenContext)thenExpr).isExpressionExecutableInEngine(this.preferredEngine);
    boolean elseInPreferred = ((CodeGenContext)elseExpr).isExpressionExecutableInEngine(this.preferredEngine);

    if (((condInPreferred == thenInPreferred) && (condInPreferred == elseInPreferred)) ||
      (condInPreferred && (thenInPreferred || canExecuteInAllCodeGenerators(thenExpr)) && (elseInPreferred || canExecuteInAllCodeGenerators(elseExpr)))) {
      // either all in preferred codegenerator, or none in preferred codegenerator; or
      // condition in preferred and then and else can be executed in preferred
      LogicalExpression resultExpr = new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenExpr))
        .setElse(elseExpr)
        .setOutputType(ifExpr.getCompleteType())
        .build();

      CodeGenContext result = new CodeGenContext(resultExpr);
      if (condInPreferred) {
        // all in preferred codegenerator
        result.addSupportedExecutionEngineForExpression(this.preferredEngine);
        result.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
      }

      myTracker.addAllDependencies(thenTracker);
      myTracker.addAllDependencies(elseTracker);
      logger.trace("Evaluating then {} and else {} in one engine", thenExpr, elseExpr);
      return result;
    }

    // All expressions cannot be executed in same code generator. One of the below holds:
    // 1) condInPreferred = thenInPreferred, or
    // 2) condInPreferred = elseInPreferred, or
    // 3) thenInPreferred = elseInPreferred

    // splits in case 1
    // a) xxx = condExpr
    // b) yyy = if (xxx) then (thenExpr) else (nullExpr)
    // c) if (xxx) then (yyy) else (elseExpr)

    // splits in case 2
    // a) xxx = condExpr
    // b) yyy = if (xxx) then (nullExpr) else (elseExpr)
    // c) if (xxx) then (thenExpr) else (yyy)

    // splits in case 3
    // a) xxx = condExpr
    // b) if (xxx) then (thenExpr) else (elseExpr)

    IfExpression.Builder finalSplit = new IfExpression.Builder()
      .setOutputType(ifExpr.getCompleteType());

    TypedNullConstant nullExpression = new TypedNullConstant(ifExpr.getCompleteType());
    CodeGenContext nullExprContext = new CodeGenContext(nullExpression);
    // Can the last split be evaluated in preferred generator?
    boolean lastSplitInPreferredCodeGenerator = false;

    if ((thenInPreferred == elseInPreferred) || canExecuteInAllCodeGenerators(thenExpr) || canExecuteInAllCodeGenerators(elseExpr)) {
      // case 3
      // splits in case 3
      // a) xxx = condExpr - already done
      // b) if (xxx) then (thenExpr) else (elseExpr)

      // build the if-expression for the last split
      finalSplit.setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenExpr))
        .setElse(elseExpr);

      // last split involves both the then and else expressions
      lastSplitInPreferredCodeGenerator = ((CodeGenContext)elseExpr).isExpressionExecutableInEngine(preferredEngine) &&
        ((CodeGenContext)thenExpr).isExpressionExecutableInEngine(preferredEngine);
      // can execute in all code generators.
      myTracker.addAllDependencies(thenTracker);
      myTracker.addAllDependencies(elseTracker);
      logger.trace("Evaluating then {} and else {} in one engine", thenExpr, elseExpr);
    } else if (condInPreferred == thenInPreferred) {
      // case 1
      // splits in case 1
      // a) xxx = condExpr - already done
      // b) yyy = if (xxx) then (thenExpr) else (nullExpr)
      // c) if (xxx) then (yyy) else (elseExpr)

      // evalThen represents the 2nd split
      IfExpression evalThen = new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenExpr))
        .setElse(nullExprContext)
        .setOutputType(ifExpr.getCompleteType())
        .build();
      SplitDependencyTracker splitTracker = new SplitDependencyTracker(ifEvalType, myPath);
      // 2nd split depends ont he condition and everything that then expression depends on
      // add these dependencies
      splitTracker.addAllDependencies(thenTracker);
      splitTracker.addDependency(condSplit);

      CodeGenContext evalThenContextNode = CodeGenContext.buildWithNoDefaultSupport(evalThen);
      if (thenInPreferred && splitter.canSplitAt(thenExpr, this.preferredEngine)) {
        evalThenContextNode.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
        evalThenContextNode.addSupportedExecutionEngineForExpression(this.preferredEngine);
      } else {
        evalThenContextNode.addSupportedExecutionEngineForSubExpression(this.nonPreferredEngine);
        evalThenContextNode.addSupportedExecutionEngineForExpression(this.nonPreferredEngine);
      }

      logger.trace("Evaluating cond {} and then {} in one engine", condSplit, thenExpr);
      ExpressionSplit thenSplit = splitter.splitAndGenerateVectorReadExpression(evalThenContextNode, myTracker, splitTracker);
      CodeGenContext thenValueVec = thenSplit.getReadExpressionContext();

      // build the if-expression for the last split
      finalSplit.setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenValueVec))
        .setElse(elseExpr);
      // final split depends on everything that else expression depends on
      myTracker.addAllDependencies(elseTracker);

      // last split is the else expression
      lastSplitInPreferredCodeGenerator = ((CodeGenContext)elseExpr).isExpressionExecutableInEngine(preferredEngine);
    } else {
      // case 2
      // splits in case 2
      // a) xxx = condExpr -- already done
      // b) yyy = if (xxx) then (nullExpr) else (elseExpr)
      // c) if (xxx) then (thenExpr) else (yyy)

      // evalElse represents the 2nd split
      IfExpression evalElse = new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(conditionValueVec, nullExprContext))
        .setElse(elseExpr)
        .setOutputType(ifExpr.getCompleteType())
        .build();
      CodeGenContext evalElseContext = CodeGenContext.buildWithNoDefaultSupport(evalElse);

      SplitDependencyTracker splitTracker = new SplitDependencyTracker(ifEvalType, myPath);
      // 2nd split depends ont he condition and everything that else expression depends on
      // add these dependencies
      splitTracker.addAllDependencies(elseTracker);
      splitTracker.addDependency(condSplit);

      if (elseInPreferred && splitter.canSplitAt(elseExpr, this.preferredEngine)) {
        evalElseContext.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
        evalElseContext.addSupportedExecutionEngineForExpression(this.preferredEngine);
      } else {
        evalElseContext.addSupportedExecutionEngineForSubExpression(this.nonPreferredEngine);
        evalElseContext.addSupportedExecutionEngineForExpression(this.nonPreferredEngine);
      }

      logger.trace("Evaluating cond {} and else {} in one engine", condSplit, elseExpr);
      ExpressionSplit elseSplit = splitter.splitAndGenerateVectorReadExpression(evalElseContext, myTracker, splitTracker);
      CodeGenContext elseValueVec = elseSplit.getReadExpressionContext();

      // build the if-expression for the last split
      finalSplit.setIfCondition(new IfExpression.IfCondition(conditionValueVec, thenExpr))
        .setElse(elseValueVec);
      // final split depends on everything that then expression depends on
      myTracker.addAllDependencies(thenTracker);

      // last split is the then expression
      lastSplitInPreferredCodeGenerator = ((CodeGenContext)thenExpr).isExpressionExecutableInEngine(preferredEngine);
    }

    LogicalExpression newIfExpr = finalSplit.build();
    CodeGenContext newIfExprContext = CodeGenContext.buildWithNoDefaultSupport(newIfExpr);
    if (lastSplitInPreferredCodeGenerator) {
      newIfExprContext.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
      newIfExprContext.addSupportedExecutionEngineForExpression(this.preferredEngine);
    } else {
      newIfExprContext.addSupportedExecutionEngineForSubExpression(this.nonPreferredEngine);
      newIfExprContext.addSupportedExecutionEngineForExpression(this.nonPreferredEngine);
    }
    return newIfExprContext;
  }

  CodeGenContext booleanAndToIfExpr(LogicalExpression exprA, LogicalExpression exprB) {
    // A && B is the same as if (A) then (B) else (false)

    IfExpression ifExpr = IfExpression.newBuilder()
      .setIfCondition(new IfExpression.IfCondition(exprA, exprB))
      .setElse(falseExprContext)
      .setOutputType(exprA.getCompleteType())
      .build();
    CodeGenContext ifExprContext = new CodeGenContext(ifExpr);

    // the else expression (false) can be evaluated in Preferred Generator.
    ifExprContext.addSupportedExecutionEngineForExpression(preferredEngine);
    if (((CodeGenContext)exprA).isSubExpressionExecutableInEngine(this.preferredEngine) &&
      ((CodeGenContext)exprB).isSubExpressionExecutableInEngine(this.preferredEngine)) {
      ifExprContext.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
    } else {
      ifExprContext.markSubExprIsMixed();
    }

    return ifExprContext;
  }

  CodeGenContext booleanOrToIfExpr(LogicalExpression exprA, LogicalExpression exprB) {
    // A || B is the same as if (A) then (true) else (B)

    IfExpression ifExpr = IfExpression.newBuilder()
      .setIfCondition(new IfExpression.IfCondition(exprA, trueExprContext))
      .setElse(exprB)
      .setOutputType(exprA.getCompleteType())
      .build();

    CodeGenContext ifExprContext = new CodeGenContext(ifExpr);
    // the then expression (true) can be evaluated in Preferred Generator.
    ifExprContext.addSupportedExecutionEngineForExpression(preferredEngine);
    if (((CodeGenContext)exprA).isSubExpressionExecutableInEngine(this.preferredEngine) &&
      ((CodeGenContext)exprB).isSubExpressionExecutableInEngine(this.preferredEngine)) {
      ifExprContext.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
    } else {
      ifExprContext.markSubExprIsMixed();
    }

    return ifExprContext;
  }

  CodeGenContext handleAndExpr(BooleanOperator andExpr, SplitDependencyTracker myTracker) throws Exception {
    // TODO: Combine args executed by the same code generators to reduce the number of if-statements that get generated
    // For e.g. AND(e1, 22, e3, ...) can be grouped as
    // AND(E1, E2, ..) where
    // each E = AND(e1, e2, ...) and all arguments can be evaulated by the preferred code generator or not evaluated by the
    // preferred code generator
    // This reduces the number of if-exprs: and also reduces the number of splits
    int numArgs = andExpr.args.size();
    int curIndex = numArgs;

    CodeGenContext ifExpressionContext = booleanAndToIfExpr(andExpr.args.get(curIndex - 2), andExpr.args.get(curIndex - 1));
    curIndex -= 2;

    while (curIndex > 0) {
      ifExpressionContext = booleanAndToIfExpr(andExpr.args.get(curIndex - 1), ifExpressionContext);
      curIndex--;
    }

    return visitIfExpression((IfExpression)ifExpressionContext.getChild(),
      Tuple.of(ifExpressionContext, myTracker));
  }

  CodeGenContext handleOrExpr(BooleanOperator orExpr, SplitDependencyTracker myTracker) throws Exception {
    // TODO: Combine args to reduce the number of if-expressions. See handleAndExpr()
    int numArgs = orExpr.args.size();
    int curIndex = numArgs;

    CodeGenContext ifExpressionContext = booleanOrToIfExpr(orExpr.args.get(curIndex - 2), orExpr.args.get(curIndex - 1));
    curIndex -= 2;

    while (curIndex > 0) {
      ifExpressionContext = booleanOrToIfExpr(orExpr.args.get(curIndex - 1), ifExpressionContext);
      curIndex--;
    }

    return visitIfExpression((IfExpression)ifExpressionContext.getChild(),
      Tuple.of(ifExpressionContext, myTracker));
  }

  @Override
  public CodeGenContext visitBooleanOperator(BooleanOperator op, Tuple<CodeGenContext,
    SplitDependencyTracker> value) throws Exception {
    CodeGenContext context = value.first;
    SplitDependencyTracker myTracker = value.second;
    if (context.isSubExpressionExecutableInEngine(this.preferredEngine)) {
      return context;
    }

    if (op.isAnd()) {
      return handleAndExpr(op, myTracker);
    }

    if (op.isOr()) {
      return handleOrExpr(op, myTracker);
    }

    return context;
  }

  @Override
  public CodeGenContext visitUnknown(LogicalExpression expr, Tuple<CodeGenContext,
    SplitDependencyTracker> value) throws Exception {
    // All other cases, return the expression as-is
    // TODO: Need to check how the splitter handles complex data types
    // Ideally like(a.b, "Barbie") should execute in Gandiva with a.b evaluated in Java to produce a ValueVector
    return value.first;
  }

  @Override
  public CodeGenContext visitCaseExpression(CaseExpression caseExpression, Tuple<CodeGenContext, SplitDependencyTracker> value) throws Exception {
    // TODO: Implement splitting for case expressions
    CodeGenContext caseExpressionContext = CodeGenContext.buildWithNoDefaultSupport(caseExpression);
    caseExpressionContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.JAVA);
    caseExpressionContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.JAVA);
    return caseExpressionContext;
  }
}
