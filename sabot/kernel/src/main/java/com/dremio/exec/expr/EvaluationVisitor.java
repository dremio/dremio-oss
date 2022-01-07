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

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.arrow.vector.ValueHolderHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.InExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.NullExpression;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.TypedNullConstant;
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
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.compile.sig.ConstantExpressionIdentifier;
import com.dremio.exec.compile.sig.ConstantExtractor;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator.BlockType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JAssignmentTarget;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JSwitch;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

/**
 * Visitor that generates code for eval
 */
public class EvaluationVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EvaluationVisitor.class);
  private final FunctionContext functionContext;
  private final int newMethodThreshold;
  private final int constantArrayThreshold;

  public EvaluationVisitor(FunctionContext functionContext) {
    super();
    this.functionContext = functionContext;
    this.newMethodThreshold = functionContext.getCompilationOptions().getNewMethodThreshold();
    this.constantArrayThreshold = functionContext.getCompilationOptions().getConstantArrayThreshold();
  }

  public int getFunctionErrorContextsCount() {
    return functionContext.getFunctionErrorContextSize();
  }

  public HoldingContainer addExpr(LogicalExpression e, ClassGenerator<?> generator, boolean allowInnerMethods) {
    Set<LogicalExpression> constantBoundaries;
    if (generator.getMappingSet().hasEmbeddedConstant()) {
      constantBoundaries = Collections.emptySet();
    } else {
      final ConstantExtractor extractor = ConstantExpressionIdentifier.getConstantExtractor(e);
      constantBoundaries = extractor.getConstantExpressionIdentitySet();
      generator.setConstantExtractor(extractor, constantArrayThreshold);
    }
    return e.accept(new ConstantFilter(constantBoundaries, functionContext, allowInnerMethods), generator);
  }

  private class ExpressionHolder {
    private LogicalExpression expression;
    private GeneratorMapping mapping;
    private MappingSet mappingSet;

    ExpressionHolder(LogicalExpression expression, MappingSet mappingSet) {
      this.expression = expression;
      this.mapping = mappingSet.getCurrentMapping();
      this.mappingSet = mappingSet;
    }

    @Override
    public int hashCode() {
      return expression.accept(new HashVisitor(), null);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ExpressionHolder)) {
        return false;
      }
      ExpressionHolder that = (ExpressionHolder) obj;
      return this.mappingSet == that.mappingSet && this.mapping == that.mapping && expression.accept(new EqualityVisitor(), that.expression);
    }
  }

  private void setIsSet(HoldingContainer out, ClassGenerator<?> generator) {
    generator.getEvalBlock().assign(out.getIsSet(), JExpr.lit(1));
  }

  // map tracking expressions in scope (includes all previous scope and current scope)
  Map<ExpressionHolder, HoldingContainer> previousExpressions = new HashMap<>();
  // list of expressions at each scope in a stack. Top of stack has expressions in current scope
  Deque<List<ExpressionHolder>> mapKeys = new ArrayDeque<>();

  void newScope() {
    mapKeys.push(new ArrayList<>());
  }

  void leaveScope() {
    List<ExpressionHolder> currentExpressionsToPop = mapKeys.pop();
    currentExpressionsToPop.forEach(previousExpressions::remove);
  }

  /**
   * Get a HoldingContainer for the expression if it had been already evaluated
   */
  private HoldingContainer getPrevious(LogicalExpression expression, MappingSet mappingSet) {
    HoldingContainer previous = previousExpressions.get(new ExpressionHolder(expression, mappingSet));
    if (previous != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Found previously evaluated expression: {}", ExpressionStringBuilder.toString(expression));
      }
    }
    return previous;
  }

  private void put(LogicalExpression expression, HoldingContainer hc, MappingSet mappingSet) {
    final ExpressionHolder eh = new ExpressionHolder(expression, mappingSet);
    previousExpressions.put(eh, hc);
    if (mapKeys.isEmpty()) {
      mapKeys.push(new ArrayList<>());
    }
    mapKeys.peek().add(eh);
  }

  /**
   * Clear the stack of expressions when we move to another method
   */
  public void clearPreviousExpressions() {
    previousExpressions.clear();
  }

  private class EvalVisitor extends AbstractExprVisitor<HoldingContainer, ClassGenerator<?>, RuntimeException> {
    private final FunctionContext functionContext;

    EvalVisitor(FunctionContext functionContext) {
      super();
      this.functionContext = functionContext;
    }

    FunctionContext getFunctionContext() {
      return functionContext;
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall call, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "
          + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator op,
                                                 ClassGenerator<?> generator) throws RuntimeException {
      if (op.getName().equals("booleanAnd")) {
        return visitBooleanAnd(op, generator);
      } else if (op.getName().equals("booleanOr")) {
        return visitBooleanOr(op, generator);
      } else {
        throw new UnsupportedOperationException("BooleanOperator can only be booleanAnd, booleanOr. You are using " + op.getName());
      }
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression holderExpr,
                                                          ClassGenerator<?> generator) throws RuntimeException {
      CompleteType resolvedOutput = holderExpr.getCompleteType();
      AbstractFunctionHolder holder = (AbstractFunctionHolder) holderExpr.getHolder();

      FunctionErrorContext errorContext = null;
      if (holder.usesErrContext()) {
        errorContext = FunctionErrorContextBuilder.builder()
          .build();
        functionContext.registerFunctionErrorContext(errorContext);
      }
      JVar[] workspaceVars = holder.renderStart(generator, resolvedOutput, null, errorContext);

      if (holder.isNested()) {
        generator.getMappingSet().enterChild();
      }

      HoldingContainer[] args = new HoldingContainer[holderExpr.args.size()];
      for (int i = 0; i < holderExpr.args.size(); i++) {
        args[i] = holderExpr.args.get(i).accept(this, generator);
      }

      holder.renderMiddle(generator, resolvedOutput, args, workspaceVars);

      if (holder.isNested()) {
        generator.getMappingSet().exitChild();
      }

      return holder.renderEnd(generator, resolvedOutput, args, workspaceVars);
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, ClassGenerator<?> generator) throws RuntimeException {
      JBlock local = generator.getEvalBlock();

      JBlock conditionalBlock = new JBlock(false, false);
      IfCondition c = ifExpr.ifCondition;

      HoldingContainer holdingContainer = c.condition.accept(this, generator);

      if (ifExpr.getCompleteType().isComplex() || ifExpr.getCompleteType().isUnion()) {
        JVar complexOutput = generator.declareClassField("inputReader", generator.getModel()._ref(FieldReader.class));

        final JConditional jc = conditionalBlock._if(holdingContainer.getIsSet().eq(JExpr.lit(1))
          .cand(holdingContainer.getValue().eq(JExpr.lit(1))));
        generator.nestEvalBlock(jc._then());
        HoldingContainer thenExpr = c.expression.accept(this, generator);
        generator.unNestEvalBlock();

        if (thenExpr.isConstant()) {
          JClass nrClass = generator.getModel().ref(org.apache.arrow.vector.complex.impl.NullReader.class);
          JExpression nullReader = nrClass.staticRef("INSTANCE");
          jc._then().assign(complexOutput, nullReader);
        } else {
          final CompleteType fieldType = thenExpr.getCompleteType();
          final JExpression fieldReader = fieldType.isUnion()
            ? thenExpr.getHolder().ref("reader") : thenExpr.getHolder();
          jc._then().assign(complexOutput, fieldReader);
        }

        generator.nestEvalBlock(jc._else());
        HoldingContainer elseExpr = ifExpr.elseExpression.accept(this, generator);
        generator.unNestEvalBlock();

        if (elseExpr.isConstant()) {
          JClass nrClass = generator.getModel().ref(org.apache.arrow.vector.complex.impl.NullReader.class);
          JExpression nullReader = nrClass.staticRef("INSTANCE");
          jc._else().assign(complexOutput, nullReader);
        } else {
          final CompleteType fieldType = elseExpr.getCompleteType();
          final JExpression fieldReader = fieldType.isUnion()
            ? elseExpr.getHolder().ref("reader") : elseExpr.getHolder();
          jc._else().assign(complexOutput, fieldReader);
        }

        HoldingContainer out = new HoldingContainer(ifExpr.getCompleteType(), complexOutput, null, null, false, true);
        local.add(conditionalBlock);
        return out;
      }

      HoldingContainer output = generator.declare(ifExpr.getCompleteType());

      final JConditional jc = conditionalBlock._if(holdingContainer.getIsSet().eq(JExpr.lit(1))
        .cand(holdingContainer.getValue().eq(JExpr.lit(1))));

      generator.nestEvalBlock(jc._then());

      HoldingContainer thenExpr = c.expression.accept(this, generator);

      generator.unNestEvalBlock();

      JConditional newCond = jc._then()._if(thenExpr.getIsSet().ne(JExpr.lit(0)));
      JBlock b = newCond._then();
      b.assign(output.getHolder(), thenExpr.getHolder());

      generator.nestEvalBlock(jc._else());

      HoldingContainer elseExpr = ifExpr.elseExpression.accept(this, generator);

      generator.unNestEvalBlock();

      JConditional newCond2 = jc._else()._if(elseExpr.getIsSet().ne(JExpr.lit(0)));
      JBlock b2 = newCond2._then();
      b2.assign(output.getHolder(), elseExpr.getHolder());
      //b.assign(output.getIsSet(), elseExpr.getIsSet());
      local.add(conditionalBlock);
      return output;
    }

    @Override
    public HoldingContainer visitCaseExpression(CaseExpression caseExpression, ClassGenerator<?> generator)
      throws RuntimeException {
      BiConsumer<HoldingContainer, JBlock> assigner;
      Supplier<HoldingContainer> outputSupplier;
      final CompleteType outputType = caseExpression.getCompleteType();
      if (outputType.isComplex() || outputType.isUnion()) {
        JVar complexOutput = generator.declareClassField("inputReader",
          generator.getModel()._ref(FieldReader.class));
        assigner = (expr, block) -> {
          if (expr.isConstant()) {
            JClass nrClass = generator.getModel().ref(org.apache.arrow.vector.complex.impl.NullReader.class);
            JExpression nullReader = nrClass.staticRef("INSTANCE");
            block.assign(complexOutput, nullReader);
          } else {
            final CompleteType fieldType = expr.getCompleteType();
            final JExpression fieldReader = fieldType.isUnion()
              ? expr.getHolder().ref("reader") : expr.getHolder();
            block.assign(complexOutput, fieldReader);
          }
        };
        outputSupplier = () -> new HoldingContainer(outputType, complexOutput, null, null, false, true);
      } else {
        final HoldingContainer output = generator.declare(outputType);
        assigner = (expr, block) -> {
          final JBlock ifBLock = block._if(expr.getIsSet().ne(JExpr.lit(0)))._then();
          ifBLock.assign(output.getHolder(), expr.getHolder());
        };
        outputSupplier = () -> output;
      }
      JLabel caseLabel = generator.getEvalBlockLabel("caseBlock");
      JBlock eval = generator.createInnerEvalBlock();
      generator.nestEvalBlock(eval);
      final HoldingContainer doSwitch = checkIsSwitch(caseExpression, generator);
      if (doSwitch != null) {
        final JBlock evalBlock = generator.getEvalBlock();
        final JBlock conditionalBlock = new JBlock(false, false);
        final JConditional jc = conditionalBlock._if(doSwitch.getIsSet().eq(JExpr.lit(1)));
        JExpression switchTest = doSwitch.getCompleteType().toMinorType().equals(TypeProtos.MinorType.VARCHAR) ?
          generator.getModel().ref(StringFunctionHelpers.class).staticInvoke("getStringFromNullableVarCharHolder")
            .arg(doSwitch.getHolder()) : doSwitch.getValue();
        final JSwitch switchBlock = jc._then()._switch(switchTest);
        {
          final Set<Object> literalsSoFar = new HashSet<>();
          for (final CaseExpression.CaseConditionNode node : caseExpression.caseConditions) {
            JExpression caseExpr = extractLiteral(node.whenExpr, literalsSoFar);
            if (caseExpr == null) {
              // must be a duplicate
              continue;
            }
            JBlock caseBody = switchBlock._case(caseExpr).body();
            generator.nestEvalBlock(caseBody);
            HoldingContainer thenExpr = node.thenExpr.accept(this, generator);
            assigner.accept(thenExpr, caseBody);
            generator.unNestEvalBlock();
            caseBody._break(caseLabel);
          }
        }
        JBlock defaultBody = switchBlock._default().body();
        defaultBody._break();
        evalBlock.add(conditionalBlock);
        generator.nestEvalBlock(evalBlock);
        HoldingContainer thenExpr = caseExpression.elseExpr.accept(this, generator);
        assigner.accept(thenExpr, evalBlock);
        generator.unNestEvalBlock();
      } else {
        for (final CaseExpression.CaseConditionNode node : caseExpression.caseConditions) {
          final JBlock local = generator.getEvalBlock();
          final JBlock conditionalBlock = new JBlock(false, false);
          final HoldingContainer holdingContainer = node.whenExpr.accept(this, generator);
          final JConditional jc = conditionalBlock._if(holdingContainer.getIsSet().eq(JExpr.lit(1))
            .cand(holdingContainer.getValue().eq(JExpr.lit(1))));
          generator.nestEvalBlock(jc._then());
          HoldingContainer thenExpr = node.thenExpr.accept(this, generator);
          generator.unNestEvalBlock();
          assigner.accept(thenExpr, jc._then());
          jc._then()._break(caseLabel);
          local.add(conditionalBlock);
        }
        final JBlock elseBlock = generator.getEvalBlock();
        generator.nestEvalBlock(elseBlock);
        final HoldingContainer elseExpr = caseExpression.elseExpr.accept(this, generator);
        generator.unNestEvalBlock();
        assigner.accept(elseExpr, elseBlock);
      }
      generator.unNestEvalBlock();
      return outputSupplier.get();
    }

    private JExpression extractLiteral(LogicalExpression whenExpr, Set<Object> literalsSoFar) {
      final FunctionHolderExpr expr = (FunctionHolderExpr) whenExpr;
      return (expr.args.get(0) instanceof ValueVectorReadExpression)
        ? extractLiteralFrom(expr.args.get(1), literalsSoFar) : extractLiteralFrom(expr.args.get(0), literalsSoFar);
    }

    private JExpression extractLiteralFrom(LogicalExpression litExpr, Set<Object> literalsSoFar) {
      JExpression ret;
      Object objToAdd;
      if (litExpr instanceof IntExpression) {
        final int val = ((IntExpression) litExpr).getInt();
        objToAdd = val;
        ret = JExpr.lit(val);
      } else {
        final String val = ((QuotedString) litExpr).getString();
        objToAdd = val;
        ret = JExpr.lit(val);
      }
      if (literalsSoFar.contains(objToAdd)) {
        return null;
      }
      literalsSoFar.add(objToAdd);
      return ret;
    }

    private HoldingContainer checkIsSwitch(CaseExpression caseExpression, ClassGenerator<?> generator) {
      if (caseExpression.caseConditions.size() <= 2) {
        return null;
      }
      TypedFieldId prevFieldId = null;
      ValueVectorReadExpression readExpr = null;
      for (final CaseExpression.CaseConditionNode node : caseExpression.caseConditions) {
        readExpr = extractValueVector(node.whenExpr);
        final TypedFieldId nextFieldId = (readExpr == null) ? null : readExpr.getFieldId();
        if (nextFieldId == null || (prevFieldId != null && !prevFieldId.equals(nextFieldId))) {
          // not matching
          readExpr = null;
          break;
        }
        prevFieldId = nextFieldId;
      }
      return readExpr == null ? null : readExpr.accept(this, generator);
    }

    private static final String EQUAL_FN = "equal";
    private ValueVectorReadExpression extractValueVector(LogicalExpression whenExpr) {
      ValueVectorReadExpression ret = null;
      if (whenExpr instanceof FunctionHolderExpr &&
        ((FunctionHolderExpr) whenExpr).getName().equalsIgnoreCase(EQUAL_FN)) {
        final FunctionHolderExpr expr = (FunctionHolderExpr) whenExpr;
        if (expr.args.size() == 2) {
          final LogicalExpression arg1 = expr.args.get(0);
          final LogicalExpression arg2 = expr.args.get(1);
          if (arg1 instanceof ValueVectorReadExpression &&
            (arg2 instanceof IntExpression || arg2 instanceof QuotedString)) {
            ret = (ValueVectorReadExpression) arg1;
          } else if (arg2 instanceof ValueVectorReadExpression &&
            (arg1 instanceof IntExpression || arg1 instanceof QuotedString)) {
            ret = (ValueVectorReadExpression) arg2;
          }
        }
      }
      return ret;
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath path, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("All schema paths should have been replaced with ValueVectorExpressions.");
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getLong()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getInt()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDate()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTime()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getIntervalYear()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTimeStamp()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitFloatConstant(FloatExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getFloat()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDouble()));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      HoldingContainer out = generator.declare(e.getCompleteType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getBoolean() ? 1 : 0));
      setIsSet(out, generator);
      return out;
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, generator);
      } else if (e instanceof ValueVectorWriteExpression) {
        return visitValueVectorWriteExpression((ValueVectorWriteExpression) e, generator);
      } else if (e instanceof ReturnValueExpression) {
        return visitReturnValueExpression((ReturnValueExpression) e, generator);
      } else if (e instanceof HoldingContainerExpression) {
        return ((HoldingContainerExpression) e).getContainer();
      } else if (e instanceof NullExpression) {
        return generator.declare(CompleteType.INT);
      } else if (e instanceof TypedNullConstant) {
        return generator.declare(e.getCompleteType());
      } else if (e instanceof InExpression) {
        return visitInExpression((InExpression) e, generator);
      } else {
        return super.visitUnknown(e, generator);
      }

    }

    @Override
    public HoldingContainer visitInExpression(InExpression e, final ClassGenerator<?> generator) {
      final JCodeModel model = generator.getModel();
      final JClass listType = e.getListType(model);
      final JVar valueSet = generator.declareClassField(generator.getNextVar("inMap"), listType);

      buildInSet:
      {
        final List<HoldingContainer> constants = FluentIterable.from(e.getConstants()).transform(new com.google.common.base.Function<LogicalExpression, HoldingContainer>() {
          @Override
          public HoldingContainer apply(LogicalExpression input) {
            return input.accept(EvalVisitor.this, generator);
          }
        }).toList();

        final HoldingContainer exampleConstant = constants.get(0);

        generator.getMappingSet().enterConstant();
        final JBlock block = generator.getSetupBlock();
        JClass holderType = e.getHolderType(model);
        JArray holders = JExpr.newArray(holderType, constants.size());
        JVar inVars = block.decl(holderType.array(), generator.getNextVar("inVals"), holders);
        for (int i = 0; i < constants.size(); i++) {
          block.assign(JExpr.component(inVars, JExpr.lit(i)), constants.get(i).getHolder());
        }

        block.assign(valueSet, JExpr._new(listType).arg(inVars));

        generator.getMappingSet().exitConstant();
      }

      buildInEvaluation:
      {
        JBlock block = generator.getEvalBlock();
        HoldingContainer eval = e.getEval().accept(EvalVisitor.this, generator);
        JClass outType = CodeModelArrowHelper.getHolderType(CompleteType.BIT, model);
        JVar var = block.decl(outType, generator.getNextVar("inListResult"), JExpr._new(outType));
        block.assign(var.ref("isSet"), JExpr.lit(1));
        JInvocation valueFound;
        if (e.isVarType()) {
          valueFound = valueSet.invoke("isContained")
            .arg(eval.getIsSet())
            .arg(eval.getHolder().ref("start"))
            .arg(eval.getHolder().ref("end"))
            .arg(eval.getHolder().ref("buffer"));
        } else {
          valueFound = valueSet.invoke("isContained")
            .arg(eval.getIsSet())
            .arg(eval.getValue());
        }

        block.assign(var.ref("value"), valueFound);

        return new HoldingContainer(CompleteType.BIT, var, var.ref("value"), var.ref("isSet"));
      }
    }

    private HoldingContainer visitValueVectorWriteExpression(ValueVectorWriteExpression e, ClassGenerator<?> generator) {

      final LogicalExpression child = e.getChild();
      final HoldingContainer inputContainer = child.accept(this, generator);

      JBlock block = generator.getEvalBlock();
      JExpression outIndex = generator.getMappingSet().getValueWriteIndex();
      JVar vv = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getOutgoing(), e.getFieldId());

      // Only when the input is a reader, use writer interface to copy value.
      // Otherwise, input is a holder and we use vv mutator to set value.
      if (inputContainer.isReader()) {
        JType writerImpl = generator.getModel()._ref(
          TypeHelper.getWriterImpl(getArrowMinorType(inputContainer.getCompleteType().toMinorType())));
        JType writerIFace = generator.getModel()._ref(
          TypeHelper.getWriterInterface(getArrowMinorType(inputContainer.getCompleteType().toMinorType())));

        JVar writer = generator.declareClassField("writer", writerIFace);
        generator.getSetupBlock().assign(writer, JExpr._new(writerImpl).arg(vv));
        generator.getEvalBlock().add(writer.invoke("setPosition").arg(outIndex));

        if (child.getCompleteType().isUnion() || child.getCompleteType().isComplex()) {
          final JClass complexCopierClass = generator.getModel()
            .ref(org.apache.arrow.vector.complex.impl.ComplexCopier.class);
          final JExpression castedWriter = JExpr.cast(generator.getModel().ref(FieldWriter.class), writer);
          generator.getEvalBlock()
            .add(complexCopierClass.staticInvoke("copy")
              .arg(inputContainer.getHolder())
              .arg(castedWriter));
        } else {
          String copyMethod = inputContainer.isSingularRepeated() ? "copyAsValueSingle" : "copyAsValue";
          generator.getEvalBlock().add(inputContainer.getHolder().invoke(copyMethod).arg(writer));
        }

        if (e.isSafe()) {
          HoldingContainer outputContainer = generator.declare(CompleteType.BIT);
          generator.getEvalBlock().assign(outputContainer.getValue(), JExpr.lit(1));
          return outputContainer;
        }
      } else {

        final JInvocation setMeth = GetSetVectorHelper.write(e.getChild().getCompleteType().toMinorType(), vv, inputContainer, outIndex, e.isSafe() ? "setSafe" : "set");
        JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
        block = jc._then();
        block.add(setMeth);

      }

      return null;
    }

    private HoldingContainer visitValueVectorReadExpression(ValueVectorReadExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      // declare value vector

      JExpression vv1 = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getIncoming(),
        e.getFieldId());
      JExpression indexVariable = generator.getMappingSet().getValueReadIndex();

      JExpression componentVariable = indexVariable.shrz(JExpr.lit(16));
      if (e.isSuperReader()) {
        vv1 = (vv1.component(componentVariable));
        indexVariable = indexVariable.band(JExpr.lit((int) Character.MAX_VALUE));
      }

      // evaluation work.
      HoldingContainer out = generator.declare(e.getCompleteType());

      final boolean hasReadPath = e.hasReadPath();
      final boolean complex = e.getCompleteType().isComplex();
      final boolean listVector = e.getTypedFieldId().isListVector();

      int[] fieldIds = e.getFieldId().getFieldIds();

      if (!hasReadPath && !complex) {
        JBlock eval = new JBlock();
        GetSetVectorHelper.read(e.getCompleteType(), vv1, eval, out, generator.getModel(), indexVariable);
        generator.getEvalBlock().add(eval);

      } else {
        JExpression vector = e.isSuperReader() ? vv1.component(componentVariable) : vv1;
        JExpression expr = vector.invoke("getReader");
        PathSegment seg = e.getReadPath();

        JVar isNull = null;
        boolean isNullReaderLikely = isNullReaderLikely(seg, complex || listVector);
        if (isNullReaderLikely) {
          isNull = generator.getEvalBlock().decl(generator.getModel().INT, generator.getNextVar("isNull"), JExpr.lit(0));
        }

        JLabel label = generator.getEvalBlock().label("complex");
        JBlock eval = generator.getEvalBlock().block();

        // position to the correct value.
        eval.add(expr.invoke("reset"));
        eval.add(expr.invoke("setPosition").arg(indexVariable));
        int listNum = 0;

        while (seg != null) {
          if (seg.isArray()) {
            // stop once we get to the last segment and the final type is neither complex nor repeated (map, list, repeated list).
            // In case of non-complex and non-repeated type, we return Holder, in stead of FieldReader.
            if (seg.isLastPath() && !complex && !listVector) {
              break;
            }

            JVar list = generator.declareClassField("list", generator.getModel()._ref(FieldReader.class));
            eval.assign(list, expr);

            // if this is an array, set a single position for the expression to
            // allow us to read the right data lower down.
            JVar desiredIndex = eval.decl(generator.getModel().INT, "desiredIndex" + listNum,
              JExpr.lit(seg.getArraySegment().getIndex()));
            // start with negative one so that we are at zero after first call
            // to next.
            JVar currentIndex = eval.decl(generator.getModel().INT, "currentIndex" + listNum, JExpr.lit(-1));

            eval._while( //
              currentIndex.lt(desiredIndex) //
                .cand(list.invoke("next"))).body().assign(currentIndex, currentIndex.plus(JExpr.lit(1)));


            JBlock ifNoVal = eval._if(desiredIndex.ne(currentIndex))._then().block();
            ifNoVal.assign(out.getIsSet(), JExpr.lit(0));
            ifNoVal.assign(isNull, JExpr.lit(1));
            ifNoVal._break(label);

            expr = list.invoke("reader");
            listNum++;
          } else {
            JExpression fieldName = JExpr.lit(seg.getNameSegment().getPath());
            expr = expr.invoke("reader").arg(fieldName);
          }
          seg = seg.getChild();
        }

        if (complex) {
          JVar complexReader = generator.declareClassField("reader", generator.getModel()._ref(FieldReader.class));

          if (isNullReaderLikely) {
            JConditional jc = generator.getEvalBlock()._if(isNull.eq(JExpr.lit(0)));

            JClass nrClass = generator.getModel().ref(org.apache.arrow.vector.complex.impl.NullReader.class);
            JExpression nullReader = nrClass.staticRef("INSTANCE");

            jc._then().assign(complexReader, expr);
            jc._else().assign(complexReader, nullReader);
          } else {
            eval.assign(complexReader, expr);
          }

          HoldingContainer hc = new HoldingContainer(e.getCompleteType(), complexReader, null, null, false, true);
          return hc;
        } else {
          if (seg != null) {
            eval.add(expr.invoke("read").arg(JExpr.lit(seg.getArraySegment().getIndex())).arg(out.getHolder()));
          } else {
            eval.add(expr.invoke("read").arg(out.getHolder()));
          }
        }

      }

      return out;
    }

    /*  Check if a Path expression could produce a NullReader. A path expression will produce a null reader, when:
     *   1) It contains an array segment as non-leaf segment :  a.b[2].c.  segment [2] might produce null reader.
     *   2) It contains an array segment as leaf segment, AND the final output is complex or repeated : a.b[2], when
     *     the final type of this expression is a map, or releated list, or repeated map.
     */
    private boolean isNullReaderLikely(PathSegment seg, boolean complexOrRepeated) {
      while (seg != null) {
        if (seg.isArray() && !seg.isLastPath()) {
          return true;
        }

        if (seg.isLastPath() && complexOrRepeated) {
          return true;
        }

        seg = seg.getChild();
      }
      return false;
    }

    private HoldingContainer visitReturnValueExpression(ReturnValueExpression e, ClassGenerator<?> generator) {
      LogicalExpression child = e.getChild();
      // Preconditions.checkArgument(child.getMajorType().equals(Types.REQUIRED_BOOLEAN));
      HoldingContainer hc = child.accept(this, generator);
      if (e.isReturnTrueOnOne()) {
        generator.getEvalBlock()._return(hc.getValue().eq(JExpr.lit(1)));
      } else {
        generator.getEvalBlock()._return(hc.getValue());
      }

      return null;
    }

    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
      throws RuntimeException {
      CompleteType completeType = CompleteType.VARCHAR;
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = CodeModelArrowHelper.getHolderType(completeType, generator.getModel());
      JVar var = generator.declareClassField("string", holderType);
      JExpression stringLiteral = JExpr.lit(e.value);
      JExpression buffer = JExpr.direct("context").invoke("getManagedBuffer");
      setup.assign(var,
        generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getNullableVarCharHolder").arg(buffer).arg(stringLiteral));
      return new HoldingContainer((completeType), var, var.ref("value"), var.ref("isSet"));
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      CompleteType completeType = CompleteType.INTERVAL_DAY_SECONDS;
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = CodeModelArrowHelper.getHolderType(completeType, generator.getModel());
      JVar var = generator.declareClassField("intervalday", holderType);
      JExpression dayLiteral = JExpr.lit(e.getIntervalDay());
      JExpression millisLiteral = JExpr.lit(e.getIntervalMillis());
      setup.assign(
        var,
        generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getNullableIntervalDayHolder").arg(dayLiteral)
          .arg(millisLiteral));
      return new HoldingContainer(completeType, var, var.ref("value"), var.ref("isSet"));
    }

    @Override
    public HoldingContainer visitDecimalConstant(DecimalExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      CompleteType completeType = CompleteType.fromDecimalPrecisionScale(e.getPrecision(), e.getScale());
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = CodeModelArrowHelper.getHolderType(completeType, generator.getModel());
      JVar var = generator.declareClassField("dec38", holderType);
      JExpression decimal = JExpr.lit(e.getDecimal().toString());
      JExpression buffer = JExpr.direct("context").invoke("getManagedBuffer");
      setup.assign(var, generator.getModel().ref(ValueHolderHelper.class).staticInvoke
        ("getNullableDecimalHolder").arg(buffer).arg(decimal));
      return new HoldingContainer(completeType, var, var.ref("value"), var.ref("isSet"));
    }

    @Override
    public HoldingContainer visitCastExpression(CastExpression e, ClassGenerator<?> value) throws RuntimeException {
      throw new UnsupportedOperationException("CastExpression is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitConvertExpression(ConvertExpression e, ClassGenerator<?> value)
      throws RuntimeException {
      String convertFunctionName = e.getConvertFunction() + e.getEncodingType();

      List<LogicalExpression> newArgs = Lists.newArrayList();
      newArgs.add(e.getInput()); // input_expr

      FunctionCall fc = new FunctionCall(convertFunctionName, newArgs);
      return fc.accept(this, value);
    }

    private HoldingContainer visitBooleanAnd(BooleanOperator op,
                                             ClassGenerator<?> generator) {

      HoldingContainer out = generator.declare(op.getCompleteType());

      JLabel label = generator.getEvalBlockLabel("AndOP");
      JBlock eval = generator.createInnerEvalBlock();
      generator.nestEvalBlock(eval);  // enter into nested block

      HoldingContainer arg = null;

      JExpression e = null;

      //  value of boolean "and" when one side is null
      //    p       q     p and q
      //    true    null     null
      //    false   null     false
      //    null    true     null
      //    null    false    false
      //    null    null     null
      for (int i = 0; i < op.args.size(); i++) {
        arg = op.args.get(i).accept(this, generator);

        JBlock earlyExit = null;
        earlyExit = eval._if(arg.getIsSet().eq(JExpr.lit(1)).cand(arg.getValue().ne(JExpr.lit(1))))._then();
        if (e == null) {
          e = arg.getIsSet();
        } else {
          e = e.mul(arg.getIsSet());
        }
        earlyExit.assign(out.getIsSet(), JExpr.lit(1));

        earlyExit.assign(out.getValue(), JExpr.lit(0));
        earlyExit._break(label);
      }

      assert (e != null);

      JConditional notSetJC = eval._if(e.eq(JExpr.lit(0)));
      notSetJC._then().assign(out.getIsSet(), JExpr.lit(0));

      JBlock setBlock = notSetJC._else().block();
      setBlock.assign(out.getIsSet(), JExpr.lit(1));
      setBlock.assign(out.getValue(), JExpr.lit(1));

      generator.unNestEvalBlock();     // exit from nested block

      return out;
    }

    private HoldingContainer visitBooleanOr(BooleanOperator op,
                                            ClassGenerator<?> generator) {

      HoldingContainer out = generator.declare(op.getCompleteType());

      JLabel label = generator.getEvalBlockLabel("OrOP");
      JBlock eval = generator.createInnerEvalBlock();
      generator.nestEvalBlock(eval);   // enter into nested block.

      HoldingContainer arg = null;

      JExpression e = null;

      //  value of boolean "or" when one side is null
      //    p       q       p and q
      //    true    null     true
      //    false   null     null
      //    null    true     true
      //    null    false    null
      //    null    null     null

      for (int i = 0; i < op.args.size(); i++) {
        arg = op.args.get(i).accept(this, generator);

        JBlock earlyExit = eval._if(arg.getIsSet().eq(JExpr.lit(1)).cand(arg.getValue().eq(JExpr.lit(1))))._then();
        if (e == null) {
          e = arg.getIsSet();
        } else {
          e = e.mul(arg.getIsSet());
        }
        earlyExit.assign(out.getIsSet(), JExpr.lit(1));
        earlyExit.assign(out.getValue(), JExpr.lit(1));
        earlyExit._break(label);
      }

      assert (e != null);

      JConditional notSetJC = eval._if(e.eq(JExpr.lit(0)));
      notSetJC._then().assign(out.getIsSet(), JExpr.lit(0));

      JBlock setBlock = notSetJC._else().block();
      setBlock.assign(out.getIsSet(), JExpr.lit(1));
      setBlock.assign(out.getValue(), JExpr.lit(0));

      generator.unNestEvalBlock();   // exit from nested block.

      return out;
    }
  }

  private class CSEFilter extends InnerMethodNester {
    public CSEFilter(FunctionContext functionContext, boolean allowInnerMethods) {
      super(functionContext, allowInnerMethods);
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall call, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(call, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitFunctionCall(call, generator);
        put(call, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression holder, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(holder, generator.getMappingSet());
      if (hc == null || holder.isRandom()) {
        hc = super.visitFunctionHolderExpression(holder, generator);
        put(holder, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(ifExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitIfExpression(ifExpr, generator);
        put(ifExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitCaseExpression(CaseExpression caseExpr, ClassGenerator<?> generator) throws RuntimeException {
      if (caseExpr.caseConditions.size() < newMethodThreshold) {
        HoldingContainer hc = getPrevious(caseExpr, generator.getMappingSet());
        if (hc == null) {
          hc = super.visitCaseExpression(caseExpr, generator);
          put(caseExpr, hc, generator.getMappingSet());
        }
        return hc;
      } else {
        // very unlikely that large case expressions are duplicated, not worth checking and storing
        // previous computations for such large case expressions
        return super.visitCaseExpression(caseExpr, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator call, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(call, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitBooleanOperator(call, generator);
        put(call, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath path, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(path, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitSchemaPath(path, generator);
        put(path, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression intExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitIntConstant(intExpr, generator);
    }

    @Override
    public HoldingContainer visitFloatConstant(FloatExpression fExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitFloatConstant(fExpr, generator);
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression longExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitLongConstant(longExpr, generator);
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression dateExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitDateConstant(dateExpr, generator);
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression timeExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitTimeConstant(timeExpr, generator);
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression timeStampExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitTimeStampConstant(timeStampExpr, generator);
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression intervalYearExpression, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitIntervalYearConstant(intervalYearExpression, generator);
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression intervalDayExpression, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitIntervalDayConstant(intervalDayExpression, generator);
    }

    @Override
    public HoldingContainer visitDecimalConstant(DecimalExpression decExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitDecimalConstant(decExpr, generator);
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression dExpr, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitDoubleConstant(dExpr, generator);
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitBooleanConstant(e, generator);
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitQuotedStringConstant(e, generator);
    }

    @Override
    public HoldingContainer visitNullConstant(TypedNullConstant e, ClassGenerator<?> generator) throws RuntimeException {
      return super.visitNullConstant(e, generator);
    }

    @Override
    public HoldingContainer visitNullExpression(NullExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(e, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitNullExpression(e, generator);
        put(e, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        HoldingContainer hc = getPrevious(e, generator.getMappingSet());
        if (hc == null) {
          hc = super.visitUnknown(e, generator);
          put(e, hc, generator.getMappingSet());
        }
        return hc;
      }
      return super.visitUnknown(e, generator);
    }

    @Override
    public HoldingContainer visitCastExpression(CastExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(e, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitCastExpression(e, generator);
        put(e, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitConvertExpression(ConvertExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(e, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitConvertExpression(e, generator);
        put(e, hc, generator.getMappingSet());
      }
      return hc;
    }
  }

  /**
   * Keeps track of how many expression have been visited in the current eval block. A new method is rolled out whenever
   * the threshold is reached
   */
  private class InnerMethodNester extends EvalVisitor {
    private final boolean allowNewMethods;
    private final Deque<Integer> exprCount = new ArrayDeque<>();
    private final Deque<Integer> caseConditionCount = new ArrayDeque<>();

    InnerMethodNester(FunctionContext functionContext, boolean allowNewMethods) {
      super(functionContext);
      this.allowNewMethods = allowNewMethods;
      exprCount.push(0);
    }

    private void inc() {
      exprCount.push(exprCount.pop() + 1);
    }

    private boolean shouldNestMethod() {
      return ((caseConditionCount.isEmpty() && exprCount.peekLast() > newMethodThreshold)
        || (!caseConditionCount.isEmpty() && caseConditionCount.peek() > newMethodThreshold)) && allowNewMethods;
    }

    private void addCaseDepth() {
      if (!caseConditionCount.isEmpty()) {
        caseConditionCount.push(0);
      }
    }

    private void removeCaseDepth() {
      if (!caseConditionCount.isEmpty()) {
        caseConditionCount.pop();
      }
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression holder, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
        addCaseDepth();
        HoldingContainer out = generator.declare(holder.getCompleteType(), false);
        JMethod setupMethod = generator.nestSetupMethod();
        JMethod method = generator.innerMethod(holder.getCompleteType());
        HoldingContainer returnContainer = super.visitFunctionHolderExpression(holder, generator);
        method.body()._return(returnContainer.getHolder());
        generator.unNestEvalBlock();
        generator.unNestSetupBlock();
        JInvocation methodCall = generator.invokeInnerMethod(method, BlockType.EVAL);
        generator.getEvalBlock().assign(out.getHolder(), methodCall);
        generator.getSetupBlock().add(generator.invokeInnerMethod(setupMethod, BlockType.SETUP));

        removeCaseDepth();
        exprCount.pop();
        return out;
      }
      return super.visitFunctionHolderExpression(holder, generator);
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
        addCaseDepth();
        HoldingContainer out = generator.declare(ifExpr.getCompleteType(), false);
        JMethod setupMethod = generator.nestSetupMethod();
        JMethod method = generator.innerMethod(ifExpr.getCompleteType());
        HoldingContainer returnContainer = super.visitIfExpression(ifExpr, generator);
        method.body()._return(returnContainer.getHolder());
        generator.unNestEvalBlock();
        generator.unNestSetupBlock();
        JInvocation methodCall = generator.invokeInnerMethod(method, BlockType.EVAL);
        generator.getEvalBlock().assign(out.getHolder(), methodCall);
        generator.getSetupBlock().add(generator.invokeInnerMethod(setupMethod, BlockType.SETUP));

        removeCaseDepth();
        exprCount.pop();
        return out;
      }
      return super.visitIfExpression(ifExpr, generator);
    }

    @Override
    public HoldingContainer visitCaseExpression(CaseExpression caseExpr, ClassGenerator<?> generator)
      throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
        HoldingContainer out = generator.declare(caseExpr.getCompleteType(), false);
        JMethod setupMethod = generator.nestSetupMethod();
        JMethod method = generator.innerMethod(caseExpr.getCompleteType());
        HoldingContainer returnContainer = super.visitCaseExpression(caseExpr, generator);
        method.body()._return(returnContainer.getHolder());
        generator.unNestEvalBlock();
        generator.unNestSetupBlock();
        JInvocation methodCall = generator.invokeInnerMethod(method, BlockType.EVAL);
        generator.getEvalBlock().assign(out.getHolder(), methodCall);
        generator.getSetupBlock().add(generator.invokeInnerMethod(setupMethod, BlockType.SETUP));

        exprCount.pop();
        return out;
      }
      caseConditionCount.push(CaseExpressionCounter.getCaseConditionCount(caseExpr));
      try {
        return super.visitCaseExpression(caseExpr, generator);
      } finally {
        caseConditionCount.pop();
      }
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator call, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
        addCaseDepth();
        HoldingContainer out = generator.declare(call.getCompleteType(), false);
        JMethod setupMethod = generator.nestSetupMethod();
        JMethod method = generator.innerMethod(call.getCompleteType());
        HoldingContainer returnContainer = super.visitBooleanOperator(call, generator);
        method.body()._return(returnContainer.getHolder());
        generator.unNestEvalBlock();
        generator.unNestSetupBlock();
        JInvocation methodCall = generator.invokeInnerMethod(method, BlockType.EVAL);
        generator.getEvalBlock().assign(out.getHolder(), methodCall);
        generator.getSetupBlock().add(generator.invokeInnerMethod(setupMethod, BlockType.SETUP));

        removeCaseDepth();
        exprCount.pop();
        return out;
      }
      return super.visitBooleanOperator(call, generator);
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      return super.visitUnknown(e, generator);
    }

    @Override
    public HoldingContainer visitConvertExpression(ConvertExpression e, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
        addCaseDepth();
        HoldingContainer out = generator.declare(e.getCompleteType(), false);
        JMethod setupMethod = generator.nestSetupMethod();
        JMethod method = generator.innerMethod(e.getCompleteType());
        HoldingContainer returnContainer = super.visitConvertExpression(e, generator);
        method.body()._return(returnContainer.getHolder());
        generator.unNestEvalBlock();
        generator.unNestSetupBlock();
        JInvocation methodCall = generator.invokeInnerMethod(method, BlockType.EVAL);
        generator.getEvalBlock().assign(out.getHolder(), methodCall);
        generator.getSetupBlock().add(generator.invokeInnerMethod(setupMethod, BlockType.SETUP));

        removeCaseDepth();
        exprCount.pop();
        return out;
      }
      return super.visitConvertExpression(e, generator);
    }
  }

  /**
   * Filter constants at the first level as constants can be treated at global scope.
   * Also, constants with the same literal value need be rendered only once.
   */
  private class ConstantFilter extends CSEFilter {
    private final Set<LogicalExpression> constantBoundaries;
    private final Map<ExpressionHolder, HoldingContainer> processedConstantMap;

    public ConstantFilter(Set<LogicalExpression> constantBoundaries, FunctionContext functionContext,
                          boolean allowNewMethods) {
      super(functionContext, allowNewMethods);
      this.constantBoundaries = constantBoundaries;
      this.processedConstantMap = new HashMap<>();
    }

    private HoldingContainer preProcessIfConstant(LogicalExpression e, ClassGenerator<?> generator,
                                                  Supplier<HoldingContainer> containerCreator,
                                                  Supplier<HoldingContainer> containerCreatorForVar) {
      if (constantBoundaries.contains(e)) {
        final ExpressionHolder holder = new ExpressionHolder(e, generator.getMappingSet());
        final HoldingContainer hc = processedConstantMap.get(holder);
        if (hc == null) {
          // constants have global scope, so holding container can be reused globally
          generator.getMappingSet().enterConstant();
          final HoldingContainer c = containerCreator.get();
          c.setNullConstant(generator.getMappingSet().isNullConstant());
          final HoldingContainer rendered = renderConstantExpression(generator, c);
          processedConstantMap.put(holder, rendered);
          return rendered;
        } else {
          return hc;
        }
      } else if (generator.getMappingSet().isWithinConstant()) {
        return containerCreator.get().setConstant(true).setNullConstant(generator.getMappingSet().isNullConstant());
      } else {
        return (containerCreatorForVar != null) ? containerCreatorForVar.get() : containerCreator.get();
      }
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall e, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitFunctionHolderExpression(e, generator), null);
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitBooleanOperator(e, generator), () -> {
        // want to optimize a non-constant boolean operator.
        if (functionContext.getCompilationOptions().enableOrOptimization() && "booleanOr".equals(e.getName())) {
          List<LogicalExpression> newExpressions = OrInConverter.optimizeMultiOrs(e.args, constantBoundaries,
            functionContext.getCompilationOptions().getOrOptimizationThreshold(),
            functionContext.getCompilationOptions().getVarcharOrOptimizationThreshold());
          return super.visitBooleanOperator(new BooleanOperator(e.getName(), newExpressions), generator);
        }
        return super.visitBooleanOperator(e, generator);
      });
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitIfExpression(e, generator), null);
    }

    @Override
    public HoldingContainer visitCaseExpression(CaseExpression caseExpr, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(caseExpr, generator, () -> super.visitCaseExpression(caseExpr, generator), null);
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitSchemaPath(e, generator), null);
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitLongConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitDecimalConstant(DecimalExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitDecimalConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitIntConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitDateConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitTimeConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitIntervalYearConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitTimeStampConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitFloatConstant(FloatExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitFloatConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitDoubleConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitBooleanConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitNullConstant(TypedNullConstant e, ClassGenerator<?> generator)
      throws RuntimeException {
      generator.getMappingSet().setNullConstant();
      return super.visitNullConstant(e, generator).setNullConstant(true);
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitUnknown(e, generator), null);
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitQuotedStringConstant(e, generator), null);
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
      throws RuntimeException {
      return preProcessIfConstant(e, generator, () -> super.visitIntervalDayConstant(e, generator), null);
    }

    /*
     * Get a HoldingContainer for a constant expression. The returned
     * HoldingContainer will indicate it's for a constant expression.
     */
    private HoldingContainer renderConstantExpression(ClassGenerator<?> generator, HoldingContainer input) {
      final JType jType = CodeModelArrowHelper.getHolderType(input.getCompleteType(), generator.getModel());
      JAssignmentTarget target = generator.declareNextConstantField(input.getCompleteType(), jType);
      generator.getEvalBlock().assign(target, input.getHolder());
      generator.getMappingSet().exitConstant();
      return new HoldingContainer(input.getCompleteType(), target, jType, target.ref("value"), target.ref("isSet"))
        .setConstant(true);
    }
  }

  /**
   * Count total function and/or boolean expressions inside case conditions
   */
  private static class CaseExpressionCounter extends AbstractExprVisitor<Integer, Void, RuntimeException> {
    @Override
    public Integer visitCaseExpression(CaseExpression caseExpression, Void value) {
      return countChildren(caseExpression);
    }

    @Override
    public Integer visitBooleanOperator(BooleanOperator op, Void value) {
      return countChildren(op) + 1;
    }

    @Override
    public Integer visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Void value) {
      return countChildren(holderExpr) + 1;
    }

    @Override
    public Integer visitIfExpression(IfExpression ifExpr, Void value) {
      return countChildren(ifExpr) + 1;
    }

    private int countChildren(LogicalExpression e) {
      int sum = 0;
      for (LogicalExpression child : e) {
        sum += child.accept(this, null);
      }
      return sum;
    }

    @Override
    public Integer visitUnknown(LogicalExpression e, Void value) {
      return 0;
    }

    private static int getCaseConditionCount(CaseExpression caseExpression) {
      return caseExpression.accept(new CaseExpressionCounter(), null);
    }
  }
}
