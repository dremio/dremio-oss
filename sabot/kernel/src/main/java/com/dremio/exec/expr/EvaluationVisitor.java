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

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.arrow.vector.ValueHolderHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import com.dremio.common.expression.BooleanOperator;
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
import com.dremio.exec.compile.sig.ConstantExpressionIdentifier;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator.BlockType;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

/**
 * Visitor that generates code for eval
 */
public class EvaluationVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EvaluationVisitor.class);
  private final FunctionContext functionContext;
  private final int newMethodThreshold;

  public EvaluationVisitor(FunctionContext functionContext) {
    super();
    this.functionContext = functionContext;
    this.newMethodThreshold = functionContext.getCompilationOptions().getNewMethodThreshold();
  }

  public HoldingContainer addExpr(LogicalExpression e, ClassGenerator<?> generator, boolean allowInnerMethods) {

    Set<LogicalExpression> constantBoundaries;
    if (generator.getMappingSet().hasEmbeddedConstant()) {
      constantBoundaries = Collections.emptySet();
    } else {
      constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(e);
    }
    return e.accept(new CSEFilter(constantBoundaries, functionContext, allowInnerMethods), generator);
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

  Map<ExpressionHolder,HoldingContainer> previousExpressions = Maps.newHashMap();

  Stack<Map<ExpressionHolder,HoldingContainer>> mapStack = new Stack<>();

  void newScope() {
    mapStack.push(previousExpressions);
    previousExpressions = new HashMap<>(previousExpressions);
  }

  void leaveScope() {
    previousExpressions.clear();
    previousExpressions = mapStack.pop();
  }
  /**
   * Get a HoldingContainer for the expression if it had been already evaluated
   */
  private HoldingContainer getPrevious(LogicalExpression expression, MappingSet mappingSet) {
    HoldingContainer previous = previousExpressions.get(new ExpressionHolder(expression, mappingSet));
    if (previous != null) {
      logger.debug("Found previously evaluated expression: {}", ExpressionStringBuilder.toString(expression));
    }
    return previous;
  }

  private void put(LogicalExpression expression, HoldingContainer hc, MappingSet mappingSet) {
    previousExpressions.put(new ExpressionHolder(expression, mappingSet), hc);
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
      } else if(op.getName().equals("booleanOr")) {
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

      buildInSet: {
        final List<HoldingContainer> constants = FluentIterable.from(e.getConstants()).transform(new com.google.common.base.Function<LogicalExpression, HoldingContainer>(){
          @Override
          public HoldingContainer apply(LogicalExpression input) {
            return input.accept(EvalVisitor.this, generator);
          }}).toList();

        final HoldingContainer exampleConstant = constants.get(0);

        generator.getMappingSet().enterConstant();
        final JBlock block = generator.getSetupBlock();
        JClass holderType = e.getHolderType(model);
        JArray holders = JExpr.newArray(holderType, constants.size());
        JVar inVars = block.decl(holderType.array(), generator.getNextVar("inVals"), holders);
        for(int i =0; i < constants.size(); i++) {
          block.assign(JExpr.component(inVars, JExpr.lit(i)), constants.get(i).getHolder());
        }

        block.assign(valueSet, JExpr._new(listType).arg(inVars));

        generator.getMappingSet().exitConstant();
      }

      buildInEvaluation: {
        JBlock block = generator.getEvalBlock();
        HoldingContainer eval = e.getEval().accept(EvalVisitor.this, generator);
        JClass outType = CodeModelArrowHelper.getHolderType(CompleteType.BIT, model);
        JVar var = block.decl(outType, generator.getNextVar("inListResult"), JExpr._new(outType));
        block.assign(var.ref("isSet"), JExpr.lit(1));
        JInvocation valueFound;
        if(e.isVarType()) {
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
      for (int i = 1; i < fieldIds.length; i++) {

      }

      if (!hasReadPath && !complex) {
        JBlock eval = new JBlock();
        GetSetVectorHelper.read(e.getCompleteType(),  vv1, eval, out, generator.getModel(), indexVariable);
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

    @Override
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

        earlyExit.assign(out.getValue(),  JExpr.lit(0));
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
        earlyExit.assign(out.getValue(),  JExpr.lit(1));
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

  private class CSEFilter extends ConstantFilter {

    public CSEFilter(Set<LogicalExpression> constantBoundaries, FunctionContext functionContext, boolean allowInnerMethods) {
      super(constantBoundaries, functionContext, allowInnerMethods);
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
      HoldingContainer hc = getPrevious(intExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitIntConstant(intExpr, generator);
        put(intExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitFloatConstant(FloatExpression fExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(fExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitFloatConstant(fExpr, generator);
        put(fExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression longExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(longExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitLongConstant(longExpr, generator);
        put(longExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression dateExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(dateExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitDateConstant(dateExpr, generator);
        put(dateExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression timeExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(timeExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitTimeConstant(timeExpr, generator);
        put(timeExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression timeStampExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(timeStampExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitTimeStampConstant(timeStampExpr, generator);
        put(timeStampExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression intervalYearExpression, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(intervalYearExpression, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitIntervalYearConstant(intervalYearExpression, generator);
        put(intervalYearExpression, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression intervalDayExpression, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(intervalDayExpression, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitIntervalDayConstant(intervalDayExpression, generator);
        put(intervalDayExpression, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitDecimalConstant(DecimalExpression decExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(decExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitDecimalConstant(decExpr, generator);
        put(decExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression dExpr, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(dExpr, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitDoubleConstant(dExpr, generator);
        put(dExpr, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(e, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitBooleanConstant(e, generator);
        put(e, hc, generator.getMappingSet());
      }
      return hc;
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer hc = getPrevious(e, generator.getMappingSet());
      if (hc == null) {
        hc = super.visitQuotedStringConstant(e, generator);
        put(e, hc, generator.getMappingSet());
      }
      return hc;
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

    private Deque<Integer> exprCount = new LinkedList<>();

    InnerMethodNester(FunctionContext functionContext, boolean allowNewMethods) {
      super(functionContext);
      this.allowNewMethods = allowNewMethods;
      exprCount.push(0);
    }

    private void inc() {
      exprCount.push(exprCount.pop() + 1);
    }

    private boolean shouldNestMethod() {
      return exprCount.peekLast() > newMethodThreshold;
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression holder, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (allowNewMethods && shouldNestMethod()) {
        exprCount.push(0);
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

        exprCount.pop();
        return out;
      }
      return super.visitIfExpression(ifExpr, generator);
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator call, ClassGenerator<?> generator) throws RuntimeException {
      inc();
      if (shouldNestMethod()) {
        exprCount.push(0);
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

        exprCount.pop();
        return out;
      }
      return super.visitConvertExpression(e, generator);
    }
  }

  private class ConstantFilter extends InnerMethodNester {

    private Set<LogicalExpression> constantBoundaries;

    public ConstantFilter(Set<LogicalExpression> constantBoundaries, FunctionContext functionContext, boolean allowNewMethods) {
      super(functionContext, allowNewMethods);
      this.constantBoundaries = constantBoundaries;
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall e, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "
          + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitFunctionHolderExpression(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitFunctionHolderExpression(e, generator).setConstant(true);
      } else {
        return super.visitFunctionHolderExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanOperator(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitBooleanOperator(e, generator).setConstant(true);
      } else {

        // want to optimize a non-constant boolean operator.
        if(functionContext.getCompilationOptions().enableOrOptimization() && "booleanOr".equals(e.getName())) {
          List<LogicalExpression> newExpressions = OrInConverter.optimizeMultiOrs(e.args, constantBoundaries, functionContext.getCompilationOptions().getOrOptimizationThreshold());
          return super.visitBooleanOperator(new BooleanOperator(e.getName(), newExpressions), generator);
        }

        return super.visitBooleanOperator(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIfExpression(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIfExpression(e, generator).setConstant(true);
      } else {
        return super.visitIfExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitSchemaPath(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitSchemaPath(e, generator).setConstant(true);
      } else {
        return super.visitSchemaPath(e, generator);
      }
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitLongConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitLongConstant(e, generator).setConstant(true);
      } else {
        return super.visitLongConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimalConstant(DecimalExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimalConstant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimalConstant(e, generator).setConstant(true);
      } else {
        return super.visitDecimalConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDateConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDateConstant(e, generator).setConstant(true);
      } else {
        return super.visitDateConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalYearConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalYearConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalYearConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeStampConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeStampConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeStampConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitFloatConstant(FloatExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitFloatConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitFloatConstant(e, generator).setConstant(true);
      } else {
        return super.visitFloatConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDoubleConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDoubleConstant(e, generator).setConstant(true);
      } else {
        return super.visitDoubleConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitBooleanConstant(e, generator).setConstant(true);
      } else {
        return super.visitBooleanConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitUnknown(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitUnknown(e, generator).setConstant(true);
      } else {
        return super.visitUnknown(e, generator);
      }
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitQuotedStringConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitQuotedStringConstant(e, generator).setConstant(true);
      } else {
        return super.visitQuotedStringConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalDayConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalDayConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalDayConstant(e, generator);
      }
    }

    /*
     * Get a HoldingContainer for a constant expression. The returned
     * HoldingContainder will indicate it's for a constant expression.
     */
    private HoldingContainer renderConstantExpression(ClassGenerator<?> generator, HoldingContainer input) {
      JVar fieldValue = generator.declareClassField("constant", CodeModelArrowHelper.getHolderType(input.getCompleteType(), generator.getModel()));
      generator.getEvalBlock().assign(fieldValue, input.getHolder());
      generator.getMappingSet().exitConstant();
      return new HoldingContainer(input.getCompleteType(), fieldValue, fieldValue.ref("value"), fieldValue.ref("isSet"))
          .setConstant(true);
    }
  }

}
