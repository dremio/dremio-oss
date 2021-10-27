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
package com.dremio.sabot.op.join.vhash;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import javax.inject.Named;

import org.apache.arrow.vector.complex.FieldIdUtil2;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.HoldingContainerExpression;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.sun.codemodel.JExpr;

/**
 * Matches the extra condition in a hash join by generating the code for that expression and evaluating data inside
 * containers (arrow) of both probe side and build side against the expression.
 * <p>
 * This code uses JAVA code generator to generate code that matches the expression.
 * <p>
 */
public abstract class ExtraConditionEvaluator implements AutoCloseable {

  private static final TemplateClassDefinition<ExtraConditionEvaluator> TEMPLATE_DEFINITION =
    new TemplateClassDefinition<>(ExtraConditionEvaluator.class, ExtraConditionEvaluator.class);

  public void setup(
    FunctionContext context,
    VectorAccessible probeBatch,
    VectorAccessible buildBatch) {
    doSetup(context, probeBatch, buildBatch);
  }

  public abstract void doSetup(
    @Named("context") FunctionContext context,
    @Named("probeVectorAccessible") VectorAccessible probeVectorAccessible,
    @Named("buildVectorAccessible") VectorAccessible buildVectorAccessible
  );

  public abstract boolean doEval(
    @Named("probeIndex") int probeIndex,
    @Named("buildIndex") int buildIndex
  );

  /**
   * Generate code (a JAVA sub class of this class) that can evaluate the expression specified by extra join
   * condition.
   * <p>
   * Note that a build side key in the extra join condition expression is converted to a probe side key as the
   * build side container may not have keys in the schema of the hyper container in certain cases.
   * </p>
   *
   * @param expr          The extra join condition, rooted with InputReferences. Must be non null.
   * @param classProducer Tool for class generation.
   * @param probe         The probe side of the expression (single container).
   * @param build         The build side of the expression (hyper container).
   * @param build2ProbeKeys A mapping of equivalent probe side keys to build side keys
   * @return The generated Matcher
   */
  public static ExtraConditionEvaluator generate(LogicalExpression expr, ClassProducer classProducer,
                                                 VectorAccessible probe, VectorAccessible build,
                                                 Map<String, String> build2ProbeKeys) {
    final MappingSet probeMappingSet = new MappingSet("probeIndex", null, "probeVectorAccessible", null,
      ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet buildMappingSet = new MappingSet("buildIndex", null, "buildVectorAccessible", null,
      ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

    CodeGenerator<ExtraConditionEvaluator> cg = classProducer.createGenerator(TEMPLATE_DEFINITION);
    ClassGenerator<ExtraConditionEvaluator> g = cg.getRoot();

    ReferenceMaterializer referenceMaterializer = new ReferenceMaterializer(g, build2ProbeKeys, (i) -> {
      switch (i) {
        case 1:
          return new InputSide(buildMappingSet, build.getSchema());
        case 0:
          return new InputSide(probeMappingSet, probe.getSchema());
        default:
          throw new UnsupportedOperationException("Unknown input reference " + i);
      }
    });

    // materialize, identify and separate probe side and build side of expression
    final LogicalExpression materialized = classProducer.materialize(expr.accept(referenceMaterializer, null), null);

    // add the materialized expression to codegen
    final HoldingContainer out = g.addExpr(materialized, ClassGenerator.BlockCreateMode.MERGE, false);

    // return true if the condition is positive.
    g.getEvalBlock()._if(
      out.getIsSet().eq(JExpr.lit(1)).cand(out.getValue().eq(JExpr.lit(1))))._then()._return(JExpr.TRUE);

    g.getEvalBlock()._return(JExpr.FALSE);
    return cg.getImplementationClass();
  }

  /**
   * Pojo for holding the information needed to materialize each set of expression.
   */
  private static class InputSide {
    private final MappingSet mappingSet;
    private final BatchSchema schema;

    public InputSide(MappingSet mappingSet, BatchSchema schema) {
      super();
      this.mappingSet = mappingSet;
      this.schema = schema;
    }
  }

  /**
   * ExprVisitor that rewrites the tree so that each reference is pointing at the correct side of the join.
   */
  private static class ReferenceMaterializer extends AbstractExprVisitor<LogicalExpression, Void, RuntimeException> {

    private final ClassGenerator<?> generator;
    private final IntFunction<InputSide> inputFunction;
    private final Map<String, String> build2ProbeKeys;

    public ReferenceMaterializer(ClassGenerator<?> generator, Map<String, String> build2ProbeKeys,
                                 IntFunction<InputSide> inputFunction) {
      super();
      this.generator = generator;
      this.inputFunction = inputFunction;
      this.build2ProbeKeys = build2ProbeKeys;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, Void v) {
      return e;
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder,
                                                           Void v) throws RuntimeException {
      // Unexpected as this visitor is visited pre-materialization, so only FunctionCall is expected.
      throw new UnsupportedOperationException(String.format(
        "Unexpected Internal Error: A function holder expression `%s` detected pre-materialization", holder.getName()));
    }

    @Override
    public LogicalExpression visitBooleanOperator(BooleanOperator op, Void v) {
      List<LogicalExpression> args = new ArrayList<>();
      for (int i = 0; i < op.args.size(); ++i) {
        LogicalExpression newExpr = op.args.get(i).accept(this, null);
        args.add(newExpr);
      }
      return new BooleanOperator(op.getName(), args);
    }

    @Override
    public LogicalExpression visitInputReference(InputReference sideExpr, Void value) throws RuntimeException {
      if (sideExpr.getInputOrdinal() == 1) {
        // convert build side keys to equivalent probe side for VECTORIZED_GENERIC as build side keys are not
        // in the build side hyper container.
        String probeSide = build2ProbeKeys.get(sideExpr.getReference().getAsUnescapedPath());
        if (probeSide != null) {
          sideExpr = new InputReference(0, new FieldReference(probeSide));
        }
      }
      final InputSide input = inputFunction.apply(sideExpr.getInputOrdinal());
      final MappingSet orig = generator.getMappingSet();
      generator.setMappingSet(input.mappingSet);
      try {
        TypedFieldId tfId = FieldIdUtil2.getFieldId(input.schema, sideExpr.getReference());
        if (tfId == null) {
          throw UserException.validationError().message("Unable to find the referenced field: [%s].",
            sideExpr.getReference().getAsUnescapedPath()).buildSilently();
        }
        HoldingContainer container = generator.addExpr(new ValueVectorReadExpression(tfId),
          ClassGenerator.BlockCreateMode.MERGE, false);
        return new HoldingContainerExpression(container);
      } finally {
        generator.setMappingSet(orig);
      }
    }

    @Override
    public LogicalExpression visitFunctionCall(FunctionCall call, Void v) {
      List<LogicalExpression> args = new ArrayList<>();
      for (int i = 0; i < call.args.size(); ++i) {
        LogicalExpression newExpr = call.args.get(i).accept(this, null);
        args.add(newExpr);
      }

      // replace with a new function call, since its argument could be changed.
      return new FunctionCall(call.getName(), args);
    }

    @Override
    public LogicalExpression visitIfExpression(IfExpression ifExpr, Void v) {
      final IfExpression.IfCondition oldConditions = ifExpr.ifCondition;
      final LogicalExpression newCondition = oldConditions.condition.accept(this, null);
      final LogicalExpression newExpr = oldConditions.expression.accept(this, null);
      LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, null);
      IfExpression.IfCondition condition = new IfExpression.IfCondition(newCondition, newExpr);
      return IfExpression.newBuilder()
        .setElse(newElseExpr)
        .setIfCondition(condition)
        .setOutputType(ifExpr.outputType)
        .build();
    }

    @Override
    public LogicalExpression visitCaseExpression(CaseExpression caseExpression, Void value) throws RuntimeException {
      List<CaseExpression.CaseConditionNode> caseConditions = new ArrayList<>();
      for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
        caseConditions.add(new CaseExpression.CaseConditionNode(
          conditionNode.whenExpr.accept(this, value),
          conditionNode.thenExpr.accept(this, value)));
      }
      LogicalExpression elseExpr = caseExpression.elseExpr.accept(this, value);
      return CaseExpression.newBuilder().setCaseConditions(caseConditions).setElseExpr(elseExpr).build();
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, Void v) {
      return path;
    }

    @Override
    public LogicalExpression visitConvertExpression(ConvertExpression e, Void v) {
      return new ConvertExpression(e.getConvertFunction(), e.getEncodingType(), e.getInput().accept(this, null));
    }

    @Override
    public LogicalExpression visitCastExpression(CastExpression e, Void v) {
      return new CastExpression(e.getInput().accept(this, null), e.retrieveMajorType());
    }
  }

  @Override
  public void close() throws Exception {
  }
}
