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
package com.dremio.sabot.op.join.nlje;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;

import javax.inject.Named;

import org.apache.arrow.vector.complex.FieldIdUtil2;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.ConvertExpression;
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
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

import io.netty.util.internal.PlatformDependent;

/**
 * Generates and holds an iteration of matches
 *
 * This object is responsible for generating lists of matches between the provided records.
 *
 * NOTE: this is visible for compilation purposes, otherwise it would be package private.
 */
public abstract class MatchGenerator implements AutoCloseable {

  private static TemplateClassDefinition<MatchGenerator> TEMPLATE_DEFINITION = new TemplateClassDefinition<MatchGenerator>(MatchGenerator.class, MatchGenerator.class);

  private MatchedVector probeMatchVector;
  private boolean maintainMatches;

  public void setup(
      Optional<MatchedVector> probeMatchVector,
      FunctionContext context,
      VectorAccessible probeBatch,
      VectorAccessible buildBatch,
      int outputCapacity
      ) throws Exception {
    this.maintainMatches = probeMatchVector.isPresent();
    if(maintainMatches) {
      this.probeMatchVector = probeMatchVector.get();
    }
    doSetup(context, probeBatch, buildBatch);
  }

  public void clearProbeValidity(int size) {
    if(probeMatchVector != null) {
      probeMatchVector.zero(size);
    }
  }

  public MatchedVector getProbeMatchVector() {
    return probeMatchVector;
  }

  /**
   * Attempt to match records from build and probe sides.
   * @param probeStart
   * @param probeEnd
   * @param buildBatchId
   * @param buildBatchCount
   * @return
   */
  public VectorRange tryMatch(DualRange input, VectorRange output) {
    if(input.isIndexRange()) {
      return tryMatch(input.asIndexRange(), output);
    } else {
      return tryMatch(input.asVectorRange(), output);
    }
  }

  private VectorRange tryMatch(VectorRange input, VectorRange output) {
    final long buildOutputAddr = output.getBuildOffsets4();
    final long probeOutputAddr = output.getProbeOffsets2();
    long buildInputAddr = input.getBuildOffsets4();
    long probeInputAddr = input.getProbeOffsets2();
    final MatchedVector probeMatchVector = this.probeMatchVector;
    final boolean maintainMatches = this.maintainMatches;
    int outputIndex = 0;
    final int count = input.getCurrentOutputCount();
    final IntRange currentInput = input.getCurrentOutputRange();
    final int end = currentInput.end;
    for (int i = currentInput.start; i < end; i++) {
      final int compoundBuildIndex = PlatformDependent.getInt(buildInputAddr + i * 4);
      final short probeIndex = PlatformDependent.getShort(probeInputAddr + i * 2);
      if (doEval(probeIndex, compoundBuildIndex)) {

        if (maintainMatches) {
          // mark matching probe keys.
          probeMatchVector.mark(probeIndex);
        }

        // write output indices
        VectorRange.set(probeOutputAddr, buildOutputAddr, outputIndex, probeIndex, compoundBuildIndex);
        // increment output.
        outputIndex++;
      }
    }

    return output.resetRecordsFound(outputIndex);
  }

  private VectorRange tryMatch(IndexRange offsetState, VectorRange output) {
    final long buildOutputAddr = output.getBuildOffsets4();
    final long probeOutputAddr = output.getProbeOffsets2();
    final int probeStart = offsetState.getProbeStart();
    final int probeEnd = offsetState.getProbeEnd();
    final int buildBatchIndex = offsetState.getBuildBatchIndex();
    final int buildBatchCount = offsetState.getBuildBatchCount();

    final MatchedVector probeMatchVector = this.probeMatchVector;
    final boolean maintainMatches = this.maintainMatches;
    int outputIndex = 0;
    for (int probeIndex = probeStart; probeIndex < probeEnd; probeIndex++) {
      for(int buildIndex = 0; buildIndex < buildBatchCount; buildIndex++) {
        int compoundBuildIndex = (buildBatchIndex << 16) | (buildIndex & 65535);
        if(doEval(probeIndex, compoundBuildIndex)) {

          if (maintainMatches) {
            // mark matching probe keys.
            probeMatchVector.mark(probeIndex);
          }

          // write output indices
          VectorRange.set(probeOutputAddr, buildOutputAddr, outputIndex, (short) probeIndex, compoundBuildIndex);

          // increment output.
          outputIndex++;
        }
      }
    }

    return output.resetRecordsFound(outputIndex);
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
   * Generate a match holder
   * @param expr The join condition, rooted with InputReferences. Null if you want a cartesian join.
   * @param classProducer Tool for class generation.
   * @param probe The probe side of the expression (single container).
   * @param build The build side of the expression (hyper vector).
   * @return The generated MatchHolder object.
   */
  public static MatchGenerator generate(LogicalExpression expr, ClassProducer classProducer, VectorAccessible probe, VectorAccessible build) {
    final MappingSet probeMappingSet = new MappingSet("probeIndex", null, "probeVectorAccessible", null, ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet buildMappingSet = new MappingSet("buildIndex", null, "buildVectorAccessible", null, ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

    CodeGenerator<MatchGenerator> cg = classProducer.createGenerator(TEMPLATE_DEFINITION);
    ClassGenerator<MatchGenerator> g = cg.getRoot();

    ReferenceMaterializer referenceMaterializer = new ReferenceMaterializer(g, (i) -> {
      switch(i) {
      case 1:
        return new InputSide(buildMappingSet, build.getSchema());
      case 0:
        return new InputSide(probeMappingSet, probe.getSchema());
      default:
        throw new UnsupportedOperationException("Unknown input reference " + i);
      }
    });

    if(expr == null) {
      g.getEvalBlock()._return(JExpr.TRUE);
      return cg.getImplementationClass();
    }

    // first, we rewrite the evaluation stack for each side of the comparison.
    final LogicalExpression materialized = classProducer.materialize(expr.accept(referenceMaterializer, null), null);

    // then we materialize the remaining tree.
    final HoldingContainer out = g.addExpr(materialized, ClassGenerator.BlockCreateMode.MERGE, false);


    // return a true if the condition is positive.
    g.getEvalBlock()._if(out.getIsSet().eq(JExpr.lit(1)).cand(out.getValue().eq(JExpr.lit(1))))._then()._return(JExpr.TRUE);

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

    public ReferenceMaterializer(ClassGenerator<?> generator, IntFunction<InputSide> inputFunction) {
      super();
      this.generator = generator;
      this.inputFunction = inputFunction;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, Void v) {
      return e;
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder,
        Void v) throws RuntimeException {
      throw new UnsupportedOperationException();
    }

    @Override
    public LogicalExpression visitBooleanOperator(BooleanOperator op, Void v) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < op.args.size(); ++i) {
        LogicalExpression newExpr = op.args.get(i).accept(this, null);
        args.add(newExpr);
      }
      return new BooleanOperator(op.getName(), args);
    }

    @Override
    public LogicalExpression visitInputReference(InputReference sideExpr, Void value) throws RuntimeException {
      final InputSide input = inputFunction.apply(sideExpr.getInputOrdinal());
      final MappingSet orig = generator.getMappingSet();
      generator.setMappingSet(input.mappingSet);
      try {
        TypedFieldId tfId = FieldIdUtil2.getFieldId(input.schema, sideExpr.getReference());
        if (tfId == null) {
          throw UserException.validationError().message("Unable to find the referenced field: [%s].", sideExpr.getReference().getAsUnescapedPath()).buildSilently();
        }
        HoldingContainer container = generator.addExpr(new ValueVectorReadExpression(tfId), ClassGenerator.BlockCreateMode.MERGE, false);
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
      return IfExpression.newBuilder().setElse(newElseExpr).setIfCondition(condition).setOutputType(ifExpr.outputType).build();
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
    AutoCloseables.close(probeMatchVector);
  }
}
