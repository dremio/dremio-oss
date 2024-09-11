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
package com.dremio.exec.store;

import static com.dremio.common.types.TypeProtos.MinorType.VARBINARY;
import static com.dremio.common.types.TypeProtos.MinorType.VARCHAR;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CastExpressionWithOverflow;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

/** This class is responsible for doing coercion of all primitive types except varchar columns */
public class NonVarcharCoercionReader implements AutoCloseable {
  protected final SampleMutator mutator;
  protected final BatchSchema targetSchema;
  protected final List<NamedExpression> exprs;
  protected final List<ValueVector> allocationVectors = Lists.newArrayList();

  protected Projector projector;
  protected VectorContainer incoming;
  protected Stopwatch javaCodeGenWatch;
  protected ExpressionSplitter splitter;
  protected Stopwatch gandivaCodeGenWatch;

  private final OperatorContext context;
  private final TypeCoercion typeCoercion;

  public NonVarcharCoercionReader(
      SampleMutator mutator,
      OperatorContext context,
      BatchSchema targetSchema,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch) {
    this.gandivaCodeGenWatch = gandivaCodeGenWatch;
    this.javaCodeGenWatch = javaCodeGenWatch;
    this.context = context;
    this.mutator = mutator;
    this.incoming = mutator.getContainer();
    this.targetSchema = targetSchema;
    this.exprs = new ArrayList<>(targetSchema.getFieldCount());
    this.typeCoercion = typeCoercion;
  }

  private boolean canConvertComplexTypeToJson(Field field) {
    return context.getOptions().getOption(ExecConstants.ENABLE_PARQUET_MIXED_TYPES_COERCION)
        && incoming
            .getSchema()
            .findFieldIgnoreCase(field.getName())
            .map(f -> f.getType().isComplex())
            .orElse(false);
  }

  protected void addExpression(
      Field field, FieldReference inputRef, ExpressionEvaluationOptions projectorOptions) {
    TypeProtos.MajorType majorType = typeCoercion.getType(field);
    if (projectorOptions.isTrackRecordLevelErrors()
        && ColumnUtils.COPY_HISTORY_COLUMN_NAME.equals(field.getName())) {
      // adding this will make Projector write errors to this column
      exprs.add(
          new NamedExpression(
              new TypedNullConstant(CompleteType.fromMajorType(majorType)), inputRef));
      return;
    }
    LogicalExpression cast;
    if (inputRef.getCompleteType().isText()
        && (majorType.getMinorType().equals(VARCHAR)
            || majorType.getMinorType().equals(VARBINARY))) {
      cast = inputRef;
    } else if (majorType.getMinorType().equals(TypeProtos.MinorType.DECIMAL)) {
      cast = new CastExpressionWithOverflow(inputRef, majorType);
    } else if (majorType.getMinorType().equals(VARCHAR) && canConvertComplexTypeToJson(field)) {
      cast =
          FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "CompactJSON", inputRef);
      cast = FunctionCallFactory.createCast(majorType, cast);
    } else {
      cast = FunctionCallFactory.createCast(majorType, inputRef);
    }
    exprs.add(new NamedExpression(cast, inputRef));
  }

  protected void createCoercions(ExpressionEvaluationOptions projectorOptions) {
    for (Field field : targetSchema.getFields()) {
      final FieldReference inputRef = FieldReference.getWithQuotedRef(field.getName());
      final CompleteType targetType = CompleteType.fromField(field);
      if (targetType.isUnion() || targetType.isComplex()) {
        // do not add any expressions for non primitive fields
        exprs.add(null);
      } else {
        addExpression(field, inputRef, projectorOptions);
      }
    }
  }

  public VectorContainer getIncoming() {
    return incoming;
  }

  public void setupProjector(
      VectorContainer projectorOutput, ExpressionEvaluationOptions projectorOptions) {
    createCoercions(projectorOptions);
    if (incoming.getSchema() == null || incoming.getSchema().getFieldCount() == 0) {
      return;
    }

    final ClassGenerator<Projector> cg =
        context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
    final IntHashSet transferFieldIds = new IntHashSet();
    final List<TransferPair> transfers = Lists.newArrayList();

    List<Integer> decimalFields = new ArrayList<>();
    long numDecimalCoercions;
    int i = 0;
    for (Field f : targetSchema.getFields()) {
      if (MajorTypeHelper.getMajorTypeForField(f)
          .getMinorType()
          .equals(TypeProtos.MinorType.DECIMAL)) {
        decimalFields.add(i);
      }
      i++;
    }

    try {
      splitter =
          ProjectOperator.createSplitterWithExpressions(
              incoming,
              exprs,
              transfers,
              cg,
              transferFieldIds,
              context,
              projectorOptions,
              projectorOutput,
              targetSchema);
      splitter.setupProjector(projectorOutput, javaCodeGenWatch, gandivaCodeGenWatch);

      numDecimalCoercions =
          decimalFields.stream().filter(id -> !transferFieldIds.contains(id)).count();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    javaCodeGenWatch.start();
    this.projector = cg.getCodeGenerator().getImplementationClass();
    this.projector.setup(
        context.getFunctionContext(),
        incoming,
        projectorOutput,
        transfers,
        name -> null,
        projectorOptions);
    javaCodeGenWatch.stop();
    OperatorStats stats = context.getStats();
    stats.addLongStat(
        ScanOperator.Metric.JAVA_BUILD_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(
        ScanOperator.Metric.GANDIVA_BUILD_TIME_NS,
        gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(ScanOperator.Metric.NUM_HIVE_PARQUET_DECIMAL_COERCIONS, numDecimalCoercions);
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();
  }

  public void runProjector(int recordCount) {
    if (projector != null) {
      try {
        if (recordCount > 0) {
          splitter.projectRecords(recordCount, javaCodeGenWatch, gandivaCodeGenWatch);
        }
        javaCodeGenWatch.start();
        projector.projectRecords(recordCount);
        javaCodeGenWatch.stop();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      for (final ValueVector v : allocationVectors) {
        v.setValueCount(recordCount);
      }
    }
    OperatorStats stats = context.getStats();
    stats.addLongStat(
        ScanOperator.Metric.JAVA_EXECUTE_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(
        ScanOperator.Metric.GANDIVA_EXECUTE_TIME_NS,
        gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  public void clearExprs() {
    exprs.clear();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(incoming, mutator, splitter);
  }
}
