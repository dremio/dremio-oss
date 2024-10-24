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
package com.dremio.exec.store.copyinto;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.expression.CastExpressionWithOverflow;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ConvertExpression;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.NonVarcharCoercionReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector;
import com.google.common.base.Stopwatch;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

public class CopyIntoTransformationNonVarcharCoercionReader extends NonVarcharCoercionReader {

  private final List<CopyIntoTransformationProperties.Property> transformationProperties;
  private final BatchSchema targetSchema;

  public CopyIntoTransformationNonVarcharCoercionReader(
      SampleMutator mutator,
      OperatorContext context,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      List<CopyIntoTransformationProperties.Property> transformationProperties,
      BatchSchema targetSchema,
      int depth) {
    super(
        mutator,
        context,
        originalSchema,
        typeCoercion,
        javaCodeGenWatch,
        gandivaCodeGenWatch,
        depth);
    this.transformationProperties = transformationProperties;
    this.targetSchema = targetSchema;
  }

  @Override
  protected void createCoercions(ExpressionEvaluationOptions projectorOptions) {
    for (CopyIntoTransformationProperties.Property transformationProperty :
        transformationProperties) {
      String targetColName = transformationProperty.getTargetColName();
      LogicalExpression transformationExpression =
          transformationProperty.getTransformationExpression();
      for (Field field : targetSchema.getFields()) {
        String name = field.getName();
        if (name.equalsIgnoreCase(targetColName)) {
          FieldReference outputRef = FieldReference.getWithQuotedRef(name);
          CompleteType targetType = CompleteType.fromField(field);
          if (targetType.isUnion() || targetType.isComplex()) {
            exprs.add(null);
          } else {
            addExpression(field, outputRef, transformationExpression);
          }
          break;
        }
      }
    }

    if (projectorOptions.isTrackRecordLevelErrors()) {
      targetSchema.getFields().stream()
          .filter(f -> ColumnUtils.COPY_HISTORY_COLUMN_NAME.equalsIgnoreCase(f.getName()))
          .findFirst()
          .ifPresent(
              f ->
                  exprs.add(
                      new NamedExpression(
                          new TypedNullConstant(
                              CompleteType.fromMajorType(typeCoercion.getType(f))),
                          FieldReference.getWithQuotedRef(f.getName()))));
    }
  }

  private void addExpression(Field field, FieldReference outputRef, LogicalExpression expression) {
    TypeProtos.MajorType fieldType = typeCoercion.getType(field);
    LogicalExpression cast;
    if (outputRef.getCompleteType().isText()
        && (fieldType.getMinorType().equals(MinorType.VARCHAR)
            || fieldType.getMinorType().equals(MinorType.VARBINARY))) {
      cast = expression;
    } else if (fieldType.getMinorType().equals(MinorType.DECIMAL)) {
      cast = new CastExpressionWithOverflow(expression, fieldType);
    } else if (fieldType.getMinorType().equals(MinorType.VARCHAR)
        && canConvertComplexTypeToJson(field)) {
      cast =
          FunctionCallFactory.createConvert(
              ConvertExpression.CONVERT_TO, "CompactJSON", expression);
      cast = FunctionCallFactory.createCast(fieldType, cast);
    } else {
      cast = FunctionCallFactory.createCast(fieldType, expression);
    }
    exprs.add(new NamedExpression(cast, outputRef));
  }

  @Override
  protected ExpressionSplitter initSplitter(
      List<TransferPair> transfers,
      ClassGenerator<Projector> cg,
      IntHashSet transferFieldIds,
      ExpressionEvaluationOptions projectorOptions,
      VectorContainer projectorOutput)
      throws Exception {
    projectorOptions.setCanDirectTransfer(false);
    return ProjectOperator.createSplitterWithExpressions(
        incoming,
        exprs,
        transfers,
        cg,
        transferFieldIds,
        context,
        projectorOptions,
        projectorOutput,
        null);
  }
}
