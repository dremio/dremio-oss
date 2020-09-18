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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * This class is responsible for truncating varchar column values to match
 * with column length specified in Hive schema.
 * This class sets up and runs projector on each fixed-length varchar field
 *  */
public class HiveVarcharTruncationReader implements AutoCloseable {
  int truncLen;
  Field field;
  NamedExpression castExpr;
  NamedExpression noCastExpr;
  ExpressionSplitter splitter; // used for truncating varchar
  TransferPair transferPair; // used for direct transfer
  private Stopwatch varcharCheckCastWatch = Stopwatch.createUnstarted();

  public HiveVarcharTruncationReader(Field field,
                                     TypeCoercion typeCoercion) {
    this.field = field;
    TypeProtos.MajorType majorType = typeCoercion.getType(field);
    FieldReference inputRef = FieldReference.getWithQuotedRef(field.getName());
    this.truncLen = majorType.getWidth();
    this.castExpr = new NamedExpression(FunctionCallFactory.createCast(majorType, inputRef), inputRef);
    this.noCastExpr = new NamedExpression(inputRef, inputRef);
  }

  public void setupProjector(OperatorContext context,
                             Stopwatch javaCodeGenWatch,
                             Stopwatch gandivaCodeGenWatch,
                             VectorContainer incoming,
                             ExpressionEvaluationOptions projectorOptions, VectorContainer projectorOutput) {

    try {
      BatchSchema targetSchema = BatchSchema.newBuilder().addField(field).build();

      ExpressionSplitter varcharSplitter = ProjectOperator.createSplitterWithExpressions(incoming, Collections.singletonList(castExpr),
        null, null, null, context, projectorOptions, projectorOutput, targetSchema);
      varcharSplitter.setupProjector(projectorOutput, javaCodeGenWatch, gandivaCodeGenWatch);
      this.splitter = varcharSplitter;

      IntHashSet transferFieldIds = new IntHashSet();
      List<TransferPair> transfers = Lists.newArrayList();
      ProjectOperator.createSplitterWithExpressions(incoming, Collections.singletonList(noCastExpr),
        transfers, null, transferFieldIds, context, projectorOptions, projectorOutput, targetSchema);
      if (!transfers.isEmpty()) {
        this.transferPair = transfers.get(0);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void runProjector(BaseVariableWidthVector vector, int recordCount,
                           OperatorContext context,
                           Stopwatch javaCodeGenWatch,
                           Stopwatch gandivaCodeGenWatch) throws Exception {
    if (transferPair == null) {
      return;
    }

    if (castRequired(vector, recordCount, truncLen)) {
      splitter.projectRecords(recordCount, javaCodeGenWatch, gandivaCodeGenWatch);
      context.getStats().addLongStat(ScanOperator.Metric.TOTAL_HIVE_PARQUET_TRUNCATE_VARCHAR, 1);
    } else {
      javaCodeGenWatch.start();
      transferPair.transfer();
      javaCodeGenWatch.stop();
      context.getStats().addLongStat(ScanOperator.Metric.TOTAL_HIVE_PARQUET_TRANSFER_VARCHAR, 1);
    }
    context.getStats().addLongStat(ScanOperator.Metric.HIVE_PARQUET_CHECK_VARCHAR_CAST_TIME_NS,
      varcharCheckCastWatch.elapsed(TimeUnit.NANOSECONDS));
    varcharCheckCastWatch.reset();
  }

  private boolean castRequired(BaseVariableWidthVector vector, int recordCount, int truncLen) {
    varcharCheckCastWatch.start();
    if (vector.getNullCount() == recordCount) {
      varcharCheckCastWatch.stop();
      return false;
    }
    for (int i = 0; i < recordCount; ++i) {
      if (vector.getValueLength(i) > truncLen) {
        varcharCheckCastWatch.stop();
        return true;
      }
    }
    varcharCheckCastWatch.stop();
    return false;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(splitter);
  }
}

