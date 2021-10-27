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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

/**
 * Coercion reader that handles only primitive type columns.
 * Internally separates input columns into varchar columns and non varchar columns.
 */
public class PrimitiveTypeReader implements AutoCloseable {
  private final OperatorContext context;
  private final Stopwatch javaCodeGenWatch;
  private final Stopwatch gandivaCodeGenWatch;
  private final Map<String, VarcharTruncationReader> fixedLenVarCharMap = CaseInsensitiveMap.newHashMap();
  private final NonVarcharCoercionReader nonVarcharCoercionReader;

  public PrimitiveTypeReader(SampleMutator mutator, OperatorContext context, TypeCoercion typeCoercion,
                             Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch, BatchSchema originalSchema) {
    this.context = context;
    this.javaCodeGenWatch = javaCodeGenWatch;
    this.gandivaCodeGenWatch = gandivaCodeGenWatch;

    // Creating a separate schema for columns that don't need varchar truncation
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (Field field : originalSchema.getFields()) {
      if (typeCoercion.isVarcharTruncationRequired(field)) {
        fixedLenVarCharMap.put(field.getName(), new VarcharTruncationReader(field, typeCoercion));
      } else {
        schemaBuilder.addField(field);
      }
    }

    BatchSchema nonVarcharSchema = schemaBuilder.build();
    nonVarcharCoercionReader = new NonVarcharCoercionReader(mutator, context,
      nonVarcharSchema, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch);
  }


  public void setupProjector(VectorContainer incoming, ExpressionEvaluationOptions projectorOptions,
                             VectorContainer projectorOutput) {
    if (!fixedLenVarCharMap.isEmpty()) {
      context.getStats().addLongStat(ScanOperator.Metric.NUM_HIVE_PARQUET_TRUNCATE_VARCHAR, fixedLenVarCharMap.size());
    }
    nonVarcharCoercionReader.setupProjector(projectorOutput, projectorOptions);
    // Setting up the projector for columns that need varchar truncation
    for (Map.Entry<String, VarcharTruncationReader> entry : fixedLenVarCharMap.entrySet()) {
      entry.getValue().setupProjector(this.context,
        javaCodeGenWatch,
        gandivaCodeGenWatch,
        incoming,
        projectorOptions,
        projectorOutput
      );
    }

    OperatorStats stats = this.context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_BUILD_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_BUILD_TIME_NS, gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();
  }

  public void runProjector(int recordCount, VectorContainer incoming) {
    try {
      nonVarcharCoercionReader.runProjector(recordCount);
      if (recordCount > 0) {
        if (!fixedLenVarCharMap.isEmpty()) {
          for (VectorWrapper<? extends ValueVector> wrapper : incoming) {
            if (fixedLenVarCharMap.containsKey(wrapper.getField().getName())) {
              VarcharTruncationReader varcharTruncationReader = fixedLenVarCharMap.get(wrapper.getField().getName());
              varcharTruncationReader.runProjector((BaseVariableWidthVector) wrapper.getValueVector(), recordCount,
                this.context, javaCodeGenWatch, gandivaCodeGenWatch);
            }
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    OperatorStats stats = this.context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_EXECUTE_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_EXECUTE_TIME_NS, gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  public void resetProjector() {
    nonVarcharCoercionReader.clearExprs();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(fixedLenVarCharMap.values());
    fixedLenVarCharMap.clear();
    AutoCloseables.close(nonVarcharCoercionReader);
  }
}
