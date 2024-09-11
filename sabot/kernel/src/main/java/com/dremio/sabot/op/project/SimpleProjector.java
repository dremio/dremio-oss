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
package com.dremio.sabot.op.project;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Given a list of expressions, inputVectors, outputVectors evaluates expressions on inputs and
 * writes output to outputVectors. Expressions should specify input, output refs
 */
public class SimpleProjector implements AutoCloseable {

  private final VectorContainer outputVectors;
  private final OperatorContext context;
  private final VectorContainer inputVectors;
  protected Projector projector;
  protected ExpressionSplitter splitter;
  protected Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();
  protected Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
  private final ExpressionEvaluationOptions projectorOptions;
  private final List<NamedExpression> exprs;

  public SimpleProjector(
      OperatorContext context,
      VectorContainer inputVectors,
      List<NamedExpression> exprs,
      VectorContainer outputVectors) {
    this.context = context;
    this.inputVectors = inputVectors;
    this.exprs = exprs;
    this.outputVectors = outputVectors;
    this.projectorOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.projectorOptions.setCodeGenOption(
        context
            .getOptions()
            .getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName())
            .getStringVal());
  }

  public void setup() {

    final ClassGenerator<Projector> cg =
        context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
    final IntHashSet transferFieldIds = new IntHashSet();
    final List<TransferPair> transfers = Lists.newArrayList();

    try {
      splitter =
          ProjectOperator.createSplitterWithExpressions(
              inputVectors,
              this.exprs,
              transfers,
              cg,
              transferFieldIds,
              context,
              projectorOptions,
              outputVectors,
              null);
      splitter.setupProjector(outputVectors, javaCodeGenWatch, gandivaCodeGenWatch);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    javaCodeGenWatch.start();
    this.projector = cg.getCodeGenerator().getImplementationClass();
    this.projector.setup(
        context.getFunctionContext(),
        inputVectors,
        outputVectors,
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
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();
  }

  public void eval(int recordCount) {
    Preconditions.checkNotNull(projector, "eval called before setup");
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
    OperatorStats stats = context.getStats();
    stats.addLongStat(
        ScanOperator.Metric.JAVA_EXECUTE_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(
        ScanOperator.Metric.GANDIVA_EXECUTE_TIME_NS,
        gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(splitter);
  }
}
