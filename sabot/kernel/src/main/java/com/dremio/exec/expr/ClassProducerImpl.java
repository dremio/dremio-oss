/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.Map;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.PartitionExplorer;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import io.netty.buffer.ArrowBuf;

public class ClassProducerImpl implements ClassProducer {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassProducerImpl.class);

  private final CodeCompiler compiler;
  private final FunctionLookupContext functionLookupContext;
  private final FunctionContext functionContext;
  private final ContextInformation contextInformation;
  private final BufferManager bufferManager;

  public ClassProducerImpl(
      CodeCompiler compiler,
      FunctionLookupContext functionLookupContext,
      ContextInformation contextInformation,
      BufferManager bufferManager) {
    this.compiler = compiler;
    this.functionLookupContext = functionLookupContext;
    this.contextInformation = contextInformation;
    this.bufferManager = bufferManager;
    this.functionContext = new ProducerFunctionContext();
  }

  @Override
  public <T> CodeGenerator<T> createGenerator(TemplateClassDefinition<T> definition) {
    return CodeGenerator.get(definition, compiler);
  }

  @Override
  public LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(expr, batch != null ? batch.getSchema() : null, collector, functionLookupContext, false);
    }
  }

  @Override
  public LogicalExpression materializeAndAllowComplex(LogicalExpression expr, VectorAccessible batch) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(expr, batch != null ? batch.getSchema() : null, collector, functionLookupContext, true);
    }
  }

  @Override
  public LogicalExpression addImplicitCast(LogicalExpression fromExpr, CompleteType toType) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.addImplicitCastExact(fromExpr, toType, functionLookupContext, collector);
    }

  }

  @Override
  public FunctionContext getFunctionContext() {
    return functionContext;
  }

  public class ProducerFunctionContext implements FunctionContext {
    /** Stores constants and their holders by type */
    private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;

    public ProducerFunctionContext() {
      this.constantValueHolderCache = Maps.newHashMap();
    }

    @Override
    public ContextInformation getContextInformation() {
      return contextInformation;
    }

    @Override
    public ArrowBuf getManagedBuffer() {
      return bufferManager.getManagedBuffer();
    }

    @Override
    public PartitionExplorer getPartitionExplorer() {
      throw UserException.unsupportedError().message("The partition explorer interface can only be used " +
          "in functions that can be evaluated at planning time. Make sure that the %s configuration " +
          "option is set to true.", PlannerSettings.CONSTANT_FOLDING.getOptionName()).build(logger);
    }

    @Override
    public ValueHolder getConstantValueHolder(String value, MinorType type,
        Function<ArrowBuf, ValueHolder> holderInitializer) {
      if (!constantValueHolderCache.containsKey(value)) {
        constantValueHolderCache.put(value, Maps.<MinorType, ValueHolder>newHashMap());
      }

      Map<MinorType, ValueHolder> holdersByType = constantValueHolderCache.get(value);
      ValueHolder valueHolder = holdersByType.get(type);
      if (valueHolder == null) {
        valueHolder = holderInitializer.apply(getManagedBuffer());
        holdersByType.put(type, valueHolder);
      }
      return valueHolder;
    }
  }
}
