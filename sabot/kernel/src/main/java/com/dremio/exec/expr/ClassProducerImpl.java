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

import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.common.collections.Tuple;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.EndPointListProvider;
import com.dremio.exec.store.EndPointListProviderImpl;
import com.dremio.exec.store.PartitionExplorer;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.CompilationOptions;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ClassProducerImpl implements ClassProducer {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassProducerImpl.class);

  private final CompilationOptions compilationOptions;
  private final CodeCompiler compiler;
  private final FunctionLookupContext functionLookupContext;
  private final FunctionContext functionContext;
  private final ContextInformation contextInformation;
  private final BufferManager bufferManager;
  private final List<MinorFragmentEndpoint> minorFragmentEndpoints;

  public ClassProducerImpl(
      CompilationOptions compilationOptions,
      CodeCompiler compiler,
      FunctionLookupContext functionLookupContext,
      ContextInformation contextInformation,
      BufferManager bufferManager,
      List<MinorFragmentEndpoint> minorFragmentEndpoints) {
    this.compilationOptions = compilationOptions;
    this.compiler = compiler;
    this.functionLookupContext = functionLookupContext;
    this.contextInformation = contextInformation;
    this.bufferManager = bufferManager;
    this.functionContext = new ProducerFunctionContext();
    this.minorFragmentEndpoints = minorFragmentEndpoints;
  }

  @Override
  public <T> CodeGenerator<T> createGenerator(TemplateClassDefinition<T> definition) {
    return CodeGenerator.get(definition, compiler, functionContext);
  }

  @Override
  public LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(expr, batch != null ? batch.getSchema() : null, collector, functionLookupContext, false);
    }
  }

  @Override
  public LogicalExpression materializeWithBatchSchema(LogicalExpression expr, BatchSchema batchSchema) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(expr, batchSchema, collector, functionLookupContext, false);
    }
  }

  @Override
  public LogicalExpression materializeAndAllowComplex(LogicalExpression expr, VectorAccessible batch) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(expr, batch != null ? batch.getSchema() : null, collector, functionLookupContext, true);
    }
  }

  /**
   * ONLY for Projector and Filter to use for setting up code generation to follow.
   */
  @Override
  public Tuple<LogicalExpression, LogicalExpression> materializeAndAllowComplex(ExpressionEvaluationOptions options, LogicalExpression expr, VectorAccessible batch) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.materialize(options, expr, batch != null ? batch.getSchema() : null, collector, functionLookupContext, true);
    }
  }

  @Override
  public LogicalExpression addImplicitCast(LogicalExpression fromExpr, CompleteType toType) {
    try(ErrorCollector collector = new ErrorCollectorImpl()){
      return ExpressionTreeMaterializer.addImplicitCastExact(fromExpr, toType,
        functionLookupContext, collector, false);
    }

  }

  @Override
  public FunctionContext getFunctionContext() {
    return functionContext;
  }

  @Override
  public FunctionLookupContext getFunctionLookupContext() {
    return functionLookupContext;
  }

  public class ProducerFunctionContext implements FunctionContext {
    /** Stores constants and their holders by type */
    private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;
    /** Stores error contexts registered with this function context **/
    int nextErrorContextId = 0;
    private final List<FunctionErrorContext> errorContexts;

    public ProducerFunctionContext()
    {
      this.constantValueHolderCache = Maps.newHashMap();
      this.errorContexts = Lists.newArrayList();
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
    public BufferManager getBufferManager() {
      return bufferManager;
    }

    @Override
    public PartitionExplorer getPartitionExplorer() {
      throw UserException.unsupportedError().message("The partition explorer interface can only be used " +
        "in functions that can be evaluated at planning time. Make sure that the %s configuration " +
        "option is set to true.", PlannerSettings.CONSTANT_FOLDING.getOptionName()).build(logger);
    }

    @Override
    public EndPointListProvider getEndPointListProvider() {
      return new EndPointListProviderImpl(minorFragmentEndpoints);
    }

    @Override
    public OptionResolver getOptions() {
      return compilationOptions.getOptionResolver();
    }

    @Override
    public int registerFunctionErrorContext(FunctionErrorContext errorContext) {
      assert errorContexts.size() == nextErrorContextId;
      errorContexts.add(errorContext);
      errorContext.setId(nextErrorContextId);
      nextErrorContextId++;
      return errorContext.getId();
    }

    @Override
    public FunctionErrorContext getFunctionErrorContext(int errorContextId) {
      if (0 <= errorContextId && errorContextId <= errorContexts.size()) {
        return errorContexts.get(errorContextId);
      }
      throw new IndexOutOfBoundsException(String.format("Attempted to access error context ID %d. Max = %d",
        errorContextId, errorContexts.size()));
    }

    @Override
    public FunctionErrorContext getFunctionErrorContext() {
      // Dummy context. TODO (DX-9622): remove this method once we handle the function interpretation in the planning phase
      return FunctionErrorContextBuilder.builder().build();
    }

    @Override
    public int getFunctionErrorContextSize() {
      return errorContexts.size();
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

    @Override
    public CompilationOptions getCompilationOptions() {
      return compilationOptions;
    }
  }
}
