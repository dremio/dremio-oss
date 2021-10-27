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
package com.dremio.exec.compile;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.ExpressionEvalInfo;
import com.dremio.options.OptionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CodeCompiler {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeCompiler.class);

  private final ClassTransformer transformer;
  private final ClassCompilerSelector selector;
  private final LoadingCache<CodeGenerator<?>, GeneratedClassEntry> generatedCodeToCompiledClazzCache;
  private final LoadingCache<ExpressionEvalInfosHolder, GeneratedClassEntryWithFunctionErrorContextSizeInfo> expressionsToCompiledClazzCache;

  public CodeCompiler(final SabotConfig config, final OptionManager optionManager) {
    transformer = new ClassTransformer(optionManager);
    selector = new ClassCompilerSelector(config, optionManager);
    final int cacheMaxSize = config.getInt(ExecConstants.MAX_LOADING_CACHE_SIZE_CONFIG);
    generatedCodeToCompiledClazzCache = CacheBuilder.newBuilder()
        .softValues()
        .maximumSize(cacheMaxSize)
        .build(new GeneartedCodeToCompiledClazzCacheLoader());
    expressionsToCompiledClazzCache = CacheBuilder.newBuilder()
      .softValues()
      .maximumSize(cacheMaxSize)
      .build(new ExpressionsToCompiledClazzCacheLoader());
  }

  @SuppressWarnings("unchecked")
  public <T> T getImplementationClass(final CodeGenerator<?> cg) {
    return (T) getImplementationClass(cg, 1).get(0);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getImplementationClass(final CodeGenerator<?> cg, int instanceNumber) {
    try {
       if (cg.getRoot().getExpressionEvalInfos().size() > 0) {
        if(cg.getRoot().doesLazyExpsContainComplexWriterFunctionHolder()) {
          cg.getRoot().evaluateAllLazyExps();
        } else {
          ExpressionEvalInfosHolder expressionEvalInfoHolder = new ExpressionEvalInfosHolder(cg);
          return getImplementationClassFromExpToCompiledClazzCache(expressionEvalInfoHolder, instanceNumber);
        }
      }
      cg.generate();
      final GeneratedClassEntry ce = generatedCodeToCompiledClazzCache.get(cg);
      return getInstances(instanceNumber, ce);
    } catch (ExecutionException | InstantiationException | IllegalAccessException | IOException e) {
      throw new ClassTransformationException(e);
    }
  }

  public <T> List<T> getImplementationClassFromExpToCompiledClazzCache(final ExpressionEvalInfosHolder expressionEvalInfosHolder, int instanceNumber) {
    try {
      ClassGenerator<?> rootGenerator = expressionEvalInfosHolder.cg.getRoot();
      final GeneratedClassEntryWithFunctionErrorContextSizeInfo ce = expressionsToCompiledClazzCache.get(expressionEvalInfosHolder);
      rootGenerator.registerFunctionErrorContext(ce.functionErrorContextsCount);
      expressionEvalInfosHolder.cg = null;
      return getInstances(instanceNumber, ce.generatedClassEntry);
    } catch (ExecutionException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException(e);
    }

  }

  private <T> List<T> getInstances(int instanceNumber, GeneratedClassEntry ce) throws InstantiationException, IllegalAccessException {
    List<T> tList = Lists.newArrayList();
    for (int i = 0; i < instanceNumber; i++) {
      tList.add((T) ce.clazz.newInstance());
    }
    return tList;
  }

  @VisibleForTesting
  public void invalidateExpToCompiledClazzCache() {
    this.expressionsToCompiledClazzCache.invalidateAll();
  }

  private class ExpressionsToCompiledClazzCacheLoader extends CacheLoader<ExpressionEvalInfosHolder, GeneratedClassEntryWithFunctionErrorContextSizeInfo> {
    @Override
    public GeneratedClassEntryWithFunctionErrorContextSizeInfo load(final ExpressionEvalInfosHolder expressionEvalInfosHolder) throws Exception {
      final QueryClassLoader loader = new QueryClassLoader(selector);
      ClassGenerator<?> rootGenerator = expressionEvalInfosHolder.cg.getRoot();
      CodeGenerator<?> cg = expressionEvalInfosHolder.cg;
      cg.getRoot().evaluateAllLazyExps();
      cg.generate();
      final Class<?> c = transformer.getImplementationClass(loader, cg.getDefinition(), cg.getGeneratedCode(), cg.getMaterializedClassName());
      return new GeneratedClassEntryWithFunctionErrorContextSizeInfo(c, rootGenerator.getFunctionErrorContextsCount());
    }
  }

  private class GeneartedCodeToCompiledClazzCacheLoader extends CacheLoader<CodeGenerator<?>, GeneratedClassEntry> {
    @Override
    public GeneratedClassEntry load(final CodeGenerator<?> cg) throws Exception {
      final QueryClassLoader loader = new QueryClassLoader(selector);
      final Class<?> c = transformer.getImplementationClass(loader, cg.getDefinition(),
          cg.getGeneratedCode(), cg.getMaterializedClassName());
      return new GeneratedClassEntry(c);
    }
  }

  private class GeneratedClassEntry {
    private final Class<?> clazz;

    public GeneratedClassEntry(final Class<?> clazz) {
      this.clazz = clazz;
    }
  }

  private class GeneratedClassEntryWithFunctionErrorContextSizeInfo {
    private final GeneratedClassEntry generatedClassEntry;
    private final int functionErrorContextsCount;

    private GeneratedClassEntryWithFunctionErrorContextSizeInfo(final Class<?> clazz, int functionErrorContextCount) {
      this.generatedClassEntry = new GeneratedClassEntry(clazz);
      this.functionErrorContextsCount = functionErrorContextCount;
    }
  }

  private class ExpressionEvalInfosHolder {
    private final List<ExpressionEvalInfo> expressionEvalInfos;
    //This is only used once and we can make it null after that and also this is not used in the equals method,
    // hence keeping it non final and mutable, but rest of the fields are and should be immutable
    private CodeGenerator<?> cg;

    public ExpressionEvalInfosHolder(CodeGenerator<?> cg) {
      List<ExpressionEvalInfo> expEvalInfos = cg.getRoot().getExpressionEvalInfos();
      if (expEvalInfos == null) {
        expEvalInfos = Collections.emptyList();
      } else {
        expEvalInfos = ImmutableList.copyOf(expEvalInfos);
      }
      this.expressionEvalInfos = expEvalInfos;
      this.cg = cg;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || this.getClass() != o.getClass()) {
        return false;
      }

      ExpressionEvalInfosHolder that = (ExpressionEvalInfosHolder) o;
      return Objects.equals(expressionEvalInfos, that.expressionEvalInfos) ;
    }

    @Override
    public int hashCode() {
      return Objects.hash(expressionEvalInfos);
    }
  }
}
