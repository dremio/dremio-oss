/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.List;
import java.util.concurrent.ExecutionException;


import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.options.OptionManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

public class CodeCompiler {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeCompiler.class);

  private final ClassTransformer transformer;
  private final ClassCompilerSelector selector;
  private final LoadingCache<CodeGenerator<?>, GeneratedClassEntry> cache;

  public CodeCompiler(final SabotConfig config, final OptionManager optionManager) {
    transformer = new ClassTransformer(optionManager);
    selector = new ClassCompilerSelector(config, optionManager);
    final int cacheMaxSize = config.getInt(ExecConstants.MAX_LOADING_CACHE_SIZE_CONFIG);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .build(new Loader());
  }

  @SuppressWarnings("unchecked")
  public <T> T getImplementationClass(final CodeGenerator<?> cg) {
    return (T) getImplementationClass(cg, 1).get(0);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getImplementationClass(final CodeGenerator<?> cg, int instanceNumber) {
    try {
      cg.generate();
      final GeneratedClassEntry ce = cache.get(cg);
      List<T> tList = Lists.newArrayList();
      for ( int i = 0; i < instanceNumber; i++) {
        tList.add((T) ce.clazz.newInstance());
      }
      return tList;
    } catch (ExecutionException | InstantiationException | IllegalAccessException | IOException e) {
      throw new ClassTransformationException(e);
    }
  }

  private class Loader extends CacheLoader<CodeGenerator<?>, GeneratedClassEntry> {
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
}
