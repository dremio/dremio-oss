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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.commons.compiler.CompileException;

import com.dremio.exec.compile.ClassTransformer.ClassNames;
import com.dremio.exec.exception.ClassTransformationException;
import com.google.common.collect.MapMaker;

public class QueryClassLoader extends URLClassLoader {
  private final ClassCompilerSelector compilerSelector;

  private final AtomicLong index = new AtomicLong(0);

  private final ConcurrentMap<String, byte[]> customClasses = new MapMaker().concurrencyLevel(4).makeMap();

  public QueryClassLoader(ClassCompilerSelector classCompilerSelector) {
    super(new URL[0], Thread.currentThread().getContextClassLoader());
    this.compilerSelector = classCompilerSelector;
  }

  public long getNextClassIndex() {
    return index.getAndIncrement();
  }

  public void injectByteCode(String className, byte[] classBytes) throws IOException {
    if (customClasses.containsKey(className)) {
      throw new IOException(String.format("The class defined %s has already been loaded.", className));
    }
    customClasses.put(className, classBytes);
  }

  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    byte[] ba = customClasses.get(className);
    if (ba != null) {
      return this.defineClass(className, ba, 0, ba.length);
    }else{
      return super.findClass(className);
    }
  }

  public ClassBytes[] getClassByteCode(final ClassNames className, final String sourceCode)
      throws CompileException, IOException, ClassNotFoundException, ClassTransformationException {
    return compilerSelector.getClassByteCode(className, sourceCode);
  }
}
