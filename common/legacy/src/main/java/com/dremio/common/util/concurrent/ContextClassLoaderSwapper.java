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
package com.dremio.common.util.concurrent;

import com.dremio.common.util.Closeable;

/** Swaps current thread's class loader with given class loader and restore back when closed */
public final class ContextClassLoaderSwapper implements Closeable {

  private final ClassLoader originalClassLoader;

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ContextClassLoaderSwapper.class);

  private ContextClassLoaderSwapper(ClassLoader classLoader) {
    this.originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  @Override
  public void close() {
    logger.debug("Loading original class loader {}", originalClassLoader);
    Thread.currentThread().setContextClassLoader(originalClassLoader);
  }

  /**
   * Method returns a new ContextClassLoaderSwapper resource. When this method returns, current
   * thread's class loader is set to class loader of the argument passed
   *
   * @param classObj Class object whose class loader should be set as class loader of current thread
   * @return a new instance of ContextClassLoaderSwapper resource
   */
  public static Closeable swapClassLoader(final Class<?> classObj) {
    logger.debug("Current class loader swapped with {} class loader", classObj.getClassLoader());
    return new ContextClassLoaderSwapper(classObj.getClassLoader());
  }
}
