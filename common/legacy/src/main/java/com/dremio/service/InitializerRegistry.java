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
package com.dremio.service;

import com.dremio.common.DeferredException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Set;

/** The initializer service starts */
public class InitializerRegistry implements Service {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(InitializerRegistry.class);

  private final ScanResult scanResult;
  private final BindingProvider provider;

  private ImmutableMap<Class<?>, Object> outputs;

  public InitializerRegistry(ScanResult scanResult, BindingProvider provider) {
    this.scanResult = scanResult;
    this.provider = provider;
  }

  @SuppressWarnings("unchecked")
  public <T, X extends Initializer<T>> T get(Class<X> clazz) {
    Object obj = outputs.get(clazz);
    Preconditions.checkNotNull(
        obj, "No initializer with return value registered for %s.", clazz.getName());

    // this is safe since we guaranteed insertion information via the signature.
    return (T) obj;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void start() throws Exception {
    ImmutableMap.Builder builder = ImmutableMap.<Class<?>, Object>builder();
    final Set<Class<? extends Initializer>> functions =
        scanResult.getImplementations(Initializer.class);

    DeferredException ex = new DeferredException();
    for (Class<? extends Initializer> functionClass : functions) {
      final Initializer i;
      try {
        i = functionClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        ex.addException(
            new RuntimeException("Unable to load Initializer " + functionClass.getSimpleName(), e));
        continue;
      }

      try {
        Object inited = i.initialize(provider);
        if (inited != null) {
          builder.put(functionClass, inited);
        }
      } catch (Exception e) {
        ex.addException(
            new RuntimeException(
                "Unable to initialize Initializer " + functionClass.getSimpleName(), e));
      }
    }

    // fail startup if we didn't initialize successfully.
    ex.throwAndClear();
    outputs = builder.build();
  }

  @Override
  public void close() throws Exception {}
}
