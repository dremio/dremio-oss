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
package com.dremio.datastore.utility;

import java.util.Set;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.StoreCreationFunction;
import com.google.common.collect.ImmutableMap;

/**
 * Utility class to load defined KVStores.
 */
public class StoreLoader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreLoader.class);

  /**
   * Create a map of stores defined in the provided scan using the provided factory.
   * @param scan classpath scan results.
   * @param factory StoreBuildingFactory for building KVStore implementations.
   * @return a map of all kv store impls with the provided factory.
   */
  public static ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> buildStores(ScanResult scan, StoreBuildingFactory factory){
    return buildStores(scan.getImplementations(StoreCreationFunction.class), factory);
  }

  /**
   * Builds a map of all store impls using the provided factory.
   * @param impls a set of implementations of the storeCreationFunction.
   * @param factory StoreBuildingFactory for building KVStore implementations.
   * @return a map of all kv store impls with the provided factory.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> buildStores(
    Set<Class<? extends StoreCreationFunction>> impls, StoreBuildingFactory factory) {
    ImmutableMap.Builder builder = ImmutableMap.<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>>builder();

    for(Class<? extends StoreCreationFunction> functionClass : impls) {
      try {
        final KVStore<?, ?> store = functionClass.newInstance().build(factory);
        builder.put(functionClass, store);
      } catch (Exception e) {
        logger.warn("Unable to load StoreCreationFunction {}", functionClass.getSimpleName(), e);
      }
    }

    final ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> map = builder.build();
    logger.debug("Loaded the following StoreCreationFunctions: {}.", map.keySet());
    return map;
  }
}
