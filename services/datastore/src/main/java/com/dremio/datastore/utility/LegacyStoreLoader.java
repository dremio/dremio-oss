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
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.google.common.collect.ImmutableMap;

/**
 * Utility class to load defined LegacyKVStores.
 */
@Deprecated
public class LegacyStoreLoader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyStoreLoader.class);

  /**
   * Create a map of stores defined in the provided scan using the provided factory.
   * @param scan classpath scan results.
   * @param factory LegacyStoreBuildingFactory for building LegacyKVStore implementations.
   * @return a map of all legacy store impls with the provided legacy factory.
   */
  @Deprecated
  public static ImmutableMap<Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>>, LegacyKVStore<?, ?>> buildLegacyStores(ScanResult scan, LegacyStoreBuildingFactory factory){
    return buildLegacyStores(scan.getImplementations(LegacyStoreCreationFunction.class), factory);
  }

  /**
   * Builds a map of all legacy store impls using the provided legacy factory.
   * @param impls a set of implementations of the LegacyStoreCreationFunction.
   * @param factory the legacyStoreBuildingFactory.
   * @return a map of all legacy store impls with the provided legacy factory.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Deprecated
  public static ImmutableMap<Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>>, LegacyKVStore<?, ?>> buildLegacyStores(
    Set<Class<? extends LegacyStoreCreationFunction>> impls, LegacyStoreBuildingFactory factory) {
    ImmutableMap.Builder builder = ImmutableMap.<Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>>, LegacyKVStore<?, ?>>builder();

    for(Class<? extends LegacyStoreCreationFunction> functionClass : impls) {
      try {
        final LegacyKVStore<?, ?> store = functionClass.newInstance().build(factory);
        builder.put(functionClass, store);
      } catch (Exception e) {
        logger.warn("Unable to load StoreCreationFunction {}", functionClass.getSimpleName(), e);
      }
    }

    final ImmutableMap<Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>>, LegacyKVStore<?, ?>> map = builder.build();
    logger.debug("Loaded the following StoreCreationFunctions: {}.", map.keySet());
    return map;
  }
}
