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
package com.dremio.datastore;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.utility.StoreLoader;
import com.google.common.collect.ImmutableMap;

/**
 * An extension of LocalKVStoreProvider which loads the stores
 * from a set of StoreCreators instead of ScanResult.
 */
public class CustomLocalKVStoreProvider extends LocalKVStoreProvider{
  private final Set<Class<? extends StoreCreationFunction>> storeCreators;
  private static final ScanResult EMPTY_SCANRESULT = new ScanResult(
    Collections.EMPTY_LIST,
    Collections.EMPTY_LIST,
    Collections.EMPTY_LIST,
    Collections.EMPTY_LIST,
    Collections.EMPTY_LIST);

  public CustomLocalKVStoreProvider(Set<Class<? extends StoreCreationFunction>> storeCreators,
                                    String baseDirectory,
                                    boolean inMemory,
                                    boolean timed){
    super(EMPTY_SCANRESULT, baseDirectory, inMemory, timed);
    this.storeCreators = storeCreators;
  }

  @Override
  protected Supplier<ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>>> getStoreProvider(){
    return () -> StoreLoader.buildStores(storeCreators, super::newStore);
  }
}
