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
package com.dremio.dac.cmd.upgrade;

import java.util.stream.StreamSupport;

import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreStoreProviderImpl;
import com.dremio.datastore.LocalKVStoreProvider;
import com.google.common.base.Preconditions;

/**
 * Re-indexes all stores that are indexed stores.
 */
public class ReIndexAllStores extends UpgradeTask {

  public ReIndexAllStores() {
    super("Re index all stores", VERSION_106, VERSION_210);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    Preconditions.checkArgument(context.getKVStoreProvider().get() instanceof LocalKVStoreProvider);
    final LocalKVStoreProvider localStore = LocalKVStoreProvider.class.cast(context.getKVStoreProvider().get());

    StreamSupport.stream(localStore.spliterator(), false)
        .filter(storeWithId -> storeWithId.getStore() instanceof CoreIndexedStore)
        .map(CoreStoreProviderImpl.StoreWithId::getId)
        .forEach(localStore::reIndex);
  }
}
