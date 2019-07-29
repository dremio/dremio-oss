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
package com.dremio.dac.cmd.upgrade;

import java.util.stream.StreamSupport;

import com.dremio.common.Version;
import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreStoreProviderImpl;
import com.dremio.datastore.LocalKVStoreProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Re-indexes all stores that are indexed stores.
 */
public class ReIndexAllStores extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "5522c3bc-195f-41ba-8bfd-a33f91b1219a";

  public ReIndexAllStores() {
    super("Re index all stores", ImmutableList.of(DatasetConfigUpgrade.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_210;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    Preconditions.checkArgument(context.getKVStoreProvider() instanceof LocalKVStoreProvider);
    final LocalKVStoreProvider localStore = LocalKVStoreProvider.class.cast(context.getKVStoreProvider());

    StreamSupport.stream(localStore.spliterator(), false)
        .filter(storeWithId -> storeWithId.getStore() instanceof CoreIndexedStore)
        .map(CoreStoreProviderImpl.StoreWithId::getId)
        .forEach(localStore::reIndex);
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
