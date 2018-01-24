/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.namespace.NamespaceServiceImpl;

/**
 * ReIndex Namespace to add unquoted schema parts.
 *
 */
public class ReIndexDatasets extends UpgradeTask {

  public ReIndexDatasets() {
    super("ReIndex stores", VERSION_120, VERSION_140);
    }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    LocalKVStoreProvider localKVStoreProvider = (LocalKVStoreProvider)(context.getKVStoreProvider().get());
    reIndex(localKVStoreProvider, NamespaceServiceImpl.DAC_NAMESPACE);
  }

  private void reIndex(LocalKVStoreProvider localKVStoreProvider, String storeId) {
    System.out.printf("ReIndexing %s, ", storeId);
    int elementsReIndexed = localKVStoreProvider.reIndex(storeId);
    System.out.printf("%d Elements ReIndexed. \r\n", elementsReIndexed);
  }
}
