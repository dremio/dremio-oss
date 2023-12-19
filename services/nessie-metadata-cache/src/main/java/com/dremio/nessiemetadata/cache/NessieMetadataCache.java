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
package com.dremio.nessiemetadata.cache;

import java.util.Optional;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.transientstore.TransientStore;
import com.dremio.nessiemetadata.storeprovider.NessieMetadataCacheStoreProvider;
import com.dremio.service.Service;
import com.google.inject.Provider;

public class NessieMetadataCache implements Service {
  private TransientStore<String, String> store;
  private Provider<NessieMetadataCacheStoreProvider> storeProvider;

  public NessieMetadataCache(Provider<NessieMetadataCacheStoreProvider> storeProvider) {
    this.storeProvider = storeProvider;
  }

  public void invalidate(String userId) {
    store.delete(userId);
  }

  public Optional<String> get(String userId) {
    Document<String, String> doc = store.get(userId);
    if (doc == null) {
      return Optional.empty();
    }
    return Optional.of(doc.getValue());
  }

  public void put(String userId, String value) {
    store.put(userId, value);
  }

  @Override
  public void start() throws Exception {
    store = storeProvider.get().getStore(Format.ofString(), Format.ofString());
  }

  @Override
  public void close() throws Exception {
  }
}
