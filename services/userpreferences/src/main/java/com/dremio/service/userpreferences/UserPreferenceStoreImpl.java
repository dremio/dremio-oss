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

package com.dremio.service.userpreferences;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.userpreferences.proto.UserPreferenceProto.UserPreference;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Provider;

/** UserPreferenceStore implementation */
public class UserPreferenceStoreImpl implements UserPreferenceStore {

  private static final String STORE_NAME = "user_preference";
  private static final IndexKey USER_ID =
      IndexKey.newBuilder("userId", "USER_ID", String.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
          .build();
  private final Provider<KVStoreProvider> kvStoreProvider;
  private Supplier<IndexedStore<String, UserPreference>> store;

  @Inject
  public UserPreferenceStoreImpl(Provider<KVStoreProvider> kvStoreProvider) {
    Preconditions.checkNotNull(kvStoreProvider, "store provider cannot be null.");
    this.kvStoreProvider = kvStoreProvider;
  }

  @Override
  public void start() throws Exception {
    store =
        Suppliers.memoize(
            () -> kvStoreProvider.get().getStore(UserPreferenceStoreImpl.StoreCreator.class));
  }

  @Override
  public void close() throws Exception {}

  @Override
  public Optional<UserPreference> get(String userId) {
    Document<String, UserPreference> doc = store.get().get(userId);
    if (doc == null) {
      return Optional.empty();
    }
    return Optional.of(doc.getValue());
  }

  @Override
  public UserPreference update(String userId, UserPreference userPreference) {
    Preconditions.checkNotNull(userId, "userId must not be null.");
    Preconditions.checkNotNull(userPreference, "userPreference must not be null.");
    Document<String, UserPreference> doc = store.get().put(userId, userPreference);
    return doc.getValue();
  }

  /** StoreCreator */
  public static final class StoreCreator
      implements IndexedStoreCreationFunction<String, UserPreference> {

    @Override
    public IndexedStore<String, UserPreference> build(StoreBuildingFactory factory) {
      return factory
          .<String, UserPreference>newStore()
          .name(STORE_NAME)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(UserPreference.class))
          .buildIndexed(new UserPreferenceDocumentConverter());
    }
  }

  static final class UserPreferenceDocumentConverter
      implements DocumentConverter<String, UserPreference> {

    private final Integer version = 0;

    @Override
    public void convert(DocumentWriter writer, String key, UserPreference record) {
      writer.write(USER_ID, record.getUserId());
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }
}
