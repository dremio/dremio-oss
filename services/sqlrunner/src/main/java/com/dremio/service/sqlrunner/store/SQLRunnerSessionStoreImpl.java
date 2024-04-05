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
package com.dremio.service.sqlrunner.store;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.sqlrunner.proto.SQLRunnerSessionProto.SQLRunnerSession;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import java.util.Date;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.commons.lang3.StringUtils;

public class SQLRunnerSessionStoreImpl implements SQLRunnerSessionStore {
  private static final String STORE_NAME = "sqlrunner_sessions";
  private static final IndexKey USER_ID =
      IndexKey.newBuilder("user_id", "USER_ID", String.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
          .build();
  private static final IndexKey EXPIRE_AT =
      IndexKey.newBuilder("expire_at", "EXPIRE_AT", Date.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
          .build();
  private static final IndexKey EXPIRE_AT_LONG =
      IndexKey.newBuilder("expire_at_long", "EXPIRE_AT_LONG", Long.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
          .build();

  private final Provider<KVStoreProvider> kvStoreProvider;
  private Supplier<IndexedStore<String, SQLRunnerSession>> store;

  @Inject
  public SQLRunnerSessionStoreImpl(Provider<KVStoreProvider> kvStoreProvider) {
    Preconditions.checkNotNull(kvStoreProvider, "store provider cannot be null.");
    this.kvStoreProvider = kvStoreProvider;
  }

  @Override
  public void start() throws Exception {
    store =
        Suppliers.memoize(
            () -> kvStoreProvider.get().getStore(SQLRunnerSessionStoreImpl.StoreCreator.class));
  }

  @Override
  public void close() throws Exception {}

  @Override
  public Optional<SQLRunnerSession> get(String userId) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(userId), "userId must not be empty.");
    Document<String, SQLRunnerSession> doc = store.get().get(userId);
    if (doc == null) {
      return Optional.empty();
    }
    return Optional.of(doc.getValue());
  }

  @Override
  public SQLRunnerSession update(SQLRunnerSession updatedSession) {
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(updatedSession.getUserId()), "userId must not be empty.");
    Document<String, SQLRunnerSession> doc =
        store.get().put(updatedSession.getUserId(), updatedSession);
    return doc.getValue();
  }

  @Override
  public void delete(String userId) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(userId), "userId must not be empty.");
    store.get().delete(userId);
  }

  @Override
  public int deleteExpired() {
    long now = System.currentTimeMillis();
    int expired = 0;

    final SearchTypes.SearchQuery query =
        SearchQueryUtils.newRangeLong(EXPIRE_AT_LONG.getIndexFieldName(), 0L, now, true, false);
    final FindByCondition condition =
        new ImmutableFindByCondition.Builder().setCondition(query).build();
    Iterable<Document<String, SQLRunnerSession>> expiredDocs = store.get().find(condition);
    for (Document<String, SQLRunnerSession> doc : expiredDocs) {
      store.get().delete(doc.getKey());
      expired++;
    }
    return expired;
  }

  /** StoreCreator */
  public static final class StoreCreator
      implements IndexedStoreCreationFunction<String, SQLRunnerSession> {
    @Override
    public IndexedStore<String, SQLRunnerSession> build(StoreBuildingFactory factory) {
      return factory
          .<String, SQLRunnerSession>newStore()
          .name(STORE_NAME)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(SQLRunnerSession.class))
          .buildIndexed(new SQLRunnerSessionDocumentConverter());
    }
  }

  static final class SQLRunnerSessionDocumentConverter
      implements DocumentConverter<String, SQLRunnerSession> {

    private final Integer version = 0;

    @Override
    public void convert(DocumentWriter writer, String key, SQLRunnerSession record) {
      writer.write(USER_ID, record.getUserId());
      writer.writeTTLExpireAt(EXPIRE_AT, record.getTtlExpireAt());
      writer.write(EXPIRE_AT_LONG, record.getTtlExpireAt());
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }
}
