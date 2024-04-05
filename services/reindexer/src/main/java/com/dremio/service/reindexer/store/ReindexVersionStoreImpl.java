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
package com.dremio.service.reindexer.store;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.reindexer.proto.ReindexVersionInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.inject.Provider;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/** Implements {@link ReindexVersionStore} for KVStore */
public class ReindexVersionStoreImpl implements ReindexVersionStore {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReindexVersionStoreImpl.class);

  private Supplier<IndexedStore<String, ReindexVersionInfo>> store;
  private final Class<? extends ReindexVersionStoreCreator> storeCreator;
  private final Provider<KVStoreProvider> kvStoreProvider;
  public static final IndexKey COLLECTION_NAME_INDEX_KEY =
      IndexKey.newBuilder("cn", "COLLECTION_NAME", String.class).build();
  public static final IndexKey COLLECTION_VERSION_INDEX_KEY =
      IndexKey.newBuilder("cv", "collectionversion", Integer.class).build();

  public ReindexVersionStoreImpl(
      Provider<KVStoreProvider> storeProvider,
      Class<? extends ReindexVersionStoreCreator> storeCreatorFunction) {
    Preconditions.checkNotNull(storeProvider, "store provider cannot be null");
    kvStoreProvider = storeProvider;
    this.storeCreator = storeCreatorFunction;
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting ReindexVersionStore");

    // KVStoreProvider is started here because
    // 1. In coordinator, start of this class (i.e ReindexVersionStoreImpl)
    // and that of its dependent kvStoreProvider may be called in any order.
    // So this creates a necessity to start kvStoreProvider explicitly in coordinator, atleast on
    // demand.
    //
    // 2. In mt-services, both starts are called in correct order as defined XXXApplication.java
    //
    // 3. To solve above problem in coordinator, we use IdempotentStartKVStoreProvider as
    // kvStoreProvider (variable here).
    // Its start() is idempotent, so its safe to call start method multiple times.
    kvStoreProvider.get().start();
    store = Suppliers.memoize(() -> kvStoreProvider.get().getStore(storeCreator));
    logger.info("ReindexVersionStore is started");
  }

  @Override
  public void close() throws Exception {
    kvStoreProvider.get().close();
    logger.info("Stopped ReindexVersionStore");
  }

  /** document converter for version store */
  static final class ReindexVersionDocumentConverter
      implements DocumentConverter<String, ReindexVersionInfo> {
    private Integer version = 0;

    @Override
    public void convert(DocumentWriter writer, String key, ReindexVersionInfo record) {
      writer.write(COLLECTION_NAME_INDEX_KEY, record.getCollectionName());
      writer.write(COLLECTION_VERSION_INDEX_KEY, record.getVersion());
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }

  @Override
  public void save(String collectionName, ReindexVersionInfo versionInfo) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(collectionName));
    store.get().put(collectionName, versionInfo);
  }

  @Override
  public void update(
      String collectionName,
      UnaryOperator<ReindexVersionInfo> modifier,
      Predicate<Integer> predicate)
      throws ReindexVersionStoreException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(collectionName));

    Document<String, ReindexVersionInfo> versionDoc = store.get().get(collectionName);
    if (versionDoc == null) {
      throw new ReindexVersionStoreException("Missing version information in store");
    }
    if (!predicate.test(versionDoc.getValue().getStatusValue())) {
      throw new ReindexVersionStoreException("Version information status mismatch");
    }
    ReindexVersionInfo versionInfo = modifier.apply(versionDoc.getValue());
    store.get().put(collectionName, versionInfo, VersionOption.from(versionDoc));
  }

  @Override
  public void delete(String collectionName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(collectionName));
    store.get().delete(collectionName);
  }

  @Override
  public ReindexVersionInfo get(String collectionName) {
    Document<String, ReindexVersionInfo> reindexVersionInfoDocument =
        store.get().get(collectionName);
    return reindexVersionInfoDocument == null ? null : reindexVersionInfoDocument.getValue();
  }
}
