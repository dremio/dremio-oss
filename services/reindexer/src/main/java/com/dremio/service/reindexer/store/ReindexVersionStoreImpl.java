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

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.reindexer.proto.ReindexVersionInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.inject.Provider;

/**
 * Implements {@link ReindexVersionStore} for KVStore
 */
public class ReindexVersionStoreImpl implements ReindexVersionStore {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReindexVersionStoreImpl.class);

  private Supplier<IndexedStore<String, ReindexVersionInfo>> store;
  private static String collectionName = "DEFAULT_VERSION_STORE"; // Default value only for testcases.
  private final Provider<KVStoreProvider> kvStoreProvider;
  public static final IndexKey COLLECTION_NAME_INDEX_KEY = IndexKey.newBuilder("cn", "COLLECTION_NAME", String.class).build();
  public static final IndexKey VERSION_INDEX_KEY = IndexKey.newBuilder("version", "VERSION", Integer.class).build();

  public ReindexVersionStoreImpl(Provider<KVStoreProvider> storeProvider, String name) {
    Preconditions.checkNotNull(storeProvider, "store provider cannot be null");
    kvStoreProvider = storeProvider;
    collectionName = name;
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting Reindex version store service");
    store = Suppliers.memoize(() -> kvStoreProvider.get().getStore(ReindexStoreCreator.class));
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Reindex version store service");
  }

  /**
   * store creator for version store
   */
  public static final class ReindexStoreCreator implements IndexedStoreCreationFunction<String, ReindexVersionInfo> {
    @Override
    public IndexedStore<String, ReindexVersionInfo> build(StoreBuildingFactory factory) {
      return factory.<String, ReindexVersionInfo>newStore()
        .name(collectionName)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtobuf(ReindexVersionInfo.class))
        .buildIndexed(new ReindexVersionDocumentConverter());
    }
  }

  /**
   * document converter for version store
   */
  static final class ReindexVersionDocumentConverter implements DocumentConverter<String, ReindexVersionInfo> {
    private Integer version = 0;
    @Override
    public void convert(DocumentWriter writer, String key, ReindexVersionInfo record) {
      writer.write(COLLECTION_NAME_INDEX_KEY, record.getCollectionName());
      writer.write(VERSION_INDEX_KEY, record.getVersion());
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
  public void update(String collectionName, Function<ReindexVersionInfo, ReindexVersionInfo> modifier, Predicate<Integer> predicate) throws ReindexVersionStoreException {
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
  public ReindexVersionInfo get(String collectionName, int version) {
    ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder();
    SearchTypes.SearchQuery collectionQuery = SearchQueryUtils.newTermQuery(COLLECTION_NAME_INDEX_KEY, collectionName);
    SearchTypes.SearchQuery versionQuery = SearchQueryUtils.newTermQuery(VERSION_INDEX_KEY, version);
    SearchTypes.SearchQuery query = SearchQueryUtils.and(collectionQuery, versionQuery);
    FindByCondition findByCondition = builder.setCondition(query).build();

    Iterable<Document<String, ReindexVersionInfo>> iterable = store.get().find(findByCondition);
    Iterator<Document<String, ReindexVersionInfo>> iter = iterable.iterator();

    if (iter.hasNext()) {
      Document<String, ReindexVersionInfo> doc = iter.next();
      return doc.getValue();
    }

    return null;
  }
}
