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

package com.dremio.service.script;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.script.proto.ScriptProto.Script;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Provider;

/** ScriptStore to store scripts */
public class ScriptStoreImpl implements ScriptStore {

  private static final String STORE_NAME = "script";
  private final Provider<KVStoreProvider> kvStoreProvider;
  private Supplier<IndexedStore<String, Script>> store;

  @Inject
  public ScriptStoreImpl(Provider<KVStoreProvider> kvStoreProvider) {
    Preconditions.checkNotNull(kvStoreProvider, "store provider cannot be null");
    this.kvStoreProvider = kvStoreProvider;
  }

  @Override
  public void start() throws Exception {
    store =
        Suppliers.memoize(() -> kvStoreProvider.get().getStore(ScriptStoreImpl.StoreCreator.class));
  }

  @Override
  public void close() throws Exception {}

  @Override
  public Optional<Script> get(String scriptId) {
    Document<String, Script> doc = store.get().get(scriptId);
    if (doc == null) {
      return Optional.empty();
    }
    return Optional.of(doc.getValue());
  }

  @Override
  public Optional<Script> getByName(String name) throws ScriptNotFoundException {
    ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder();
    FindByCondition condition =
        builder
            .setCondition(
                SearchQueryUtils.and(
                    SearchQueryUtils.newTermQuery(ScriptStoreIndexedKeys.NAME, name)))
            .setLimit(1)
            .build();
    Iterable<Document<String, Script>> doc = store.get().find(condition);
    Iterator<Document<String, Script>> iter = doc.iterator();
    if (iter.hasNext()) {
      Document<String, Script> script = iter.next();
      return Optional.ofNullable(script.getValue());
    }
    throw new ScriptNotFoundException(name);
  }

  @Override
  public Script create(String scriptId, Script script) {
    Preconditions.checkNotNull(scriptId);
    Preconditions.checkNotNull(script);
    Document<String, Script> doc = store.get().put(scriptId, script, KVStore.PutOption.CREATE);
    return doc.getValue();
  }

  @Override
  public Script update(String scriptId, Script script) {
    Preconditions.checkNotNull(scriptId);
    Preconditions.checkNotNull(script);
    Document<String, Script> doc = store.get().put(script.getScriptId(), script);
    return doc.getValue();
  }

  @Override
  public void delete(String scriptId) {
    Preconditions.checkNotNull(scriptId);
    store.get().delete(scriptId);
  }

  @Override
  public Iterable<Document<String, Script>> getAllByCondition(FindByCondition condition) {
    return store.get().find(condition);
  }

  @Override
  public long getCountByCondition(SearchTypes.SearchQuery condition) {
    return store.get().getCounts(condition).get(0);
  }

  /** StoreCreator */
  public static final class StoreCreator implements IndexedStoreCreationFunction<String, Script> {
    @Override
    public IndexedStore<String, Script> build(StoreBuildingFactory factory) {
      return factory
          .<String, Script>newStore()
          .name(STORE_NAME)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(Script.class))
          .buildIndexed(new ScriptDocumentConverter());
    }
  }

  static final class ScriptDocumentConverter implements DocumentConverter<String, Script> {

    private final Integer version = 0;

    @Override
    public void convert(DocumentWriter writer, String key, Script record) {
      writer.write(ScriptStoreIndexedKeys.ID, record.getScriptId());
      writer.write(ScriptStoreIndexedKeys.NAME, record.getName());
      writer.write(ScriptStoreIndexedKeys.CREATED_AT, record.getCreatedAt());
      writer.write(ScriptStoreIndexedKeys.CREATED_BY, record.getCreatedBy());
      writer.write(ScriptStoreIndexedKeys.MODIFIED_AT, record.getModifiedAt());
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }
}
