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
package com.dremio.service.namespace;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.DatastoreException;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IncrementCounter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Wrapper for {@link KVStore} to set (e)tag on reads/writes and to increment source config ordinal
 * on writes.
 */
class NamespaceStore implements IndexedStore<String, NameSpaceContainer> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NamespaceStore.class);

  private static final NameSpaceContainerVersionExtractor CONTAINER_VERSION_EXTRACTOR =
      new NameSpaceContainerVersionExtractor();

  private final IndexedStore<String, NameSpaceContainer> kvStore;

  public NamespaceStore(IndexedStore<String, NameSpaceContainer> kvStore) {
    this.kvStore = kvStore;
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByCondition find, FindOption... options) {
    return StreamSupport.stream(kvStore.find(find, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
    return kvStore.getCounts(conditions);
  }

  @Override
  public Integer version() {
    return kvStore.version();
  }

  @Override
  public Document<String, NameSpaceContainer> get(String key, GetOption... options) {
    return addTag(kvStore.get(key, options));
  }

  @Override
  public List<Document<String, NameSpaceContainer>> get(List<String> keys, GetOption... options) {
    return StreamSupport.stream(kvStore.get(keys, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        // Nulls are possible here.
        .collect(Collectors.toList());
  }

  @Override
  public Document<String, NameSpaceContainer> put(
      String key, NameSpaceContainer container, PutOption... options) {
    try (AutoCloseables.RollbackCloseable rollback = new AutoCloseables.RollbackCloseable()) {
      String tag = CONTAINER_VERSION_EXTRACTOR.getTag(container);

      rollback.add(CONTAINER_VERSION_EXTRACTOR.preCommit(container));

      KVStore.PutOption putOption;
      if (Strings.isNullOrEmpty(tag)) {
        putOption = KVStore.PutOption.CREATE;
      } else {
        putOption = new ImmutableVersionOption.Builder().setTag(tag).build();
      }

      Document<String, NameSpaceContainer> document =
          addTag(kvStore.put(key, container, putOption));
      CONTAINER_VERSION_EXTRACTOR.setTag(container, document.getTag());

      rollback.commit();
      return document;
    } catch (RuntimeException e) {
      logger.warn("Failure to put", e);
      throw e;
    } catch (Exception e) {
      logger.warn("Failure to put (other)", e);
      throw new DatastoreException(e);
    }
  }

  @Override
  public boolean contains(String key, ContainsOption... options) {
    return kvStore.contains(key, options);
  }

  @Override
  public void delete(String key, DeleteOption... options) {
    kvStore.delete(key, options);
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByRange<String> find, FindOption... options) {
    return StreamSupport.stream(kvStore.find(find, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void bulkIncrement(
      Map<String, List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    kvStore.bulkIncrement(keysToIncrement, option);
  }

  @Override
  public void bulkDelete(List<String> keysToDelete, DeleteOption... deleteOptions) {
    kvStore.bulkDelete(keysToDelete, deleteOptions);
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(FindOption... options) {
    return StreamSupport.stream(kvStore.find(options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public KVAdmin getAdmin() {
    return kvStore.getAdmin();
  }

  @Override
  public String getName() {
    return kvStore.getName();
  }

  /**
   * Historically the "etag" is stored inside {@link NameSpaceContainer}, users of the store assume
   * so. The method adds the tag on reads and writes.
   */
  private static Document<String, NameSpaceContainer> addTag(
      Document<String, NameSpaceContainer> document) {
    if (document != null) {
      CONTAINER_VERSION_EXTRACTOR.setTag(document.getValue(), document.getTag());
    }
    return document;
  }
}
