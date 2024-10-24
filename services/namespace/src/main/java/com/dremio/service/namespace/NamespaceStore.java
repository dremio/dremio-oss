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
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Provider;

/**
 * Wrapper for {@link KVStore} to:
 *
 * <ul>
 *   <li>set (e)tag on reads/writes and to increment source config ordinal on writes.
 *   <li>call given callback operations on create/update/delete namespace operations
 * </ul>
 */
public class NamespaceStore {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(NamespaceStore.class);
  public static final String DAC_NAMESPACE = "dac-namespace";

  private static final NameSpaceContainerVersionExtractor CONTAINER_VERSION_EXTRACTOR =
      new NameSpaceContainerVersionExtractor();

  private final Supplier<IndexedStore<String, NameSpaceContainer>> store;
  private final List<NamespaceDmlCallback> callbacks;

  public NamespaceStore(Provider<KVStoreProvider> provider) {
    this(provider, List.of(), false);
  }

  public NamespaceStore(
      Provider<KVStoreProvider> provider,
      List<NamespaceDmlCallback> callbacks,
      boolean useCachingStore) {
    Preconditions.checkNotNull(provider, "store.get() provider required");
    this.store =
        Suppliers.memoize(
            () -> {
              IndexedStore<String, NameSpaceContainer> kvStore =
                  provider.get().getStore(NamespaceStoreCreator.class);
              return useCachingStore ? new CachingNamespaceStore(kvStore) : kvStore;
            });
    this.callbacks = callbacks;
  }

  /** Creator for namespace KVStore. */
  public static class NamespaceStoreCreator
      implements IndexedStoreCreationFunction<String, NameSpaceContainer> {
    @Override
    public IndexedStore<String, NameSpaceContainer> build(StoreBuildingFactory factory) {
      return factory
          .<String, NameSpaceContainer>newStore()
          .name(DAC_NAMESPACE)
          .keyFormat(Format.ofString())
          .valueFormat(
              Format.wrapped(
                  NameSpaceContainer.class,
                  NameSpaceContainer::toProtoStuff,
                  NameSpaceContainer::new,
                  Format.ofProtostuff(
                      com.dremio.service.namespace.protostuff.NameSpaceContainer.class)))
          .buildIndexed(getConverter());
    }

    protected DocumentConverter<String, NameSpaceContainer> getConverter() {
      return new NamespaceConverter();
    }
  }

  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByCondition find, KVStore.FindOption... options) {
    return StreamSupport.stream(store.get().find(find, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
    return store.get().getCounts(conditions);
  }

  public Document<String, NameSpaceContainer> get(String key, KVStore.GetOption... options) {
    return addTag(store.get().get(key, options));
  }

  public List<Document<String, NameSpaceContainer>> get(
      List<String> keys, KVStore.GetOption... options) {
    return StreamSupport.stream(store.get().get(keys, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        // Nulls are possible here.
        .collect(Collectors.toList());
  }

  public Document<String, NameSpaceContainer> put(String key, NameSpaceContainer container) {
    try (AutoCloseables.RollbackCloseable rollback = new AutoCloseables.RollbackCloseable()) {
      String tag = CONTAINER_VERSION_EXTRACTOR.getTag(container);

      rollback.add(CONTAINER_VERSION_EXTRACTOR.preCommit(container));

      boolean isCreate = Strings.isNullOrEmpty(tag);
      KVStore.PutOption putOption =
          isCreate
              ? KVStore.PutOption.CREATE
              : new ImmutableVersionOption.Builder().setTag(tag).build();

      Document<String, NameSpaceContainer> document =
          addTag(store.get().put(key, container, putOption));
      CONTAINER_VERSION_EXTRACTOR.setTag(container, document.getTag());

      for (NamespaceDmlCallback callback : callbacks) {
        List<String> path = container.getFullPathList();
        if (isCreate) {
          callback.onAdd(path);
        } else {
          callback.onUpdate(path);
        }
      }

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

  /* Not exposed outside the package since NamespaceInternalKey is a namespace-internal concept */
  void delete(NamespaceInternalKey key, KVStore.DeleteOption... options) {
    store.get().delete(key.getKey(), options);

    for (NamespaceDmlCallback callback : callbacks) {
      callback.onDelete(key.getPath().getPathComponents());
    }
  }

  public void delete(NamespaceKey key, KVStore.DeleteOption... options) {
    store.get().delete(new NamespaceInternalKey(key).getKey(), options);

    for (NamespaceDmlCallback callback : callbacks) {
      callback.onDelete(key.getPathComponents());
    }
  }

  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByRange<String> find, KVStore.FindOption... options) {
    return StreamSupport.stream(store.get().find(find, options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  public Iterable<Document<String, NameSpaceContainer>> find(KVStore.FindOption... options) {
    return StreamSupport.stream(store.get().find(options).spliterator(), false)
        .map(NamespaceStore::addTag)
        .collect(Collectors.toUnmodifiableList());
  }

  public void invalidateNamespaceCache(String key) {
    if (store.get() instanceof CachingNamespaceStore) {
      ((CachingNamespaceStore) store.get()).invalidateCache(key);
    }
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
