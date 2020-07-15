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
package com.dremio.datastore.adapter;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LegacyStoreBuilderHelper;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.datastore.api.StoreCreationFunction;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * An adapter to bridge KVStoreProvider to LegacyKVStoreProvider.
 */
public class LegacyKVStoreProviderAdapter implements LegacyKVStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyKVStoreProviderAdapter.class);

  private final KVStoreProvider underlyingProvider;

  private LoadingCache<Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>>, LegacyKVStore<?, ?>> stores;

  public LegacyKVStoreProviderAdapter(KVStoreProvider underlyingProvider) {
    this.underlyingProvider = underlyingProvider;
    this.stores = CacheBuilder
        .newBuilder()
        .build(CacheLoader.from(this::newStore));
  }

  /**
   * Only use this method for tests
   *
   * @param scanResult
   * @return
   */
  @Deprecated
  public static LegacyKVStoreProvider inMemory(ScanResult scanResult) {
    final LocalKVStoreProvider kvStoreProvider = new LocalKVStoreProvider(scanResult, null, true, false);
    return new LegacyKVStoreProviderAdapter(kvStoreProvider) {
      @Override
      public void start() throws Exception {
        kvStoreProvider.start();
        super.start();
      }

      @Override
      public void close() throws Exception {
        super.close();
        kvStoreProvider.close();
      }
    };
  }

  @Override
  public <K, V, T extends LegacyKVStore<K, V>, U extends KVStore<K, V>> T getStore(
      Class<? extends LegacyStoreCreationFunction<K, V, T, U>> creator) {
    return (T) Preconditions.checkNotNull(stores.getUnchecked(creator), "Unknown store creator %s", creator.getName());
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting LegacyKVStoreProviderAdapter.");
  }

  private LegacyKVStore<?, ?> newStore(Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>> functionClass) {
    try {
      LegacyStoreBuildingFactory factory = new LegacyStoreBuildingFactory() {
        @Override
        public <K, V> LegacyStoreBuilder<K, V> newStore() {
          return LegacyKVStoreProviderAdapter.this.newStoreBuilder(functionClass);
        }
      };
      return functionClass.newInstance().build(factory);
    } catch (Exception e) {
      logger.warn("Unable to load StoreCreationFunction {}", functionClass.getSimpleName(), e);
      return null;
    }
  }

  private <K, V> LegacyStoreBuilder<K, V> newStoreBuilder(Class<? extends LegacyStoreCreationFunction<?, ?, ?, ?>> functionClass) {
    return new LegacyStoreBuilderAdapter<K, V>(() -> underlyingProvider.newStore()) {
      @Override
      protected KVStore<K, V> doBuild() {
        return underlyingProvider.getStore((Class<? extends StoreCreationFunction<K, V, KVStore<K, V>>>) functionClass);
      }

      @Override
      protected IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
        return underlyingProvider.getStore((Class<? extends StoreCreationFunction<K, V, IndexedStore<K, V>>>) functionClass);
      }
    };
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(underlyingProvider)) {
      return (T) underlyingProvider;
    }
    // Continue checking if the underlying provider, or potential any providers it
    // is wrapping match the target class.
    return underlyingProvider.unwrap(clazz);
  }

  /**
   * StoreBuilder adapter implementation. Bridges LegacyStoreBuilder and StoreBuilder.
   *
   * @param <K> key type K.
   * @param <V> value type V.
   */
  public static class LegacyStoreBuilderAdapter<K, V> extends LegacyAbstractStoreBuilder<K, V> {
    private final Supplier<KVStoreProvider.StoreBuilder<K,V>> underlyingStoreBuilderFactory;
    private boolean permitCompoundKeys = false;

    public LegacyStoreBuilderAdapter(Supplier<KVStoreProvider.StoreBuilder<K,V>> underlyingStoreBuilderFactory) {
      this.underlyingStoreBuilderFactory = underlyingStoreBuilderFactory;
    }

    @Override
    public LegacyStoreBuilder<K, V> permitCompoundKeys(boolean permitCompoundKeys) {
      this.permitCompoundKeys = permitCompoundKeys;
      return this;
    }

    protected KVStore<K, V> doBuild() {
      LegacyStoreBuilderHelper<K,V> legacyHelper = getStoreBuilderHelper();
      final KVStoreProvider.StoreBuilder<K,V> builder = underlyingStoreBuilderFactory.get();
      populateBuilder(builder, legacyHelper, permitCompoundKeys);

      return builder.build();
    }

    @Override
    public LegacyKVStore<K, V> build() {
      return new LegacyKVStoreAdapter<>(doBuild(), getStoreBuilderHelper().tryGetVersionExtractor());
    }

    protected IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
      LegacyStoreBuilderHelper<K,V> legacyHelper = getStoreBuilderHelper();
      final KVStoreProvider.StoreBuilder<K,V> builder = underlyingStoreBuilderFactory.get();
      populateBuilder(builder, legacyHelper, permitCompoundKeys);

      return builder.buildIndexed(documentConverter);
    }

    @Override
    public LegacyIndexedStore<K, V> buildIndexed(DocumentConverter<K, V> documentConverter) {
      return new LegacyIndexedStoreAdapter<>(doBuildIndexed(documentConverter),
          getStoreBuilderHelper().tryGetVersionExtractor());
    }

    /**
     * Helper method to set configurations in StoreBuilder with settings provided by the
     * this LegacyStoreBuilder.
     *
     * @param builder the KVStoreProvider.StoreBuilder with configurations to be set.
     * @param legacyStoreBuilderHelper legacy helper to provide settings for builder.
     * @param permitCompoundKeys Indicates if CompoundKeys should be allowed.
     */
    private void populateBuilder(KVStoreProvider.StoreBuilder<K, V> builder,
                                 LegacyStoreBuilderHelper<K,V> legacyStoreBuilderHelper,
                                 boolean permitCompoundKeys) {
      builder
        .name(legacyStoreBuilderHelper.getName())
        .keyFormat(legacyStoreBuilderHelper.getKeyFormat())
        .valueFormat(legacyStoreBuilderHelper.getValueFormat())
        .permitCompoundKeys(permitCompoundKeys);
    }
  }
}
