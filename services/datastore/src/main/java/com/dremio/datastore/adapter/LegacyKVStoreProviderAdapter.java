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
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.datastore.utility.LegacyStoreLoader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

/**
 * An adapter to bridge KVStoreProvider to LegacyKVStoreProvider.
 */
public class LegacyKVStoreProviderAdapter implements LegacyKVStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LegacyKVStoreProviderAdapter.class);

  private ImmutableMap<Class<? extends LegacyStoreCreationFunction<?>>, LegacyKVStore<?, ?>> stores;
  private final KVStoreProvider underlyingProvider;
  private final ScanResult scan;
  private final boolean startUnderlyingProvider;

  public LegacyKVStoreProviderAdapter(KVStoreProvider underlyingProvider, ScanResult scan, boolean startUnderlyingProvider) {
    this.underlyingProvider = underlyingProvider;
    this.scan = scan;
    this.startUnderlyingProvider = startUnderlyingProvider;
  }

  public LegacyKVStoreProviderAdapter(KVStoreProvider underlyingProvider, ScanResult scan) {
    this(underlyingProvider, scan, true);
  }

  @Override
  public <T extends LegacyKVStore<?, ?>> T getStore(Class<? extends LegacyStoreCreationFunction<T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting LegacyKVStoreProviderAdapter.");

    if (startUnderlyingProvider) {
      logger.info("Starting underlying KVStoreProvider.");
      underlyingProvider.start();
    }

    stores = LegacyStoreLoader.buildLegacyStores(scan, new LegacyStoreBuildingFactory() {
      @Override
      public <K, V> LegacyStoreBuilder<K, V> newStore() {
        return LegacyKVStoreProviderAdapter.this.newStore();
      }
    });
  }

  @VisibleForTesting
  <K, V> LegacyStoreBuilder<K, V> newStore() {
    return new LegacyStoreBuilderAdapter<>(() -> underlyingProvider.newStore());
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping LegacyKVStoreProviderAdapter by stopping underlying KVStoreProvider.");
    underlyingProvider.close();
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

    public LegacyStoreBuilderAdapter(Supplier<KVStoreProvider.StoreBuilder<K,V>> underlyingStoreBuilderFactory) {
      this.underlyingStoreBuilderFactory = underlyingStoreBuilderFactory;
    }

    @Override
    public LegacyKVStore<K, V> build() {
      LegacyStoreBuilderHelper<K,V> legacyHelper = getStoreBuilderHelper();
      final KVStoreProvider.StoreBuilder<K,V> builder = underlyingStoreBuilderFactory.get();
      setBuilderWithLegacyHelper(builder, legacyHelper);

      return new LegacyKVStoreAdapter<>(builder.build(), legacyHelper.tryGetVersionExtractor());
    }

    @Override
    public LegacyIndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass) {
      LegacyStoreBuilderHelper<K,V> legacyHelper = getStoreBuilderHelper();
      final KVStoreProvider.StoreBuilder<K,V> builder = underlyingStoreBuilderFactory.get();
      setBuilderWithLegacyHelper(builder, legacyHelper);

      return new LegacyIndexedStoreAdapter<>(builder.buildIndexed(documentConverterClass), legacyHelper.tryGetVersionExtractor());
    }

    /**
     * Helper method to set configurations in StoreBuilder with settings provided by the LegacyStoreBuilderHelper.
     *
     * @param builder the KVStoreProvider.StoreBuilder with configurations to be set.
     * @param legacyStoreBuilderHelper legacy helper to provide settigns for builder.
     */
    private void setBuilderWithLegacyHelper(KVStoreProvider.StoreBuilder<K, V> builder,
                                      LegacyStoreBuilderHelper<K,V> legacyStoreBuilderHelper) {
      builder.name(legacyStoreBuilderHelper.getName());
      builder.keyFormat(legacyStoreBuilderHelper.getKeyFormat());
      builder.valueFormat(legacyStoreBuilderHelper.getValueFormat());
    }
  }
}
