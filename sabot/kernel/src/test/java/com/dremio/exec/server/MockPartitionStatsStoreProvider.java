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
package com.dremio.exec.server;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.transientstore.TransientStore;
import com.dremio.partitionstats.storeprovider.PartitionStatsCacheStoreProvider;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class MockPartitionStatsStoreProvider<K, V> implements PartitionStatsCacheStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockPartitionStatsStoreProvider.class);

  private final MockedPartitionStatsStore<K, V> mockedPartitionStatsStore;

  public MockPartitionStatsStoreProvider(){
    mockedPartitionStatsStore = new MockedPartitionStatsStore();
  }

  @Override
  public void start() throws Exception {
    logger.info("Started MockPartitionStatsStoreProvider");
  }

  @Override
  public <K, V, T extends TransientStore<K, V>> T getStore(Format<K> keyFormat, Format<V> valueFormat) {
    return null;
  }

  @Override
  public <K, V, T extends TransientStore<K, V>> T getStore(Format<K> keyFormat, Format<V> valueFormat, int ttl) {
    return (T) mockedPartitionStatsStore;
  }

  @Override
  public void close() throws Exception {
    logger.info("Closed MockPartitionStatsStoreProvider");
  }

  public Set<K> getKeys(){
    return mockedPartitionStatsStore.cache.asMap().keySet();
  }

  public int getFreq(K key){
    return mockedPartitionStatsStore.cache.getIfPresent(key).getValue();
  }

  private class MockedPartitionStatsStore<K, V> implements TransientStore<K, V>{

    private final Cache<K, Map.Entry<V, Integer>> cache;

    public MockedPartitionStatsStore() {
      this.cache = Caffeine.newBuilder().build();
    }

    @Override
    public Document<K,V>  get(K key, KVStore.GetOption... options) {
      Map.Entry<V, Integer> value = cache.getIfPresent(key);
      if (null != value) {
        value.setValue(value.getValue()+1);
        return new ImmutableDocument
          .Builder<K, V>()
          .setKey(key)
          .setValue(value.getKey())
          .setTag(value.getValue()+"")
          .build();
      } else {
        return null;
      }
    }

    @Override
    public Iterable<Document<K,V>> get(List<K> keys, KVStore.GetOption... options) {
      return null;
    }

    @Override
    public Document<K,V>  put(K key, V value, KVStore.PutOption... options) {
      cache.put(key, new AbstractMap.SimpleEntry<>(value, 1));
      return null;
    }

    @Override
    public void delete(K key, KVStore.DeleteOption... options) {
      //pass
    }

    @Override
    public boolean contains(K key) {
      return false;
    }

  }
}
