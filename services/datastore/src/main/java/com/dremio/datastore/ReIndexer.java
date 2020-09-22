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
package com.dremio.datastore;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.indexed.CoreIndexedStoreImpl;
import com.dremio.datastore.indexed.LuceneSearchIndex;
import com.dremio.datastore.indexed.SimpleDocumentWriter;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Replays updates from {@link CoreKVStore} to {@link LuceneSearchIndex}.
 */
public class ReIndexer implements ReplayHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReIndexer.class);

  private final IndexManager indexManager;
  private final Map<String, StoreWithId<?, ?>> idToStore;

  // caches
  private final Map<String, DocumentConverter<?, ?>> converters = Maps.newHashMap();
  private final Map<String, Serializer<?, byte[]>> keySerializers = Maps.newHashMap();
  private final Map<String, Serializer<?, byte[]>> valueSerializers = Maps.newHashMap();

  private final Map<String, ReIndexMetrics> metricsMap = Maps.newHashMap();

  ReIndexer(IndexManager indexManager, Map<String, StoreWithId<?, ?>> idToStore) {
    this.indexManager = indexManager;
    this.idToStore = idToStore;
  }

  @Override
  public void put(String tableName, byte[] key, byte[] value) {
    if (!isIndexed(tableName)) {
      logger.trace("Ignoring put: {} on table '{}'", key, tableName);
      return;
    }

    final KVStoreTuple<?> keyTuple = keyTuple(tableName, key);
    final Document document = toDoc(tableName, keyTuple, valueTuple(tableName, value));
    if (document != null) {
      // this is an update(...) and not an add; see CoreIndexedStoreImpl#index
      indexManager.getIndex(tableName)
          .update(keyAsTerm(keyTuple), document);

      metrics(tableName).puts++;
    }
  }

  @Override
  public void delete(String tableName, byte[] key) {
    if (!isIndexed(tableName)) {
      logger.trace("Ignoring delete: {} on table '{}'", key, tableName);
      return;
    }

    indexManager.getIndex(tableName)
        .deleteDocuments(keyAsTerm(keyTuple(tableName, key)));

    metrics(tableName).deletes++;
  }

  private boolean isIndexed(String name) {
    return idToStore.containsKey(name) &&
        (idToStore.get(name).getStore() instanceof CoreIndexedStore);
  }

  @SuppressWarnings("unchecked")
  private <K, V> DocumentConverter<K, V> converter(String name) {
    assert isIndexed(name);

    if (!converters.containsKey(name)) {
      converters.put(name,
        idToStore.get(name).getStoreBuilderHelper().getDocumentConverter());
    }

    return (DocumentConverter<K, V>) converters.get(name);
  }

  private Serializer<?, byte[]> keySerializer(String name) {
    assert isIndexed(name);

    if (!keySerializers.containsKey(name)) {
      keySerializers.put(name,
        idToStore.get(name).getStoreBuilderHelper().getKeyFormat().apply(ByteSerializerFactory.INSTANCE));
    }

    return keySerializers.get(name);
  }

  private Serializer<?, byte[]> valueSerializer(String name) {
    assert isIndexed(name);

    if (!valueSerializers.containsKey(name)) {
      valueSerializers.put(name,
        idToStore.get(name).getStoreBuilderHelper().getValueFormat().apply(ByteSerializerFactory.INSTANCE));
    }

    return valueSerializers.get(name);
  }

  private KVStoreTuple<?> keyTuple(String tableName, byte[] serializedBytes) {
    return new KVStoreTuple<>(keySerializer(tableName))
        .setSerializedBytes(serializedBytes);
  }

  private KVStoreTuple<?> valueTuple(String tableName, byte[] serializedBytes) {
    return new KVStoreTuple<>(valueSerializer(tableName))
        .setSerializedBytes(serializedBytes);
  }

  private <K, V> Document toDoc(String tableName, KVStoreTuple<K> key, KVStoreTuple<V> value) {
    final Document doc = new Document();
    final SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);
    converter(tableName).convert(documentWriter, key.getObject(), value.getObject());

    if (doc.getFields().isEmpty()) {
      return null;
    }

    documentWriter.write(CoreIndexedStoreImpl.ID_KEY, key.getSerializedBytes());

    return doc;
  }

  private static Term keyAsTerm(KVStoreTuple<?> key) {
    return CoreIndexedStoreImpl.keyAsTerm(key);
  }

  private ReIndexMetrics metrics(String tableName) {
    return metricsMap.computeIfAbsent(tableName, s -> new ReIndexMetrics());
  }

  String getMetrics() {
    return Joiner.on(",\n")
        .join(metricsMap.entrySet()
            .stream()
            .map(entry -> {
              final ReIndexMetrics metrics = entry.getValue();
              return "{ name: '" + entry.getKey()
                  + "', puts: " + metrics.puts
                  + ", deletes: " + metrics.deletes
                  + " }";
            }).collect(Collectors.toList())
        );
  }

  /**
   * Metrics about re-indexing.
   */
  private static class ReIndexMetrics {
    private int puts = 0;
    private int deletes = 0;
  }
}
