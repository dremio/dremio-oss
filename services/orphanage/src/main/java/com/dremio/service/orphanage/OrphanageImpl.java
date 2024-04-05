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
package com.dremio.service.orphanage;

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
import com.dremio.service.orphanage.proto.OrphanEntry;
import java.util.UUID;
import java.util.function.BiFunction;
import javax.inject.Inject;

/** Provides the orphan store and its management */
public class OrphanageImpl implements Orphanage {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OrphanageImpl.class);

  protected static final String ORPHAN_STORE_NAME = "orphan_store";
  private static final IndexKey ORPHAN_SCHEDULED_AT_INDEX_KEY =
      IndexKey.newBuilder("sdat", "scheduledAt", Long.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
          .build();

  private final IndexedStore<OrphanEntry.OrphanId, OrphanEntry.Orphan> orphanEntriesStore;

  /** Factory for OrphanageImpl */
  public static final class Factory implements Orphanage.Factory {
    private final KVStoreProvider kvStoreProvider;

    @Inject
    public Factory(KVStoreProvider kvStoreProvider) {
      this.kvStoreProvider = kvStoreProvider;
    }

    @Override
    public Orphanage get() {
      return new OrphanageImpl(kvStoreProvider);
    }
  }

  @Inject
  public OrphanageImpl(final KVStoreProvider kvStoreProvider) {
    this.orphanEntriesStore = createStore(kvStoreProvider);
  }

  protected IndexedStore<OrphanEntry.OrphanId, OrphanEntry.Orphan> createStore(
      final KVStoreProvider kvStoreProvider) {
    return kvStoreProvider.getStore(OrphanStoreCreator.class);
  }

  @Override
  public void addOrUpdateOrphan(OrphanEntry.OrphanId key, OrphanEntry.Orphan val) {

    orphanEntriesStore.put(key, val);
  }

  @Override
  public void addOrphan(OrphanEntry.Orphan val) {

    UUID uuid = UUID.randomUUID();
    String key = uuid.toString();
    OrphanEntry.OrphanId orphanId = OrphanEntry.OrphanId.newBuilder().setOrphanId(key).build();
    orphanEntriesStore.put(orphanId, val);
  }

  @Override
  public Document<OrphanEntry.OrphanId, OrphanEntry.Orphan> getOrphan(OrphanEntry.OrphanId key) {
    return orphanEntriesStore.get(key);
  }

  @Override
  public Iterable<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> getAllOrphans() {

    final SearchTypes.SearchQuery query = SearchQueryUtils.newMatchAllQuery();
    final SearchTypes.SearchFieldSorting sort =
        ORPHAN_SCHEDULED_AT_INDEX_KEY.toSortField(SearchTypes.SortOrder.ASCENDING);
    ImmutableFindByCondition.Builder builder = new ImmutableFindByCondition.Builder();
    FindByCondition findByCondition = builder.setCondition(query).addSort(sort).build();

    return orphanEntriesStore.find(findByCondition);
  }

  @Override
  public void deleteOrphan(OrphanEntry.OrphanId key) {
    logger.debug("Deleting key {} from the orphanage", key);
    orphanEntriesStore.delete(key);
  }

  /** document converter for Orphanage with indexed on the created time of the orphan entry */
  public static class OrphanDocumentConverter
      implements DocumentConverter<OrphanEntry.OrphanId, OrphanEntry.Orphan> {
    private Integer version = 0;

    @Override
    public void convert(
        DocumentWriter writer, OrphanEntry.OrphanId key, OrphanEntry.Orphan record) {
      writer.write(ORPHAN_SCHEDULED_AT_INDEX_KEY, record.getScheduledAt());
    }

    @Override
    public Integer getVersion() {
      return version;
    }
  }

  private static BiFunction<
          String, StoreBuildingFactory, IndexedStore<OrphanEntry.OrphanId, OrphanEntry.Orphan>>
      storeCreatorFunction =
          (s, storeBuildingFactory) ->
              storeBuildingFactory
                  .<OrphanEntry.OrphanId, OrphanEntry.Orphan>newStore()
                  .name(s)
                  .keyFormat(Format.ofProtobuf(OrphanEntry.OrphanId.class))
                  .valueFormat(Format.ofProtobuf(OrphanEntry.Orphan.class))
                  .buildIndexed(new OrphanDocumentConverter());

  /** StoreCreator for orphanage */
  public static class OrphanStoreCreator
      implements IndexedStoreCreationFunction<OrphanEntry.OrphanId, OrphanEntry.Orphan> {
    @Override
    public IndexedStore<OrphanEntry.OrphanId, OrphanEntry.Orphan> build(
        StoreBuildingFactory factory) {
      return storeCreatorFunction.apply(ORPHAN_STORE_NAME, factory);
    }
  }
}
