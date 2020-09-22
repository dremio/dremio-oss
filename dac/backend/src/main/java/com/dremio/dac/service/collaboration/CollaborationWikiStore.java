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
package com.dremio.dac.service.collaboration;

import java.util.Iterator;
import java.util.Map;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Collaboration Wiki store
 */
public class CollaborationWikiStore {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollaborationWikiStore.class);

  public static final IndexKey ENTITY_ID = IndexKey.newBuilder("id", "ENTITY_ID", String.class)
    .build();
  public static final IndexKey CREATED_AT = IndexKey.newBuilder("createdAt", "CREATED_AT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();

  private static final long MAX_WIKI_LENGTH = 100_000;

  private static final SearchTypes.SearchFieldSorting LATEST_WIKI = SearchTypes.SearchFieldSorting.newBuilder()
    .setType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .setField(CREATED_AT.getIndexFieldName())
    .setOrder(SearchTypes.SortOrder.DESCENDING)
    .build();

  private static final String WIKI_STORE = "collaboration_wiki";
  private final Supplier<LegacyIndexedStore<String, CollaborationWiki>> wikiStore;

  public CollaborationWikiStore(final LegacyKVStoreProvider storeProvider) {
    Preconditions.checkNotNull(storeProvider, "kvStore provider required");
    wikiStore = Suppliers.memoize(() -> storeProvider.getStore(CollaborationWikiStore.CollaborationWikiStoreStoreCreator.class));
  }

  public void save(CollaborationWiki wiki) {
    validateWiki(wiki);

    wikiStore.get().put(wiki.getId(), wiki);
  }

  public Iterable<Map.Entry<String, CollaborationWiki>> find() {
    return wikiStore.get().find();
  }

  public void delete(String id) {
    wikiStore.get().delete(id);
  }

  private void validateWiki(CollaborationWiki wiki) {
    if (wiki.getEntityId() == null) {
      throw UserException.validationError()
        .message("Wiki entity id cannot be null.")
        .build(logger);
    }

    String text = wiki.getText();
    if (text != null && text.length() > MAX_WIKI_LENGTH) {
      throw UserException.validationError()
        .message("Wiki text can not contain more than 100K characters but found [%s].", text.length())
        .build(logger);
    }
  }

  public Optional<CollaborationWiki> getLatestWikiForEntityId(String id) {
    final LegacyIndexedStore.LegacyFindByCondition findByCondition = new LegacyIndexedStore.LegacyFindByCondition()
      .addSorting(LATEST_WIKI)
      .setOffset(0)
      .setLimit(1)
      .setCondition(SearchQueryUtils.newTermQuery(ENTITY_ID, id));
    Iterable<Map.Entry<String, CollaborationWiki>> entries = wikiStore.get().find(findByCondition);

    Iterator<Map.Entry<String, CollaborationWiki>> iterator = entries.iterator();
    return iterator.hasNext() ? Optional.fromNullable(iterator.next().getValue()) : Optional.absent();
  }

  /**
   * store creator
   */
  public static final class CollaborationWikiStoreStoreCreator implements LegacyIndexedStoreCreationFunction<String, CollaborationWiki> {
    @Override
    public LegacyIndexedStore<String, CollaborationWiki> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, CollaborationWiki>newStore()
        .name(WIKI_STORE)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtostuff(CollaborationWiki.class))
        .buildIndexed(new CollaborationWikiStore.CollaborationWikiConverter());
    }
  }

  private static final class CollaborationWikiConverter implements DocumentConverter<String, CollaborationWiki> {
    @Override
    public void convert(DocumentWriter writer, String id, CollaborationWiki record) {
      writer.write(ENTITY_ID, record.getEntityId());
      writer.write(CREATED_AT, record.getCreatedAt());
    }
  }
}
