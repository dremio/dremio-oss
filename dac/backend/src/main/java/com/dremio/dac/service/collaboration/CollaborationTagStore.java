/*
 * Copyright (C) 2017-2018 Dremio Corporation
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


import java.util.Map;

import org.apache.commons.collections.map.HashedMap;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.accelerator.store.serializer.SchemaSerializer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Collaboration Tag store
 */
public class CollaborationTagStore {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollaborationTagStore.class);

  public static final IndexKey ENTITY_ID = new IndexKey("id", "ENTITY_ID", String.class, null, false, false);
  public static final IndexKey LAST_MODIFIED = new IndexKey("lastModified", "LAST_MODIFIED", Long.class, null, false, false);

  private static final String TAGS_STORE = "collaboration_tags";
  private final Supplier<IndexedStore<String, CollaborationTag>> tagsStore;

  private static final long MAX_TAG_LENGTH = 128;

  public CollaborationTagStore(final KVStoreProvider storeProvider) {
    Preconditions.checkNotNull(storeProvider, "kvStore provider required");
    tagsStore = Suppliers.memoize(() -> storeProvider.getStore(CollaborationTagsStoreCreator.class));
  }

  public void save(CollaborationTag tag) {
    validateTag(tag);

    tag.setLastModified(System.currentTimeMillis());

    tagsStore.get().put(tag.getEntityId(), tag);
  }

  public Optional<CollaborationTag> getTagsForEntityId(String id) {
    return Optional.fromNullable(tagsStore.get().get(id));
  }

  public Iterable<Map.Entry<String, CollaborationTag>> find() {
    return tagsStore.get().find();
  }

  public void delete(String id) {
    tagsStore.get().delete(id);
  }

  public Iterable<Map.Entry<String, CollaborationTag>> find(FindByCondition condition) {
    return tagsStore.get().find(condition);
  }

  private void validateTag(CollaborationTag collaborationTag) {
    // tags must be unique and MAX_TAG_LENGTH characters max length
    final Map<String, Boolean> valueMap = new HashedMap();

    for (String tag : collaborationTag.getTagsList()) {
      if (tag.length() > MAX_TAG_LENGTH) {
        throw UserException.validationError()
          .message("Tags must be at most %s characters in length, but found [%s] which is [%s] long.", MAX_TAG_LENGTH, tag, tag.length())
          .build(logger);
      }

      if (valueMap.containsKey(tag)) {
        throw UserException.validationError()
          .message("Tags must be unique but found multiple instances of [%s].", tag)
          .build(logger);
      }

      valueMap.put(tag, true);
    }
  }

  /**
   * store creator
   */
  public static final class CollaborationTagsStoreCreator implements StoreCreationFunction<IndexedStore<String, CollaborationTag>> {
    @Override
    public IndexedStore<String, CollaborationTag> build(StoreBuildingFactory factory) {
      return factory.<String, CollaborationTag>newStore()
        .name(TAGS_STORE)
        .keySerializer(StringSerializer.class)
        .valueSerializer(CollaborationTagSerializer.class)
        .versionExtractor(CollaborationTagVersionExtractor.class)
        .buildIndexed(CollaborationTagConverter.class);
    }
  }

  private static final class CollaborationTagSerializer extends SchemaSerializer<CollaborationTag> {
    CollaborationTagSerializer() {
      super(CollaborationTag.getSchema());
    }
  }

  private static final class CollaborationTagVersionExtractor implements VersionExtractor<CollaborationTag> {
    @Override
    public Long getVersion(CollaborationTag value) {
      return value.getVersion();
    }

    @Override
    public Long incrementVersion(CollaborationTag value) {
      final Long current = value.getVersion();
      value.setVersion(Optional.fromNullable(current).or(-1L) + 1);
      return current;
    }

    @Override
    public void setVersion(CollaborationTag value, Long version) {
      value.setVersion(version == null ? 0 : version);
    }
  }

  private static final class CollaborationTagConverter implements KVStoreProvider.DocumentConverter<String, CollaborationTag> {
    @Override
    public void convert(KVStoreProvider.DocumentWriter writer, String id, CollaborationTag record) {
      writer.write(ENTITY_ID, record.getEntityId());
      writer.write(LAST_MODIFIED, record.getLastModified());
    }
  }
}
