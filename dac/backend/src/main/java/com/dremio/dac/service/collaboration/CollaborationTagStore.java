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


import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Collaboration Tag store
 */
public class CollaborationTagStore {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollaborationTagStore.class);

  public static final IndexKey ENTITY_ID = IndexKey.newBuilder("id", "ENTITY_ID", String.class)
    .build();
  public static final IndexKey LAST_MODIFIED = IndexKey.newBuilder("lastModified", "LAST_MODIFIED", Long.class)
    .build();

  private static final String TAGS_STORE = "collaboration_tags";
  private final Supplier<LegacyIndexedStore<String, CollaborationTag>> tagsStore;

  private static final long MAX_TAG_LENGTH = 128;

  public CollaborationTagStore(final LegacyKVStoreProvider storeProvider) {
    Preconditions.checkNotNull(storeProvider, "kvStore provider required");
    tagsStore = Suppliers.memoize(() -> storeProvider.getStore(CollaborationTagsStoreCreator.class));
  }

  public void save(CollaborationTag tag) {
    validateTag(tag);

    tag.setLastModified(System.currentTimeMillis());

    tagsStore.get().put(tag.getEntityId(), tag);
  }

  public Optional<CollaborationTag> getTagsForEntityId(String id) {
    return Optional.ofNullable(tagsStore.get().get(id));
  }

  public Iterable<Map.Entry<String, CollaborationTag>> find() {
    return tagsStore.get().find();
  }

  public void delete(String id) {
    tagsStore.get().delete(id);
  }

  public Iterable<Map.Entry<String, CollaborationTag>> find(LegacyFindByCondition condition) {
    return tagsStore.get().find(condition);
  }

  private void validateTag(CollaborationTag collaborationTag) {
    // tags must be unique and MAX_TAG_LENGTH characters max length
    Set<String> seenTags = new HashSet<>();

    for (String tag : collaborationTag.getTagsList()) {
      if (tag.length() > MAX_TAG_LENGTH) {
        throw UserException.validationError()
          .message("Tags must be at most %s characters in length, but found [%s] which is [%s] long.", MAX_TAG_LENGTH, tag, tag.length())
          .build(logger);
      }

      if (seenTags.contains(tag)) {
        throw UserException.validationError()
          .message("Tags must be unique but found multiple instances of [%s].", tag)
          .build(logger);
      }

      seenTags.add(tag);
    }
  }

  /**
   * store creator
   */
  public static final class CollaborationTagsStoreCreator implements LegacyIndexedStoreCreationFunction<String, CollaborationTag> {
    @Override
    public LegacyIndexedStore<String, CollaborationTag> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, CollaborationTag>newStore()
        .name(TAGS_STORE)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtostuff(CollaborationTag.class))
        .versionExtractor(CollaborationTagVersionExtractor.class)
        .buildIndexed(new CollaborationTagConverter());
    }
  }

  private static final class CollaborationTagVersionExtractor implements VersionExtractor<CollaborationTag> {
    @Override
    public Long getVersion(CollaborationTag value) {
      return value.getVersion();
    }

    @Override
    public void setVersion(CollaborationTag value, Long version) {
      value.setVersion(version == null ? 0 : version);
    }

    @Override
    public String getTag(CollaborationTag value) {
      return value.getTag();
    }

    @Override
    public void setTag(CollaborationTag value, String tag) {
      value.setTag(tag);
    }
  }

  private static final class CollaborationTagConverter implements DocumentConverter<String, CollaborationTag> {
    private Integer version = 0;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(DocumentWriter writer, String id, CollaborationTag record) {
      writer.write(ENTITY_ID, record.getEntityId());
      writer.write(LAST_MODIFIED, record.getLastModified());
    }
  }
}
