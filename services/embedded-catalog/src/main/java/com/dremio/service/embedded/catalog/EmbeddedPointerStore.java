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
package com.dremio.service.embedded.catalog;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;

/** Manages the Metadata Pointer KV store for the embedded (internal) Iceberg tables. */
public class EmbeddedPointerStore {

  private static final String NAMESPACE_LOCATION = "<namespace>";
  private static final String ID_VALUE_SEPARATOR = ":";

  private final KVStore<String, String> store;

  public EmbeddedPointerStore(KVStoreProvider storeProvider) {
    this.store = storeProvider.getStore(EmbeddedPointerStoreBuilder.class);
  }

  public void delete(ContentKey key) {
    store.delete(key.toPathString());
  }

  public Content get(ContentKey key) {
    Document<String, String> doc = store.get(key.toPathString());
    return toContent(doc);
  }

  public void put(ContentKey key, Content content) {
    String id;
    String loc;
    if (content instanceof IcebergTable) {
      id = UUID.randomUUID().toString(); // new ID for every commit
      loc = ((IcebergTable) content).getMetadataLocation();
    } else if (content instanceof Namespace) {
      id = asNamespaceId(key);
      loc = NAMESPACE_LOCATION;
    } else {
      throw new IllegalArgumentException("Unsupported content type: " + content.getType());
    }

    put(key, id, loc);
  }

  private void put(ContentKey key, String id, String metadataLocation) {
    store.put(key.toPathString(), id + ID_VALUE_SEPARATOR + metadataLocation);
  }

  public Stream<Document<ContentKey, Content>> findAll() {
    return StreamSupport.stream(store.find().spliterator(), false)
        .map(this::toContentDoc)
        .filter(Objects::nonNull);
  }

  private Document<ContentKey, Content> toContentDoc(Document<String, String> doc) {
    Content content = toContent(doc);
    if (content == null) {
      return null;
    }

    ContentKey key = ContentKey.fromPathString(doc.getKey());
    return new ImmutableDocument.Builder<ContentKey, Content>()
        .setKey(key)
        .setValue(content)
        .build();
  }

  private Content toContent(Document<String, String> doc) {
    if (doc == null) {
      return null;
    }

    ContentKey key = ContentKey.fromPathString(doc.getKey());
    String value = doc.getValue();
    int idx = value.indexOf(ID_VALUE_SEPARATOR);
    if (idx <= 0) {
      throw new IllegalStateException("Unsupported value format: " + value);
    }

    String id = value.substring(0, idx);
    String metadataLocation = value.substring(idx + 1);
    if (NAMESPACE_LOCATION.equals(metadataLocation)) {
      return Namespace.builder().elements(key.getElements()).id(id).build();
    } else {
      return IcebergTable.of(metadataLocation, 0, 0, 0, 0, id);
    }
  }

  public static String asNamespaceId(ContentKey key) {
    return UUID.nameUUIDFromBytes(key.toPathString().getBytes(StandardCharsets.UTF_8)).toString();
  }
}
