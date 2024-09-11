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
package com.dremio.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.service.search.SearchDocumentIdProto;
import com.dremio.test.DremioTest;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSearchDocumentIdStoreImpl {
  private LocalKVStoreProvider kvStoreProvider;
  private SearchDocumentIdStoreImpl store;

  @BeforeEach
  public void setUp() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    store = new SearchDocumentIdStoreImpl(kvStoreProvider);
  }

  @AfterEach
  public void tearDown() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testGet_empty() {
    assertTrue(store.get(SearchDocumentIdProto.SearchDocumentId.getDefaultInstance()).isEmpty());
  }

  @Test
  public void testPutGet() {
    SearchDocumentIdProto.SearchDocumentId documentId =
        SearchDocumentIdProto.SearchDocumentId.newBuilder().setPath("main").build();
    Document<String, SearchDocumentIdProto.SearchDocumentId> document = store.put(documentId, null);
    assertThat(document.getTag()).isNotNull();
    assertThat(document.getKey()).isEqualTo("8b22daab2ff3ca18302bf93f08a11562");

    Optional<Document<String, SearchDocumentIdProto.SearchDocumentId>> optionalDocument =
        store.get(documentId);
    assertTrue(optionalDocument.isPresent());
    assertThat(optionalDocument.get().getKey()).isEqualTo(document.getKey());
    assertThat(optionalDocument.get().getValue()).isEqualTo(documentId);
  }

  @Test
  public void testDelete() {
    SearchDocumentIdProto.SearchDocumentId documentId =
        SearchDocumentIdProto.SearchDocumentId.newBuilder().setPath("main").build();
    Document<String, SearchDocumentIdProto.SearchDocumentId> document = store.put(documentId, null);
    assertTrue(store.get(documentId).isPresent());

    assertThat(store.delete(documentId, document.getTag())).isEqualTo(document.getKey());

    assertTrue(store.get(documentId).isEmpty());
  }
}
