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
  private static final String PROJECT_ID = "project_id";
  private static final String ORG_ID = "org_id";
  private LocalKVStoreProvider kvStoreProvider;
  private SearchDocumentIdStoreImpl store;

  @BeforeEach
  public void setUp() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    store = new SearchDocumentIdStoreImpl(() -> kvStoreProvider);
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
        SearchDocumentIdProto.SearchDocumentId.newBuilder()
            .putContext(PROJECT_ID, "ProjectId")
            .putContext(ORG_ID, "OrgId")
            .addPath("main")
            .build();
    Document<String, SearchDocumentIdProto.SearchDocumentId> document = store.put(documentId, null);
    assertThat(document.getTag()).isNotNull();
    assertThat(document.getKey()).isEqualTo("bb6398240be08d7b796cdccb9afeb018");

    Optional<Document<String, SearchDocumentIdProto.SearchDocumentId>> optionalDocument =
        store.get(documentId);
    assertTrue(optionalDocument.isPresent());
    assertThat(optionalDocument.get().getKey()).isEqualTo(document.getKey());
    assertThat(optionalDocument.get().getValue()).isEqualTo(documentId);
  }

  @Test
  public void testComputeKey() {
    SearchDocumentIdProto.SearchDocumentId documentIdA =
        SearchDocumentIdProto.SearchDocumentId.newBuilder()
            .putContext(PROJECT_ID, "ProjectIdA")
            .putContext(ORG_ID, "OrgIdA")
            .addPath("main")
            .build();
    SearchDocumentIdProto.SearchDocumentId documentIdB =
        SearchDocumentIdProto.SearchDocumentId.newBuilder()
            .putContext(PROJECT_ID, "ProjectIdB")
            .putContext(ORG_ID, "OrgIdB")
            .addPath("main")
            .build();
    SearchDocumentIdProto.SearchDocumentId documentIdC =
        SearchDocumentIdProto.SearchDocumentId.newBuilder().addPath("main").build();
    String documentAKey = store.computeKey(documentIdA);
    assertThat(documentAKey).isEqualTo("8a545208de38c72b9fbc4acf1eb3d6b3");
    String documentBKey = store.computeKey(documentIdB);
    assertThat(documentBKey).isEqualTo("9d1d6c48349e3b1b65ceb6236889e65a");
    String documentCKey = store.computeKey(documentIdC);
    assertThat(documentCKey).isEqualTo("d1cb0ea303259b25d920e0e6f20ebac9");
    assertThat(documentAKey).isNotEqualTo(documentBKey);
    assertThat(documentAKey).isNotEqualTo(documentCKey);
    assertThat(documentBKey).isNotEqualTo(documentCKey);
  }

  @Test
  public void testDelete() {
    SearchDocumentIdProto.SearchDocumentId documentId =
        SearchDocumentIdProto.SearchDocumentId.newBuilder().addPath("main").build();
    Document<String, SearchDocumentIdProto.SearchDocumentId> document = store.put(documentId, null);
    assertTrue(store.get(documentId).isPresent());

    assertThat(store.delete(documentId, document.getTag())).isEqualTo(document.getKey());

    assertTrue(store.get(documentId).isEmpty());
  }
}
