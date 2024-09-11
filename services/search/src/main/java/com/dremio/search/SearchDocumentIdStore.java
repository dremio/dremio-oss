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

import com.dremio.datastore.api.Document;
import com.dremio.service.search.SearchDocumentIdProto;
import java.util.Optional;

/**
 * Store for search document ids. The key is an id in search document storage, value is the {@link
 * com.dremio.service.search.SearchDocumentIdProto.SearchDocumentId} proto with fields that uniquely
 * identify searchable objects in KV store.
 */
public interface SearchDocumentIdStore {
  /** Gets existing document. The key is calculated from the document id. */
  Optional<Document<String, SearchDocumentIdProto.SearchDocumentId>> get(
      SearchDocumentIdProto.SearchDocumentId documentId);

  /**
   * Creates or updates document id in the store. Tag from {@link Document} must be passed in when
   * updating the document id.
   */
  Document<String, SearchDocumentIdProto.SearchDocumentId> put(
      SearchDocumentIdProto.SearchDocumentId documentId, String tag);

  /**
   * Deletes given document id. The tag from {@link Document} must be passed in to detect concurrent
   * updates.
   *
   * @return Key of the deleted document id, which can be used to delete actual search document in
   *     the index.
   */
  String delete(SearchDocumentIdProto.SearchDocumentId documentId, String tag);

  /**
   * Converts document id to a key for the KV table.
   *
   * @param searchDocumentId the search document ID to compute the key for
   * @return the computed key as a string
   */
  String computeKey(SearchDocumentIdProto.SearchDocumentId searchDocumentId);
}
