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
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.search.SearchDocumentIdProto;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store for search document ids. The key is an id in search document storage, value is the {@link
 * com.dremio.service.search.SearchDocumentIdProto.SearchDocumentId} proto with fields that uniquely
 * identify searchable objects in KV store.
 */
public class SearchDocumentIdStoreImpl implements SearchDocumentIdStore {
  private static final Logger logger = LoggerFactory.getLogger(SearchDocumentIdStoreImpl.class);

  private static final JsonFormat.Printer JSON_FORMAT_PRINTER = JsonFormat.printer();

  private static final String SEARCH_DOCUMENT_ID_STORE = "search_document_id";

  private static final IndexKey PATH_INDEX_KEY =
      IndexKey.newBuilder("path", "PATH", String.class).build();

  private final Supplier<IndexedStore<String, SearchDocumentIdProto.SearchDocumentId>>
      storeSupplier;

  @Inject
  public SearchDocumentIdStoreImpl(KVStoreProvider kvStoreProvider) {
    storeSupplier =
        Suppliers.memoize(
            () -> kvStoreProvider.getStore(SearchDocumentIdStoreCreationFunction.class));
  }

  @Override
  public Optional<Document<String, SearchDocumentIdProto.SearchDocumentId>> get(
      SearchDocumentIdProto.SearchDocumentId documentId) {
    return Optional.ofNullable(storeSupplier.get().get(computeKey(documentId)));
  }

  @Override
  public Document<String, SearchDocumentIdProto.SearchDocumentId> put(
      SearchDocumentIdProto.SearchDocumentId documentId, String tag) {
    String key = computeKey(documentId);
    if (Strings.isNullOrEmpty(tag)) {
      return storeSupplier.get().put(key, documentId);
    } else {
      return storeSupplier
          .get()
          .put(key, documentId, new ImmutableVersionOption.Builder().setTag(tag).build());
    }
  }

  @Override
  public String delete(SearchDocumentIdProto.SearchDocumentId documentId, String tag) {
    String key = computeKey(documentId);
    if (Strings.isNullOrEmpty(tag)) {
      storeSupplier.get().delete(key);
    } else {
      storeSupplier.get().delete(key, new ImmutableVersionOption.Builder().setTag(tag).build());
    }
    return key;
  }

  /** Computes MD5 hash of the JSON representation of the proto. */
  @Override
  public String computeKey(SearchDocumentIdProto.SearchDocumentId documentId) {
    try {
      byte[] documentIdBytes =
          JSON_FORMAT_PRINTER.print(documentId).getBytes(StandardCharsets.UTF_8);
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(documentIdBytes);
      return Hex.encodeHexString(digest.digest());
    } catch (InvalidProtocolBufferException | NoSuchAlgorithmException e) {
      logger.error("Failed to calculate MD5 from {}", documentId, e);
      throw new RuntimeException(e);
    }
  }

  /** Builds {@link IndexedStore} for search document ids. */
  public static final class SearchDocumentIdStoreCreationFunction
      implements IndexedStoreCreationFunction<String, SearchDocumentIdProto.SearchDocumentId> {
    @Override
    public IndexedStore<String, SearchDocumentIdProto.SearchDocumentId> build(
        StoreBuildingFactory factory) {
      return factory
          .<String, SearchDocumentIdProto.SearchDocumentId>newStore()
          .name(SEARCH_DOCUMENT_ID_STORE)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(SearchDocumentIdProto.SearchDocumentId.class))
          .buildIndexed(new SearchDocumentIdConverter());
    }
  }

  /** Adds fields for secondary indices. */
  private static final class SearchDocumentIdConverter
      implements DocumentConverter<String, SearchDocumentIdProto.SearchDocumentId> {

    @Override
    public void convert(
        DocumentWriter writer, String key, SearchDocumentIdProto.SearchDocumentId document) {
      writer.write(PATH_INDEX_KEY, document.getPath());
    }

    @Override
    public Integer getVersion() {
      return 0;
    }
  }
}
