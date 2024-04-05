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
package com.dremio.context;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import java.util.Map;
import java.util.UUID;

/** A context representing the active Catalog. */
public class CatalogContext implements SerializableContext {
  public static final RequestContext.Key<CatalogContext> CTX_KEY =
      RequestContext.newKey("catalog_ctx_key");
  public static final String DEFAULT_SERVICE_CATALOG_ID = "0a41079c-920c-4a28-aae7-21ed5ad440a6";

  public static final Metadata.Key<String> CATALOG_ID_HEADER_KEY =
      Metadata.Key.of("x-dremio-catalog-id-key", Metadata.ASCII_STRING_MARSHALLER);

  private final UUID catalogId;

  public CatalogContext(String catalogId) {
    this.catalogId = UUID.fromString(catalogId);
  }

  public UUID getCatalogId() {
    return catalogId;
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(CATALOG_ID_HEADER_KEY.name(), catalogId.toString());
  }

  public static class Transformer implements SerializableContextTransformer {
    @Override
    public RequestContext deserialize(final Map<String, String> headers, RequestContext builder) {
      if (headers.containsKey(CATALOG_ID_HEADER_KEY.name())) {
        return builder.with(
            CatalogContext.CTX_KEY, new CatalogContext(headers.get(CATALOG_ID_HEADER_KEY.name())));
      }

      return builder;
    }
  }
}
