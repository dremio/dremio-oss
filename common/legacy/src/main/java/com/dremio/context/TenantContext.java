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

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import io.grpc.Metadata;

/**
 * Tenant context.
 */
public class TenantContext implements SerializableContext {
  public static final RequestContext.Key<TenantContext> CTX_KEY = RequestContext.newKey("tenant_ctx_key");
  // The default tenant id used in product.
  public static final String DEFAULT_PRODUCT_PROJECT_ID = "77a89f85-c936-4f42-ab21-2ee90e9609b8";
  // The default tenant id used in service (for testing)
  public static final String DEFAULT_SERVICE_PROJECT_ID = "77a89f85-c936-4f42-ab21-2ee90e9609b9";
  public static final String DEFAULT_SERVICE_ORG_ID = "77a89f85-c936-4f42-ab21-2ee90e96099b";

  public static final TenantContext DEFAULT_SERVICE_CONTEXT =
    new TenantContext(DEFAULT_SERVICE_PROJECT_ID, DEFAULT_SERVICE_ORG_ID);

  // Note: These are public for use in annotating traces.
  public static final Metadata.Key<String> PROJECT_ID_HEADER_KEY =
    Metadata.Key.of("x-dremio-project-id-key", Metadata.ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> ORG_ID_HEADER_KEY =
    Metadata.Key.of("x-dremio-org-id-key", Metadata.ASCII_STRING_MARSHALLER);

  private final UUID projectId;
  private final UUID orgId;

  public TenantContext(String projectId, String orgId) {
    this.projectId = UUID.fromString(projectId);
    this.orgId = UUID.fromString(orgId);
  }

  public UUID getProjectId() {
    return projectId;
  }

  public UUID getOrgId() {
    return orgId;
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(PROJECT_ID_HEADER_KEY.name(), projectId.toString());
    builder.put(ORG_ID_HEADER_KEY.name(), orgId.toString());
  }

  public static class Transformer implements SerializableContextTransformer {
    @Override
    public RequestContext deserialize(final Map<String, String> headers, RequestContext builder) {
      if (headers.containsKey(PROJECT_ID_HEADER_KEY.name())
        && headers.containsKey(ORG_ID_HEADER_KEY.name())) {
        return builder.with(
          TenantContext.CTX_KEY,
          new TenantContext(
            headers.get(PROJECT_ID_HEADER_KEY.name()),
            headers.get(ORG_ID_HEADER_KEY.name())));
      }

      return builder;
    }
  }
}
