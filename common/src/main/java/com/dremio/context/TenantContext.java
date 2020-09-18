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

import java.util.UUID;

/**
 * Tenant context.
 */
public class TenantContext {
  public static final RequestContext.Key<TenantContext> CTX_KEY = RequestContext.newKey("tenant_ctx_key");
  // The default tenant id used in product.
  public static final String DEFAULT_PRODUCT_PROJECT_ID = "77a89f85-c936-4f42-ab21-2ee90e9609b8";
  // The default tenant id used in service (for testing)
  public static final String DEFAULT_SERVICE_PROJECT_ID = "77a89f85-c936-4f42-ab21-2ee90e9609b9";
  public static final String DEFAULT_SERVICE_ORG_ID = "77a89f85-c936-4f42-ab21-2ee90e96099b";

  public static TenantContext DEFAULT_SERVICE_CONTEXT =
    new TenantContext(DEFAULT_SERVICE_PROJECT_ID, DEFAULT_SERVICE_ORG_ID);

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

}
