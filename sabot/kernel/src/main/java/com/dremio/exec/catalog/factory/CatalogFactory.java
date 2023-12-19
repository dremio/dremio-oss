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
package com.dremio.exec.catalog.factory;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;

/**
 * Factory for Catalog creation.
 */
public class CatalogFactory implements CatalogSupplier {
  private final CatalogService catalogService;
  private final SecurityContext context;

  @Inject
  public CatalogFactory(CatalogService catalogService, SecurityContext context) {
    super();
    this.catalogService = catalogService;
    this.context = context;
  }

  @Override
  public Catalog get() {
    return catalogService.getCatalog(MetadataRequestOptions.of(
      SchemaConfig.newBuilder(CatalogUser.from(context.getUserPrincipal().getName()))
        .build()));
  }

  @Override
  public Catalog getSystemCatalog() {
    return catalogService.getCatalog(MetadataRequestOptions.of(SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()));
  }
}
