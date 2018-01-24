/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.ischema.tables;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * INFORMATION_SCHEMA.CATALOGS
 */
public class CatalogsTable extends BaseInfoSchemaTable<CatalogsTable.Catalog> {

  public static final CatalogsTable DEFINITION = new CatalogsTable();

  private CatalogsTable() {
    super("CATALOGS", CatalogsTable.Catalog.class);
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.CATALOGS */
  public static class Catalog {
    public final String CATALOG_NAME;
    public final String CATALOG_DESCRIPTION;
    public final String CATALOG_CONNECT;

    public Catalog(String name, String description, String connect) {
      this.CATALOG_NAME = name;
      this.CATALOG_DESCRIPTION = description;
      this.CATALOG_CONNECT = connect;
    }
  }

  @Override
  public Iterable<Catalog> asIterable(String catalogName, NamespaceService service, SearchQuery query) {
    Preconditions.checkArgument(query == null);
    return ImmutableList.of(new Catalog(catalogName, InfoSchemaConstants.IS_CATALOG_DESCR, InfoSchemaConstants.IS_CATALOG_CONNECT));
  }

}
