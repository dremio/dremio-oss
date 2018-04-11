/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Objects;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.store.ischema.tables.SchemataTable.Schema;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.collect.FluentIterable;

/**
 * INFORMATION_SCHEMA.SCHEMATA
 */
public class SchemataTable extends BaseInfoSchemaTable<Schema> {

  public static final SchemataTable DEFINITION = new SchemataTable();

  private SchemataTable() {
    super("SCHEMATA", SchemataTable.Schema.class);
  }

  @Override
  public Iterable<Schema> asIterable(final String catalogName, String username, DatasetListingService service, SearchQuery query) {
    final LinkedHashSet<Schema> set = new LinkedHashSet<>();

    /*
     * We'll loop through all datasets and then return the unique set of schema that include at least one dataset.
     */
    final Iterable<Entry<NamespaceKey, NameSpaceContainer>> searchResults;
    try {
      searchResults = service.find(username, query == null ? null : new FindByCondition().setCondition(query));
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
    for (Entry<NamespaceKey, NameSpaceContainer> input : searchResults) {
      final NameSpaceContainer c = input.getValue();

      if (c.getType() != NameSpaceContainer.Type.DATASET) {
        continue;
      }

      if (input.getKey().getRoot().startsWith("__")) {
        continue;
      }

      set.add(new Schema(catalogName, input.getKey().getParent().toUnescapedString(), "<owner>", "simple", false));

    }

    return set;
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.SCHEMATA */
  public static class Schema {
    public final String CATALOG_NAME;
    public final String SCHEMA_NAME;
    public final String SCHEMA_OWNER;
    public final String TYPE;
    public final String IS_MUTABLE;

    public Schema(String catalog, String name, String owner, String type, boolean isMutable) {
      this.CATALOG_NAME = catalog;
      this.SCHEMA_NAME = name;
      this.SCHEMA_OWNER = owner;
      this.TYPE = type;
      this.IS_MUTABLE = isMutable ? "YES" : "NO";
    }

    @Override
    public int hashCode() {
      return Objects.hash(CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER, TYPE, IS_MUTABLE);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Schema other = (Schema) obj;
      return Objects.equals(CATALOG_NAME, other.CATALOG_NAME) &&
          Objects.equals(SCHEMA_NAME, other.SCHEMA_NAME) &&
          Objects.equals(SCHEMA_OWNER, other.SCHEMA_OWNER) &&
          Objects.equals(TYPE, other.TYPE) &&
          Objects.equals(IS_MUTABLE, other.IS_MUTABLE);
    }


  }
}
