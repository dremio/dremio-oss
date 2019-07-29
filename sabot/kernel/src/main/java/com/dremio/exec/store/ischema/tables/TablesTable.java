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
package com.dremio.exec.store.ischema.tables;

import java.util.Map.Entry;

import org.apache.calcite.schema.Schema.TableType;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * INFORMATION_SCHEMA.TABLES
 */
public class TablesTable extends BaseInfoSchemaTable<TablesTable.Table> {

  public static final TablesTable DEFINITION = new TablesTable();

  private TablesTable() {
    super("TABLES", TablesTable.Table.class);
  }

  @Override
  public Iterable<Table> asIterable(final String catalogName, String username, DatasetListingService service, SearchQuery query) {
    final Iterable<Entry<NamespaceKey, NameSpaceContainer>> searchResults;
    try {
      searchResults = service.find(username, query == null ? null : new FindByCondition().setCondition(query));
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
    return FluentIterable.from(searchResults)
        .filter(new Predicate<Entry<NamespaceKey, NameSpaceContainer>>() {
          @Override
          public boolean apply(Entry<NamespaceKey, NameSpaceContainer> input) {
            final NameSpaceContainer c = input.getValue();

            if (c.getType() != NameSpaceContainer.Type.DATASET) {
              return false;
            }

            if (input.getKey().getRoot().startsWith("__")) {
              return false;
            }

            return true;
          }
        })
        .transform(new Function<Entry<NamespaceKey, NameSpaceContainer>, Table>() {
          @Override
          public Table apply(Entry<NamespaceKey, NameSpaceContainer> input) {
            final String source = input.getKey().getRoot();
            final String tableType;
            if (input.getValue().getDataset().getType() == DatasetType.VIRTUAL_DATASET) {
              tableType = TableType.VIEW.name();
            } else if ("sys".equals(source) || "INFORMATION_SCHEMA".equals(source)) {
              tableType = TableType.SYSTEM_TABLE.name();
            } else {
              tableType = TableType.TABLE.name();
            }

            return new Table(catalogName, input.getKey().getParent().toUnescapedString(), input.getKey().getName(), tableType);
          }
        });
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.TABLES */
  public static class Table {
    public final String TABLE_CATALOG;
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;
    public final String TABLE_TYPE;

    public Table(String catalog, String schema, String name, String type) {
      this.TABLE_CATALOG = catalog;
      this.TABLE_SCHEMA = schema;
      this.TABLE_NAME = name;
      this.TABLE_TYPE = type;
    }
  }

}
