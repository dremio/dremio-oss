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

import java.util.Map.Entry;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * INFORMATION_SCHEMA.VIEWS
 */
public class ViewsTable extends BaseInfoSchemaTable<ViewsTable.View> {

  private ViewsTable() {
    super("VIEWS", ViewsTable.View.class);
  }

  public static final ViewsTable DEFINITION = new ViewsTable();

  @Override
  public Iterable<View> asIterable(final String catalogName, NamespaceService service, SearchQuery query) {
    return FluentIterable.from(service.find(query == null ? null : new FindByCondition().setCondition(query)))
        .filter(new Predicate<Entry<NamespaceKey, NameSpaceContainer>>(){
          @Override
          public boolean apply(Entry<NamespaceKey, NameSpaceContainer> input) {
            final NameSpaceContainer c = input.getValue();

            if(c.getType() != NameSpaceContainer.Type.DATASET) {
              return false;
            }

            if(c.getDataset().getType() != DatasetType.VIRTUAL_DATASET) {
              return false;
            }

            if(input.getKey().getRoot().startsWith("__")) {
              return false;
            }

            return true;
          }})
        .transform(new Function<Entry<NamespaceKey, NameSpaceContainer>, View>() {
      @Override
      public View apply(Entry<NamespaceKey, NameSpaceContainer> input) {
        final String viewDefinition = input.getValue().getDataset().getVirtualDataset().getSql();
        return new View(catalogName, input.getKey().getParent().toUnescapedString(), input.getKey().getName(), viewDefinition);
      }
    });
  }

  /** Pojo object for a record in INFORMATION_SCHEMA.VIEWS */
  public static class View {
    public final String TABLE_CATALOG;
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;
    public final String VIEW_DEFINITION;

    public View(String catalog, String schema, String name, String definition) {
      this.TABLE_CATALOG = catalog;
      this.TABLE_SCHEMA = schema;
      this.TABLE_NAME = name;
      this.VIEW_DEFINITION = definition;
    }
  }


}
