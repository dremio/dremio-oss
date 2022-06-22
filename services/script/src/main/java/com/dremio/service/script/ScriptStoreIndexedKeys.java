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

package com.dremio.service.script;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Indexes for ScriptStore
 */
public interface ScriptStoreIndexedKeys {
  IndexKey ID = IndexKey.newBuilder("id", "ID", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey NAME = IndexKey.newBuilder("name", "NAME", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey CREATED_AT =
    IndexKey.newBuilder("created_at", "CREATED_AT", Long.class)
      .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
      .build();
  IndexKey CREATED_BY = IndexKey.newBuilder("created_by", "CREATED_BY", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey MODIFIED_AT =
    IndexKey.newBuilder("modified_at", "MODIFIED_AT", Long.class)
      .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
      .build();
}
