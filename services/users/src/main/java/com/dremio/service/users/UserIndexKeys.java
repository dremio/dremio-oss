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
package com.dremio.service.users;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Search index keys for users
 */
public interface UserIndexKeys {
  IndexKey UID = IndexKey.newBuilder("id", "id", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey NAME = IndexKey.newBuilder("name", "USERNAME", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey FIRST_NAME = IndexKey.newBuilder("first", "FIRST_NAME", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey LAST_NAME = IndexKey.newBuilder("last", "LAST_NAME", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey EMAIL = IndexKey.newBuilder("email", "EMAIL", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .build();
  IndexKey ROLE = IndexKey.newBuilder("role", "ROLE", String.class)
    .build();

  FilterIndexMapping MAPPING = new FilterIndexMapping(UID, NAME, FIRST_NAME, LAST_NAME, EMAIL);

}
