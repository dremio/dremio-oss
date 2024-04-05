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
package com.dremio.service.namespace;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.indexed.IndexKey;

/** Index keys for the namespace service. */
public interface NamespaceIndexKeys {
  IndexKey ENTITY_ID =
      IndexKey.newBuilder("entid", "ENTITY_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey SOURCE_ID =
      IndexKey.newBuilder("id", "SOURCE_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey SPACE_ID =
      IndexKey.newBuilder("id", "SPACE_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey UDF_ID =
      IndexKey.newBuilder("id", "UDF_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey HOME_ID =
      IndexKey.newBuilder("id", "HOME_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey FOLDER_ID =
      IndexKey.newBuilder("id", "FOLDER_ID", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey ENTITY_TYPE =
      IndexKey.newBuilder("enttyp", "ENTITY_TYPE", Integer.class)
          .setSortedValueType(SearchFieldSorting.FieldType.INTEGER)
          .build();
  // lower case path without escaping.
  IndexKey UNQUOTED_LC_PATH =
      IndexKey.newBuilder("ulpth", "SEARCH_PATH_LC", String.class)
          .setSortedValueType(SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey LAST_MODIFIED =
      IndexKey.newBuilder("lastmodified", "LAST_MODIFIED", Long.class)
          .setSortedValueType(SearchFieldSorting.FieldType.LONG)
          .build();
}
