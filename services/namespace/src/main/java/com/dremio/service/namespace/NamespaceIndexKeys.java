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
package com.dremio.service.namespace;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Index keys for the namespace service.
 */
public interface NamespaceIndexKeys {
  IndexKey SOURCE_ID = new IndexKey("id", "SOURCE_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey SPACE_ID = new IndexKey("id", "SPACE_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey HOME_ID = new IndexKey("id", "HOME_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey FOLDER_ID = new IndexKey("id", "FOLDER_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey ENTITY_TYPE = new IndexKey("enttyp", "ENTITY_TYPE", Integer.class, SearchTypes.SearchFieldSorting.FieldType.INTEGER, false, false);
  // lower case path without escaping.
  IndexKey UNQUOTED_LC_PATH = new IndexKey("ulpth", "SEARCH_PATH_LC", String.class, SearchFieldSorting.FieldType.STRING, false, false);


}
