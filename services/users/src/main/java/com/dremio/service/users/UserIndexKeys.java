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
package com.dremio.service.users;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Search index keys for users
 */
public interface UserIndexKeys {
  IndexKey UID = new IndexKey("id", "id", String.class, SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey NAME = new IndexKey("name", "USERNAME", String.class, SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey FIRST_NAME = new IndexKey("first", "FIRST_NAME", String.class, SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey LAST_NAME = new IndexKey("last", "LAST_NAME", String.class, SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey EMAIL = new IndexKey("email", "EMAIL", String.class, SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey ROLE = new IndexKey("role", "ROLE", String.class, null, false, false);

  FilterIndexMapping MAPPING = new FilterIndexMapping(UID, NAME, FIRST_NAME, LAST_NAME, EMAIL);

}
