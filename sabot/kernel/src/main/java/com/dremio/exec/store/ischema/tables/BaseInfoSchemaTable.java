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
import com.dremio.service.namespace.NamespaceService;

/**
 * Base class for tables in INFORMATION_SCHEMA.  Defines the table (fields and
 * types).
 */
abstract class BaseInfoSchemaTable<RECORD> {

  private final String name;
  private final Class<RECORD> clazz;

  public BaseInfoSchemaTable(
      String name,
      Class<RECORD> clazz) {
    this.name = name;
    this.clazz = clazz;
  }


  public Class<RECORD> getRecordClass(){
    return clazz;
  }

  public abstract Iterable<RECORD> asIterable(String catalogName, NamespaceService service, SearchQuery query);

}
