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

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Index keys for dataset.
 */
public interface DatasetIndexKeys {

  String LOWER_CASE_SUFFIX = "_LC";

  IndexKey DATASET_ID = IndexKey.newBuilder("id", "DATASET_ID", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .setIncludeInSearchAllFields(true)
    .build();
  IndexKey DATASET_UUID = IndexKey.newBuilder("uuid", "DATASET_UUID", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  IndexKey DATASET_SQL = IndexKey.newBuilder("sql", "SQL", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  IndexKey DATASET_PARENTS = IndexKey.newBuilder("par", "PARENTS", String.class)
    .setCanContainMultipleValues(true)
    .build(); // to get immidiate children
  IndexKey DATASET_COLUMNS_NAMES = IndexKey.newBuilder("col", "COLUMNS", String.class)
    .setIncludeInSearchAllFields(true)
    .setCanContainMultipleValues(true)
    .build();
  IndexKey DATASET_OWNER = IndexKey.newBuilder("usr", "OWNER", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  IndexKey DATASET_SOURCES = IndexKey.newBuilder("src", "SOURCE", String.class)
    .setCanContainMultipleValues(true)
    .build();
  IndexKey DATASET_ALLPARENTS = IndexKey.newBuilder("apar", "ALL_PARENTS", String.class)
    .setCanContainMultipleValues(true)
    .build(); // get all descendants

  // name and schema without escaping for use in jdbc/odbc and information metadata probes. (for case sensitive matching)
  IndexKey UNQUOTED_NAME = IndexKey.newBuilder("uname", "SEARCH_NAME", String.class)
    .build();
  IndexKey UNQUOTED_SCHEMA = IndexKey.newBuilder("uschm", "SEARCH_SCHEMA", String.class)
    .build();

  // lower-cased paths for case insensitive matching.
  IndexKey UNQUOTED_LC_NAME = IndexKey.newBuilder("ulname", UNQUOTED_NAME.getIndexFieldName() + LOWER_CASE_SUFFIX, String.class)
    .build();
  IndexKey UNQUOTED_LC_SCHEMA = IndexKey.newBuilder("ulschm", UNQUOTED_SCHEMA.getIndexFieldName() + LOWER_CASE_SUFFIX, String.class)
    .build();


  // TODO add Physical dataset search index keys

  FilterIndexMapping MAPPING = new FilterIndexMapping(DATASET_ID, DATASET_UUID, DATASET_SQL, DATASET_PARENTS, DATASET_COLUMNS_NAMES,
    DATASET_OWNER, DATASET_SOURCES, DATASET_ALLPARENTS);

}
