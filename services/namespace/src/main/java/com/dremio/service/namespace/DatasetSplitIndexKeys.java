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

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.indexed.IndexKey;

/** Search index keys for dataset split index */
public interface DatasetSplitIndexKeys {
  IndexKey SPLIT_ID =
      IndexKey.newBuilder("id", "SPLIT_ID", String.class).setIncludeInSearchAllFields(true).build();
  IndexKey SPLIT_VERSION = IndexKey.newBuilder("sver", "SPLIT_VERSION", Long.class).build();
  IndexKey DATASET_ID = IndexKey.newBuilder("dsid", "DATASET_ID", String.class).build();
  IndexKey SPLIT_IDENTIFIER =
      IndexKey.newBuilder("spltid", "SPLIT_IDENTIFIER", String.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
          .build();
  IndexKey SPLIT_SIZE =
      IndexKey.newBuilder("size", "SPLIT_SIZE", Long.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
          .build();
  IndexKey SPLIT_ROWS =
      IndexKey.newBuilder("rows", "SPLIT_ROWS", Long.class)
          .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
          .build();
}
