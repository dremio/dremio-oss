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
import com.dremio.datastore.indexed.IndexKey;

/**
 * Search index keys for dataset split index
 */
public interface DatasetSplitIndexKeys {
  IndexKey SPLIT_ID = new IndexKey("id", "SPLIT_ID", String.class, null, true, false);
  IndexKey SPLIT_VERSION = new IndexKey("sver", "SPLIT_VERSION", Long.class, null, false, false);
  IndexKey DATASET_ID = new IndexKey("dsid", "DATASET_ID", String.class, null, false, false);
  IndexKey SPLIT_IDENTIFIER = new IndexKey("spltid", "SPLIT_IDENTIFIER", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  IndexKey SPLIT_SIZE = new IndexKey("size", "SPLIT_SIZE", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  IndexKey SPLIT_ROWS = new IndexKey("rows", "SPLIT_ROWS", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
}
