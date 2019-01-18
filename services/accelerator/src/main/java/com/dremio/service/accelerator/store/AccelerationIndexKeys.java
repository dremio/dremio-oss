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
package com.dremio.service.accelerator.store;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Wrapper around acceleration index keys.
 */
public final class AccelerationIndexKeys {
    public static final IndexKey ACCELERATION_ID = IndexKey.newBuilder("id", "ACCELERATION_ID", String.class)
      .setStored(true).build();
    public static final IndexKey ACCELERATION_STATE = IndexKey.newBuilder("state", "ACCELERATION_STATE", String.class)
      .setSortedValueType(SearchFieldSorting.FieldType.STRING)
      .setStored(true)
      .build();
    public static final IndexKey LAYOUT_ID = IndexKey.newBuilder("layout_id", "LAYOUT_ID", String.class)
      .setStored(true)
      .setCanContainMultipleValues(true).build();
    public static final IndexKey LAYOUT_TYPE = IndexKey.newBuilder("layout_type", "LAYOUT_TYPE", String.class)
      .setStored(true)
      .setCanContainMultipleValues(true)
      .build();
    public static final IndexKey DATASET_PATH = IndexKey.newBuilder("dataset", "DATASET_PATH", String.class)
      .setSortedValueType(SearchFieldSorting.FieldType.STRING)
      .setIncludeInSearchAllFields(true)
      .setStored(true)
      .build();
    public static final IndexKey PARENT_DATASET_PATH = IndexKey.newBuilder("parent_dataset", "PARENT_DATASET", String.class)
      .setIncludeInSearchAllFields(true)
      .setStored(true)
      .setCanContainMultipleValues(true)
      .build();
    public static final IndexKey JOB_ID = IndexKey.newBuilder("job", "JOB_ID", String.class)
      .setStored(true).build();
    public static final IndexKey MATERIALIZATION_STATE = IndexKey.newBuilder("mat_state", "MATERIALIZATION_STATE", String.class)
      .setStored(true)
      .build();
    public static final IndexKey FOOTPRINT = IndexKey.newBuilder("footprint", "FOOTPRINT", Long.class)
      .setSortedValueType(SearchFieldSorting.FieldType.LONG)
      .setStored(true)
      .build();
    public static final IndexKey TOTAL_REQUESTS = IndexKey.newBuilder("requests", "NUM_REQUESTS", Long.class)
      .setSortedValueType(SearchFieldSorting.FieldType.LONG)
      .setStored(true)
      .build();
    public static final IndexKey HITS = IndexKey.newBuilder("hits", "HITS", Long.class)
      .setSortedValueType(SearchFieldSorting.FieldType.LONG)
      .setStored(true)
      .build();

    public static final FilterIndexMapping MAPPING = new FilterIndexMapping(ACCELERATION_ID, ACCELERATION_STATE, LAYOUT_ID,
        LAYOUT_TYPE, DATASET_PATH, PARENT_DATASET_PATH, JOB_ID, MATERIALIZATION_STATE, FOOTPRINT, TOTAL_REQUESTS, HITS);

    public static final SortOrder DEFAULT_ORDER = SortOrder.ASCENDING;
    public static final SearchFieldSorting DEFAULT_SORT = DATASET_PATH.toSortField(DEFAULT_ORDER);
}
