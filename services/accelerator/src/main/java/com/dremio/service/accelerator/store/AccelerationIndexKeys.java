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
package com.dremio.service.accelerator.store;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Wrapper around acceleration index keys.
 */
public final class AccelerationIndexKeys {
    public static final IndexKey ACCELERATION_ID = new IndexKey("id", "ACCELERATION_ID", String.class, null, false, true);
    public static final IndexKey ACCELERATION_STATE = new IndexKey("state", "ACCELERATION_STATE", String.class, SearchFieldSorting.FieldType.STRING, false, true);
    public static final IndexKey LAYOUT_ID = new IndexKey("layout_id", "LAYOUT_ID", String.class, null, false, true);
    public static final IndexKey LAYOUT_TYPE = new IndexKey("layout_type", "LAYOUT_TYPE", String.class, null, false, true);
    public static final IndexKey DATASET_PATH = new IndexKey("dataset", "DATASET_PATH", String.class, SearchFieldSorting.FieldType.STRING, true, true);
    public static final IndexKey PARENT_DATASET_PATH = new IndexKey("parent_dataset", "PARENT_DATASET", String.class, null, true, true);
    public static final IndexKey JOB_ID = new IndexKey("job", "JOB_ID", String.class, null, false, true);
    public static final IndexKey MATERIALIZATION_STATE = new IndexKey("mat_state", "MATERIALIZATION_STATE", String.class, null, false, true);
    public static final IndexKey FOOTPRINT = new IndexKey("footprint", "FOOTPRINT", Long.class, SearchFieldSorting.FieldType.LONG, false, true);
    public static final IndexKey TOTAL_REQUESTS = new IndexKey("requests", "NUM_REQUESTS", Long.class, SearchFieldSorting.FieldType.LONG, false, true);
    public static final IndexKey HITS = new IndexKey("hits", "HITS", Long.class, SearchFieldSorting.FieldType.LONG, false, true);

    public static final FilterIndexMapping MAPPING = new FilterIndexMapping(ACCELERATION_ID, ACCELERATION_STATE, LAYOUT_ID,
        LAYOUT_TYPE, DATASET_PATH, PARENT_DATASET_PATH, JOB_ID, MATERIALIZATION_STATE, FOOTPRINT, TOTAL_REQUESTS, HITS);

    public static final SortOrder DEFAULT_ORDER = SortOrder.ASCENDING;
    public static final SearchFieldSorting DEFAULT_SORT = DATASET_PATH.toSortField(DEFAULT_ORDER);
}
