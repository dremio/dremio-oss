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
package com.dremio.service.reflection.store;

import java.util.List;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.indexed.IndexKey;
import com.google.common.collect.ImmutableList;

/**
 * reflection related {@link IndexKey}s
 */
public class ReflectionIndexKeys {
  static final IndexKey MATERIALIZATION_ID = IndexKey.newBuilder("mid", "MATERIALIZATION_ID", String.class)
    .build();
  static final IndexKey MATERIALIZATION_STATE = IndexKey.newBuilder("mst", "MATERIALIZATION_STATE", String.class)
    .build();
  static final IndexKey MATERIALIZATION_EXPIRATION = IndexKey.newBuilder("me", "MATERIALIZATION_EXPIRATION", Long.class)
    .build();
  static final IndexKey MATERIALIZATION_MODIFIED_AT = IndexKey.newBuilder("mma", "MATERIALIZATION_MOD_AT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();
  static final IndexKey MATERIALIZATION_SERIES_ID = IndexKey.newBuilder("mat_sid", "MATERIALIZATION_SERIES_ID", Long.class)
    .build();
  static final IndexKey MATERIALIZATION_INIT_REFRESH_SUBMIT = IndexKey.newBuilder("mat_irs", "MATERIALIZATION_INIT_REFRESH_SUBMIT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();
  static final IndexKey MATERIALIZATION_REFLECTION_ID = IndexKey.newBuilder("mat_rid", "MATERIALIZATION_REFLECTION_ID", String.class)
    .build();

  static final IndexKey REFLECTION_ID = IndexKey.newBuilder("rid", "REFLECTION_ID", String.class)
    .build();
  static final IndexKey DATASET_ID = IndexKey.newBuilder("dsid", "DATASET_ID", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  static final IndexKey TARGET_DATASET_ID = IndexKey.newBuilder("tdsid", "TARGET_DATASET_ID", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  static final IndexKey REFLECTION_NAME = IndexKey.newBuilder("rname", "REFLECTION_NAME", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .build();
  static final IndexKey CREATED_AT = IndexKey.newBuilder("cat", "CREATE_AT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();
  static final IndexKey REFLECTION_GOAL_STATE = IndexKey.newBuilder("rgs", "REFLECTION_GOAL_STATE", String.class)
    .build();
  static final IndexKey REFLECTION_GOAL_MODIFIED_AT = IndexKey.newBuilder("gma", "GOAL_MOD_AT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();

  static final IndexKey REFRESH_REFLECTION_ID = IndexKey.newBuilder("rid", "REFRESH_REFLECTION_ID", String.class)
    .build();
  static final IndexKey REFRESH_SERIES_ID = IndexKey.newBuilder("sid", "REFRESH_SERIES_ID", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();
  static final IndexKey REFRESH_CREATE = IndexKey.newBuilder("dt", "REFRESH_CREATE_DT", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .build();
  static final IndexKey REFRESH_SERIES_ORDINAL = IndexKey.newBuilder("rso", "REFRESH_SERIES_ORDINAL", Integer.class)
    .build();

  public static final List<SearchTypes.SearchFieldSorting> DEFAULT_SORT = ImmutableList.of(DATASET_ID.toSortField(SearchTypes.SortOrder.DESCENDING), REFLECTION_NAME.toSortField(SearchTypes.SortOrder.ASCENDING));
}
