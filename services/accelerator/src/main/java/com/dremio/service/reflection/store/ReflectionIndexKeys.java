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
package com.dremio.service.reflection.store;

import java.util.List;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.indexed.IndexKey;
import com.google.common.collect.ImmutableList;

/**
 * reflection related {@link IndexKey}s
 */
public class ReflectionIndexKeys {
  static final IndexKey MATERIALIZATION_ID = new IndexKey("mid", "MATERIALIZATION_ID", String.class, null, false, false);
  static final IndexKey MATERIALIZATION_STATE = new IndexKey("mst", "MATERIALIZATION_STATE", String.class, null, false, false);
  static final IndexKey MATERIALIZATION_EXPIRATION = new IndexKey("me", "MATERIALIZATION_EXPIRATION", Long.class, null, false, false);
  static final IndexKey MATERIALIZATION_MODIFIED_AT = new IndexKey("mma", "MATERIALIZATION_MOD_AT", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  static final IndexKey MATERIALIZATION_SERIES_ID = new IndexKey("mat_sid", "MATERIALIZATION_SERIES_ID", Long.class, null, false, false);
  static final IndexKey MATERIALIZATION_INIT_REFRESH_SUBMIT = new IndexKey("mat_irs", "MATERIALIZATION_INIT_REFRESH_SUBMIT", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  static final IndexKey MATERIALIZATION_REFLECTION_ID = new IndexKey("mat_rid", "MATERIALIZATION_REFLECTION_ID", String.class, null, false, false);

  static final IndexKey REFLECTION_ID = new IndexKey("rid", "REFLECTION_ID", String.class, null, false, false);
  static final IndexKey DATASET_ID = new IndexKey("dsid", "DATASET_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  static final IndexKey TARGET_DATASET_ID = new IndexKey("tdsid", "TARGET_DATASET_ID", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  static final IndexKey REFLECTION_NAME = new IndexKey("rname", "REFLECTION_NAME", String.class, SearchTypes.SearchFieldSorting.FieldType.STRING, false, false);
  static final IndexKey CREATED_AT = new IndexKey("cat", "CREATE_AT", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  static final IndexKey REFLECTION_GOAL_STATE = new IndexKey("rgs", "REFLECTION_GOAL_STATE", String.class, null, false, false);
  static final IndexKey REFLECTION_GOAL_MODIFIED_AT = new IndexKey("gma", "GOAL_MOD_AT", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);

  static final IndexKey REFRESH_REFLECTION_ID = new IndexKey("rid", "REFRESH_REFLECTION_ID", String.class, null, false, false);
  static final IndexKey REFRESH_SERIES_ID = new IndexKey("sid", "REFRESH_SERIES_ID", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  static final IndexKey REFRESH_CREATE = new IndexKey("dt", "REFRESH_CREATE_DT", Long.class, SearchTypes.SearchFieldSorting.FieldType.LONG, false, false);
  static final IndexKey REFRESH_SERIES_ORDINAL = new IndexKey("rso", "REFRESH_SERIES_ORDINAL", Integer.class, null, false, false);

  public static final List<SearchTypes.SearchFieldSorting> DEFAULT_SORT = ImmutableList.of(DATASET_ID.toSortField(SearchTypes.SortOrder.DESCENDING), REFLECTION_NAME.toSortField(SearchTypes.SortOrder.ASCENDING));
}
