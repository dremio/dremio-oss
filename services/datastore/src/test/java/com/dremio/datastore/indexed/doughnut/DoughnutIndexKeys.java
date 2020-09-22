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
package com.dremio.datastore.indexed.doughnut;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;

/**
 * Doughnut IndexKeys.
 */
public final class DoughnutIndexKeys {
  private DoughnutIndexKeys() {}

  public static final IndexKey NAME = IndexKey.newBuilder("n", "name", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .setStored(true)
    .build();
  public static final IndexKey FLAVOR = IndexKey.newBuilder("f", "flavor", String.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING)
    .setStored(true)
    .build();
  public static final IndexKey PRICE = IndexKey.newBuilder("p", "price", Double.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.DOUBLE)
    .setStored(true)
    .build();
  public static final IndexKey THICKNESS = IndexKey.newBuilder("t", "thickness", Integer.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.INTEGER)
    .setStored(true)
    .build();
  public static final IndexKey DIAMETER = IndexKey.newBuilder("d", "diameter", Long.class)
    .setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG)
    .setStored(true)
    .build();

  public static final FilterIndexMapping MAPPING = new FilterIndexMapping(NAME, FLAVOR, PRICE, THICKNESS, DIAMETER);
}
