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
package com.dremio.datastore.indexed;

import java.util.Collections;
import java.util.Map;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.google.common.base.Preconditions;


/**
 * Search key which is a part of document stored in lucene.
 */
public final class IndexKey {

  private final String shortName;
  private final String indexFieldName;
  private final Class<?> valueType;
  private final SearchFieldSorting.FieldType sortedValueType;
  private final boolean stored;
  private final boolean includeInSearchAllFields;
  private final Map<String, SearchQuery> reservedValues;

  public IndexKey(String shortName, String indexKey, Class<?> valueType, SearchFieldSorting.FieldType sortedValueType,
                  boolean includeInSearchAllFields, boolean stored) {
    this(shortName, indexKey, valueType, sortedValueType, includeInSearchAllFields, stored, Collections.<String, SearchQuery>emptyMap());
  }

  public IndexKey(String shortName, String indexFieldName, Class<?> valueType, SearchFieldSorting.FieldType sortedValueType,
                  boolean includeInSearchAllFields, boolean stored, Map<String, SearchQuery> reservedValues) {
    this.shortName = shortName;
    this.indexFieldName = indexFieldName;
    this.valueType = valueType;
    this.sortedValueType = sortedValueType;
    this.includeInSearchAllFields = includeInSearchAllFields;
    this.stored = stored;
    this.reservedValues = reservedValues;
  }

  public Map<String, SearchQuery> getReservedValues() {
    return reservedValues;
  }

  @Override
  public String toString() {
    return indexFieldName;
  }

  public String getShortName() {
    return shortName;
  }

  public String getIndexFieldName() {
    return indexFieldName;
  }

  public boolean isStored() {
    return stored;
  }

  public boolean isIncludeInSearchAllFields() {
    return includeInSearchAllFields;
  }

  public boolean isSorted() {
    return sortedValueType != null;
  }

  public SearchFieldSorting.FieldType getSortedValueType() {
    return sortedValueType;
  }

  public Class<?> getValueType() {
    return valueType;
  }

  public SearchFieldSorting toSortField(SortOrder order){
    Preconditions.checkArgument(isSorted());
    return SearchFieldSorting.newBuilder()
        .setField(indexFieldName)
        .setType(sortedValueType)
        .setOrder(order)
        .build();
  }
}
