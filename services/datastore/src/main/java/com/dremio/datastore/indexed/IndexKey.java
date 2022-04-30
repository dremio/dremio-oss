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
package com.dremio.datastore.indexed;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import com.dremio.datastore.SearchTypes;
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
  private final boolean canContainMultipleValues;
  public static final String LOWER_CASE_SUFFIX = "_LC";

  /**
   * Sample use case, if index key is of enum type with index on enum's number value, converter can be used for required
   * conversion from enum's name to number before applying filter on it
   * @throws com.dremio.datastore.EnumSearchValueNotFoundException if no enum found corresponding to input string
   */
  private final Function converter;

  private IndexKey(String shortName, String indexFieldName, Class<?> valueType, SearchFieldSorting.FieldType sortedValueType,
                   boolean includeInSearchAllFields, boolean stored, Map<String, SearchQuery> reservedValues,
                   Boolean canContainMultipleValues, Function converter) {
    this.shortName = shortName;
    this.indexFieldName = indexFieldName;
    this.valueType = valueType;
    this.sortedValueType = sortedValueType;
    this.includeInSearchAllFields = includeInSearchAllFields;
    this.stored = stored;
    this.reservedValues = reservedValues;
    this.canContainMultipleValues = canContainMultipleValues;
    this.converter = converter;
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

  public boolean canContainMultipleValues() {
    return canContainMultipleValues;
  }

  public Function getConverter() {
    return converter;
  }

  public SearchFieldSorting toSortField(SortOrder order){
    Preconditions.checkArgument(isSorted());
    return SearchFieldSorting.newBuilder()
        .setField(indexFieldName)
        .setType(sortedValueType)
        .setOrder(order)
        .build();
  }

  public static Builder newBuilder(String shortName, String indexFieldName, Class<?> valueType) {
    Preconditions.checkArgument(shortName != null, "IndexKey requires a short name");
    Preconditions.checkArgument(indexFieldName != null, "IndexKey requires a field name");
    Preconditions.checkArgument(valueType != null, "IndexKey requires a value type");

    return new Builder(shortName, indexFieldName, valueType);
  }

  public static Builder newBuilder(String shortName, String indexFieldName, Class<?> valueType, Function converter) {
    Builder builder = newBuilder(shortName, indexFieldName, valueType);
    builder.setConverter(converter);
    return builder;
  }

  /**
   * IndexKey Builder
   */
  public static class Builder {
    private final String shortName;
    private final String indexFieldName;
    private final Class<?> valueType;
    private SearchTypes.SearchFieldSorting.FieldType sortedValueType = null;
    private boolean includeInSearchAllFields = false;
    private boolean stored = false;
    private Map<String, SearchTypes.SearchQuery> reservedValues = Collections.emptyMap();
    private boolean canContainMultipleValues = false;
    private Function converter;

    Builder(String shortName, String indexFieldName, Class<?> valueType) {
      this.shortName = shortName;
      this.indexFieldName = indexFieldName;
      this.valueType = valueType;
    }

    public Builder setSortedValueType(SearchTypes.SearchFieldSorting.FieldType sortedValueType) {
      this.sortedValueType = sortedValueType;
      return this;
    }

    public Builder setIncludeInSearchAllFields(boolean includeInSearchAllFields) {
      this.includeInSearchAllFields = includeInSearchAllFields;
      return this;
    }

    public Builder setStored(boolean stored) {
      this.stored = stored;
      return this;
    }

    public Builder setReservedValues(Map<String, SearchTypes.SearchQuery> reservedValues) {
      this.reservedValues = reservedValues;
      return this;
    }

    public Builder setCanContainMultipleValues(Boolean canContainMultipleValues) {
      this.canContainMultipleValues = canContainMultipleValues;
      return this;
    }

    public void setConverter(Function converter) {
      this.converter = converter;
    }

    public IndexKey build() {
      return new IndexKey(shortName, indexFieldName, valueType, sortedValueType, includeInSearchAllFields, stored, reservedValues, canContainMultipleValues, converter);
    }
  }
}
