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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;


/**
 * Search key which is a part of document stored in lucene.
 */
public final class IndexKey {

  private static final int MAX_STRING_LENGTH = 30000;
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

  void addToDoc(Document doc, String... values){
    Preconditions.checkArgument(valueType == String.class);
    if (isSorted()) {
      Preconditions.checkArgument(values.length < 2, "sorted fields cannot have multiple values");
    }

    // add distinct elements to doc
    final Iterable<String> nonNull = FluentIterable.from(Arrays.asList(values))
        .filter(new Predicate<String>() {
          @Override
          public boolean apply(@Nullable final String input) {
            return input != null;
          }
        });

    for (final String value : ImmutableSet.copyOf(nonNull)) {
      final String truncatedValue = StringUtils.abbreviate(value, MAX_STRING_LENGTH);
      doc.add(new StringField(indexFieldName, truncatedValue, stored ? Store.YES : Store.NO));
    }

    if (isSorted() && values.length == 1) {
      Preconditions.checkArgument(sortedValueType == SearchFieldSorting.FieldType.STRING);
      doc.add(new SortedDocValuesField(indexFieldName, new BytesRef(values[0])));
    }
  }

  void addToDoc(Document doc, byte[]... values){
    Preconditions.checkArgument(valueType == String.class);
    if (isSorted()) {
      Preconditions.checkArgument(values.length < 2, "sorted fields cannot have multiple values");
    }

    // add distinct elements to doc
    final Iterable<byte[]> nonNull = FluentIterable.from(Arrays.asList(values))
      .filter(new Predicate<byte[]>() {
        @Override
        public boolean apply(@Nullable final byte[] input) {
          return input != null;
        }
      });

    for (final byte[] value : ImmutableSet.copyOf(nonNull)) {
      final BytesRef truncatedValue = new BytesRef(value,0, Math.min(value.length, MAX_STRING_LENGTH));
      doc.add(new StringField(indexFieldName, truncatedValue, stored ? Store.YES : Store.NO));
    }

    if (isSorted() && values.length == 1) {
      Preconditions.checkArgument(sortedValueType == SearchFieldSorting.FieldType.STRING);
      doc.add(new SortedDocValuesField(indexFieldName, new BytesRef(values[0])));
    }
  }

  void addToDoc(Document doc, Long value){
    Preconditions.checkArgument(valueType == Long.class);
    if(value == null){
      return;
    }

    if(stored) {
      doc.add(new StoredField(indexFieldName, value));
    }

    doc.add(new LongPoint(indexFieldName, value));

    if(isSorted()){
      Preconditions.checkArgument(sortedValueType == SearchFieldSorting.FieldType.LONG);
      doc.add(new NumericDocValuesField(indexFieldName, value));
    }
  }

  void addToDoc(Document doc, Integer value){
    Preconditions.checkArgument(valueType == Integer.class);
    if(value == null){
      return;
    }


    if(stored) {
      doc.add(new StoredField(indexFieldName, value));
    }

    doc.add(new IntPoint(indexFieldName, value));

    if(isSorted()){
      Preconditions.checkArgument(sortedValueType == SearchFieldSorting.FieldType.INTEGER);
      doc.add(new NumericDocValuesField(indexFieldName, (long)value));
    }
  }

  void addToDoc(Document doc, Double value){
    Preconditions.checkArgument(valueType == Double.class);
    if(value == null){
      return;
    }

    if(stored) {
      doc.add(new StoredField(indexFieldName, value));
    }

    doc.add(new DoublePoint(indexFieldName, value));
    if(isSorted()){
      Preconditions.checkArgument(sortedValueType == SearchFieldSorting.FieldType.DOUBLE);
      doc.add(new DoubleDocValuesField(indexFieldName, value));
    }
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
