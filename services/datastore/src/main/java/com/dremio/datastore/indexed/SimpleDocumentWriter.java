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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.arrow.util.VisibleForTesting;
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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.api.DocumentWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;

/**
 * A basic document writer
 *
 * Doesn't reuse document
 *
 */
public final class SimpleDocumentWriter implements DocumentWriter {
  @VisibleForTesting
  public static final int MAX_STRING_LENGTH = 30000;

  private final Document doc;

  public SimpleDocumentWriter(Document doc) {
    this.doc = doc;
  }

  @Override
  public void write(IndexKey key, Double value) {
    if(value != null){
      addToDoc(key, value);
    }
  }

  @Override
  public void write(IndexKey key, Integer value) {
    if(value != null){
      addToDoc(key, value);
    }
  }

  @Override
  public void write(IndexKey key, Long value) {
    if(value != null){
      addToDoc(key, value);
    }
  }

  @Override
  public void write(IndexKey key, byte[]... values) {
    addToDoc(key, values);
  }

  @Override
  public void write(IndexKey key, String... values) {
    addToDoc(key, values);
  }

  @VisibleForTesting
  Document getDoc() {
    return doc;
  }

  private void addToDoc(IndexKey key, String... values){
    Preconditions.checkArgument(key.getValueType() == String.class);
    final boolean sorted = key.isSorted();
    if (sorted) {
      checkIfSorted(key, (Object[]) values);
    }

    checkIfMultiValueField(key, (Object[]) values);

    final String indexFieldName = key.getIndexFieldName();
    final Store stored = key.isStored() ? Store.YES : Store.NO;
    for (String value : values) {
      if (value == null) {
        continue;
      }
      if (Utf8.encodedLength(value) > MAX_STRING_LENGTH) {
        value =  new BytesRef(value.getBytes(UTF_8), 0, MAX_STRING_LENGTH)
                    .utf8ToString();
      }
      doc.add(new StringField(indexFieldName, value, stored));
    }

    if (sorted && values.length == 1 && values[0] != null) {
      Preconditions.checkArgument(key.getSortedValueType() == SearchFieldSorting.FieldType.STRING);
      doc.add(new SortedDocValuesField(indexFieldName, new BytesRef(values[0])));
    }
  }

  private void addToDoc(IndexKey key, byte[]... values){
    Preconditions.checkArgument(key.getValueType() == String.class);
    final boolean sorted = key.isSorted();
    if (sorted) {
      checkIfSorted(key, (Object[]) values);
    }

    checkIfMultiValueField(key, (Object[]) values);

    final String indexFieldName = key.getIndexFieldName();
    final Store stored = key.isStored() ? Store.YES : Store.NO;
    for (final byte[] value : values) {
      if (value == null) {
        continue;
      }
      final BytesRef truncatedValue = new BytesRef(value,0, Math.min(value.length, MAX_STRING_LENGTH));
      doc.add(new StringField(indexFieldName, truncatedValue, stored));
    }

    if (sorted && values.length == 1 && values[0] != null) {
      Preconditions.checkArgument(key.getSortedValueType() == SearchFieldSorting.FieldType.STRING);
      doc.add(new SortedDocValuesField(indexFieldName, new BytesRef(values[0])));
    }
  }

  private void addToDoc(IndexKey key, Long value){
    Preconditions.checkArgument(key.getValueType() == Long.class);
    if(value == null){
      return;
    }

    checkIfMultiValueField(key);

    final String indexFieldName = key.getIndexFieldName();
    doc.add(new LongPoint(indexFieldName, value));
    if (key.isStored()) {
      doc.add(new StoredField(indexFieldName, value));
    }
    if (key.isSorted()) {
      Preconditions.checkArgument(key.getSortedValueType() == SearchFieldSorting.FieldType.LONG);
      doc.add(new NumericDocValuesField(indexFieldName, value));
    }
  }

  void addToDoc(IndexKey key, Integer value){
    Preconditions.checkArgument(key.getValueType() == Integer.class);
    if(value == null){
      return;
    }

    checkIfMultiValueField(key);

    final String indexFieldName = key.getIndexFieldName();
    doc.add(new IntPoint(indexFieldName, value));
    if (key.isStored()) {
      doc.add(new StoredField(indexFieldName, value));
    }
    if (key.isSorted()) {
      Preconditions.checkArgument(key.getSortedValueType() == SearchFieldSorting.FieldType.INTEGER);
      doc.add(new NumericDocValuesField(indexFieldName, value));
    }
  }

  private void addToDoc(IndexKey key, Double value){
    Preconditions.checkArgument(key.getValueType() == Double.class);
    if(value == null){
      return;
    }

    checkIfMultiValueField(key);

    final String indexFieldName = key.getIndexFieldName();
    doc.add(new DoublePoint(indexFieldName, value));
    if (key.isStored()) {
      doc.add(new StoredField(indexFieldName, value));
    }
    if (key.isSorted()) {
      Preconditions.checkArgument(key.getSortedValueType() == SearchFieldSorting.FieldType.DOUBLE);
      doc.add(new DoubleDocValuesField(indexFieldName, value));
    }
  }

  private void checkIfMultiValueField(IndexKey key) {
    final IndexableField field = doc.getField(key.getIndexFieldName());

    // ensure that fields that can only contain a single value don't get multiple values
    if (!key.canContainMultipleValues()) {
      Preconditions.checkState(field == null,
        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
    }
  }

  private void checkIfMultiValueField(IndexKey key, Object... values) {
    final IndexableField field = doc.getField(key.getIndexFieldName());

    // ensure that fields that can only contain a single value don't get multiple values
    if (!key.canContainMultipleValues()) {
      Preconditions.checkState(field == null && values.length == 1,
        "Cannot add multiple values to field [%s]", key.getIndexFieldName());
    }
  }

  private void checkIfSorted(IndexKey key, Object... values) {
    Preconditions.checkArgument(values.length < 2, "sorted fields cannot have multiple values");
  }
}
