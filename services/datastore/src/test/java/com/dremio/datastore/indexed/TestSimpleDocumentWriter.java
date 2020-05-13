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

import static org.junit.Assert.assertTrue;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

/**
 * Tests for SimpleDocumentWriter.
 */
public class TestSimpleDocumentWriter extends AbstractTestDocumentWriter<SimpleDocumentWriter> {
  @Override
  protected SimpleDocumentWriter createDocumentWriter() {
    final Document doc = new Document();
    return new SimpleDocumentWriter(doc);
  }

  @Override
  protected void verifySingleIndexValue(SimpleDocumentWriter writer, IndexKey index, Object expectedValue) {
    final Object value;
    final IndexableField field = writer.getDoc().getField(index.getIndexFieldName());
    value = getValueFromField(expectedValue, field);
    verifyHelper(expectedValue, value);
  }

  @Override
  protected void verifyMultiIndexValue(SimpleDocumentWriter writer, IndexKey index, Object... expectedValues) {
    for (int i = 0; i < expectedValues.length; i++) {
      final IndexableField field = writer.getDoc().getFields(index.getIndexFieldName())[i];
      final Object value = getValueFromField(expectedValues[i], field);
      verifyHelper(expectedValues[i], value);
    }
  }

  @Override
  protected void verifyNoValues(SimpleDocumentWriter writer, IndexKey index) {
    assertTrue(writer.getDoc().getFields(index.getIndexFieldName()).length == 0);
  }

  private static Object getValueFromField(Object expectedValue, IndexableField field) {
    if (expectedValue instanceof String) {
      return field.stringValue();
    } else if (expectedValue instanceof byte[]) {
      return field.binaryValue().bytes;
    }

    return field.numericValue();
  }
}
