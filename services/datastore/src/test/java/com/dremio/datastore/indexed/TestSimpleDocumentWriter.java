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
import static org.junit.Assert.assertTrue;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

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

  @Test
  public void testLongASCIIString() {
    final IndexKey indexKey = newIndexKey(String.class, false);
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<3500;i++) {
      sb.append("0123456789");
    }
    SimpleDocumentWriter writer = createDocumentWriter();
    writer.write(indexKey, sb.toString());

    verifySingleIndexValue(writer, indexKey,  sb.substring(0, SimpleDocumentWriter.MAX_STRING_LENGTH));
  }

  /**
   * Prepare a large sql, such that truncation happens at middle of two-byte char,
   * resulting in incorrect last char.
   * Test writing and reading from SimpleDocumentWriter does have no issues.
   */
  @Test
  public void testLongStringWithNonAscii() {
    final IndexKey indexKey = newIndexKey(String.class, false);
    StringBuilder sb = new StringBuilder();
    for (int i=0;i<27000/26;i++) {
      sb.append("abcdefghijklmnopqrstuvwxyz");
    }
    sb.append("123");
    for(int i=0;i<400;i++) {
      sb.append("\u00E4\u00FC\u00F6\u00E4\u00FC\u00F6\u00E4\u00FC\u00F6\u00E4");
    }
    SimpleDocumentWriter writer = createDocumentWriter();
    writer.write(indexKey, sb.toString());

    final byte[] strBytes = sb.toString().getBytes(UTF_8);
    final BytesRef truncatedValue = new BytesRef(strBytes, 0, Math.min(strBytes.length, SimpleDocumentWriter.MAX_STRING_LENGTH));
    verifySingleIndexValue(writer, indexKey,  truncatedValue.utf8ToString());
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
